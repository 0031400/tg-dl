"""原生 MTProto 并行下载引擎（比 TDLib 自带下载器更快）。

为什么能更快？看 TDLib 1.8 的实现：

* ``td/telegram/files/ResourceManager.h`` 里 ``MAX_RESOURCE_LIMIT = 1 << 21``，
  即整个客户端所有文件加载共享 2 MiB 的"在途数据"预算；
* ``td/telegram/files/PartsManager.cpp`` 对每个文件按 64 KiB 起步、翻倍直到
  分片数 <= ``MAX_PART_COUNT``(4000)，且 ``MAX_PART_SIZE`` = 512 KiB。

于是 1 GiB 的文件 part_size 会被抬到 512 KiB，在途分片数 = 2 MiB / 512 KiB ≈ 4，
也就是**单文件只有 ~4 个并发请求**。再加上 ``ConnectionCreator`` 对单个 DC 的
连接数上限，长肥管道根本填不满。

这里直接用 ``upload.getFile`` 自己调度：可配置的并发数（默认 16）、固定 512 KiB
分片、每个 worker 独立文件句柄按偏移写入、无 per-part fsync、无哈希校验，
同样的网络条件下吞吐通常能到 TDLib 的 2~5 倍（可用 ``tgdl bench`` 实测）。
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import logging
import os
import time
from pathlib import Path
from typing import Callable

from .config import Config, remember_dc
from .util import to_mtproto_message_id

DEFAULT_CHUNK = 512 * 1024
MAX_CHUNK = 1024 * 1024
ALIGN = 4096
MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024
# 建连接 / 单次 RPC 的上限，避免网络卡住时无限挂起
CONNECT_TIMEOUT = 25.0
RPC_TIMEOUT = 30.0

ProgressCb = Callable[[int, int], None]


class FastError(RuntimeError):
    pass


def _telethon():
    try:
        from telethon import TelegramClient, errors, functions, types  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        raise FastError("缺少 telethon 依赖，请先执行 `uv sync`") from exc
    from telethon import TelegramClient, errors, functions, types

    return TelegramClient, errors, functions, types


_CRYPTO_CHECKED = False


def check_crypto(force: bool = False) -> bool:
    """确认 Telethon 用的是 C 实现的 AES。

    没有 cryptg 时 Telethon 会退回纯 Python 的 pyaes，解密吞吐只有几百 KB/s，
    而且并发完全不起作用——这是最容易误判成"网络慢"的坑。
    """
    global _CRYPTO_CHECKED
    try:
        from telethon.crypto import aes
    except Exception:  # noqa: BLE001
        return False
    ok = aes.cryptg is not None
    if not ok and (force or not _CRYPTO_CHECKED):
        print(
            "[fast] 警告：未检测到 cryptg，Telethon 正在用纯 Python AES 解密，"
            "下载速度会被限制在几百 KB/s。修复：uv add cryptg"
        )
    _CRYPTO_CHECKED = True
    return ok


def make_client(cfg: Config, session_index: int = 1):
    TelegramClient, _, _, _ = _telethon()
    api_id, api_hash = cfg.require_credentials()
    cfg.ensure_dirs()
    check_crypto()
    # Telethon 的 INFO 日志会插进进度条里，默认只留警告
    logging.getLogger("telethon").setLevel(logging.WARNING)
    client = TelegramClient(
        str(cfg.fast_session_at(session_index)), api_id, api_hash, device_model="tgdl"
    )
    client.flood_sleep_threshold = 0  # 自己处理 FLOOD_WAIT，避免全局串行等待
    return client


# --------------------------------------------------------------------------- #
# 登录 / 登出
# --------------------------------------------------------------------------- #

async def login(
    cfg: Config,
    *,
    phone: str | None = None,
    ask_code: Callable[[], str] | None = None,
    ask_password: Callable[[], str] | None = None,
    session_index: int = 1,
) -> str:
    _, errors, _, _ = _telethon()
    client = make_client(cfg, session_index)
    await client.connect()
    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            return f"已登录：{me.first_name or ''} @{me.username or me.id}"
        number = phone or input("手机号（含国家码，如 +8613800138000）: ").strip()
        sent = await client.send_code_request(number)
        for _ in range(5):
            code = ask_code() if ask_code else input("验证码: ").strip()
            try:
                await client.sign_in(number, code=code, phone_code_hash=sent.phone_code_hash)
                break
            except errors.SessionPasswordNeededError:
                password = ask_password() if ask_password else input("两步验证密码: ")
                await client.sign_in(password=password)
                break
            except errors.PhoneCodeInvalidError:
                print("[fast] 验证码错误，请重试")
        me = await client.get_me()
        return f"登录成功：{me.first_name or ''} @{me.username or me.id}"
    finally:
        await client.disconnect()


async def logout(cfg: Config, session_index: int = 1) -> str:
    client = make_client(cfg, session_index)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            return "fast 引擎当前未登录"
        await client.log_out()
        return "fast 引擎已退出登录"
    finally:
        await client.disconnect()


async def whoami(cfg: Config, session_index: int = 1) -> str | None:
    client = make_client(cfg, session_index)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            return None
        me = await client.get_me()
        return f"{me.first_name or ''} @{me.username or me.id} (id={me.id})"
    finally:
        await client.disconnect()


# --------------------------------------------------------------------------- #
# 定位 / 尺寸
# --------------------------------------------------------------------------- #

@contextlib.asynccontextmanager
async def _sender_for(client, dc_id: int | None):
    """文件不在本会话的 DC 上时，需要借一个导出连接，否则会 FILE_MIGRATE。"""
    own = client.session.dc_id
    if dc_id and own and dc_id != own:
        sender = await client._borrow_exported_sender(dc_id)
        try:
            yield sender
        finally:
            await client._return_exported_sender(sender)
    else:
        yield client._sender


async def _safe_disconnect(sender) -> None:
    with contextlib.suppress(Exception):
        await sender.disconnect()


class AuthKeyStore:
    """把为其他 DC 生成的 auth key 落盘，下次直接复用。

    DH 握手一次约 3 秒，而用存下来的 key 重连只要 ~0.02 秒。
    注意：这个文件和 session 文件同等敏感（都是能直接读你消息的凭据），别外传。
    """

    MAX_PER_DC = 12

    def __init__(self, path: Path):
        self.path = Path(path)
        self._data: dict[str, list[str]] = {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                self._data = {
                    str(k): [str(x) for x in v] for k, v in raw.items() if isinstance(v, list)
                }
        except Exception:  # noqa: BLE001
            pass

    def keys(self, dc_id: int) -> list[str]:
        return list(self._data.get(str(dc_id), []))

    def add(self, dc_id: int, key: bytes) -> None:
        bucket = self._data.setdefault(str(dc_id), [])
        hexed = key.hex()
        if hexed not in bucket:
            bucket.append(hexed)
            del bucket[: -self.MAX_PER_DC]
            self._flush()

    def drop(self, dc_id: int, key_hex: str) -> None:
        bucket = self._data.get(str(dc_id), [])
        if key_hex in bucket:
            bucket.remove(key_hex)
            self._flush()

    def count(self) -> int:
        return sum(len(v) for v in self._data.values())

    def _flush(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps(self._data, indent=2), encoding="utf-8")
        except Exception:  # noqa: BLE001
            pass


async def _open_with_key(client, dc_id: int, key: bytes):
    """用现成的 auth key 直连（不做 DH）。"""
    from telethon import functions as _functions
    from telethon.client.telegrambaseclient import LAYER
    from telethon.crypto import AuthKey
    from telethon.network import MTProtoSender
    from telethon.tl.functions import InvokeWithLayerRequest

    dc = await client._get_dc(dc_id)
    sender = MTProtoSender(AuthKey(key), loggers=client._log)
    await asyncio.wait_for(
        sender.connect(
            client._connection(dc.ip_address, dc.port, dc_id, loggers=client._log)
        ),
        CONNECT_TIMEOUT,
    )
    try:
        # initConnection 每条连接要发一次；顺手当校验，key 失效会抛错
        init = copy.copy(client._init_request)
        init.query = _functions.help.GetConfigRequest()
        await asyncio.wait_for(sender.send(InvokeWithLayerRequest(LAYER, init)), RPC_TIMEOUT)
    except Exception:  # noqa: BLE001
        await _safe_disconnect(sender)
        raise
    return sender


async def _connect_blank(client, dc_id: int):
    """建一条还没授权的连接（DH 握手完成，尚未 importAuthorization）。"""
    from telethon.network import MTProtoSender

    dc = await client._get_dc(dc_id)
    sender = MTProtoSender(None, loggers=client._log)
    await asyncio.wait_for(
        sender.connect(
            client._connection(dc.ip_address, dc.port, dc_id, loggers=client._log)
        ),
        CONNECT_TIMEOUT,
    )
    return sender


async def _fresh_senders(
    client,
    dc_id: int,
    count: int,
    store: "AuthKeyStore | None" = None,
    used: set[str] | None = None,
) -> list:
    """批量拿 ``count`` 条**独立 auth key** 连接。

    两个关键约束（都是实测出来的）：

    1. ``importAuthorization`` **不能并发**——服务端只放行一条，其余全部返回
       ``AuthBytesInvalidError``（"The provided authorization is invalid"）。
       并发创建 3 条时只有 1 条能成功。
    2. DH 握手**可以并发**，而且它占了绝大部分时间（~3.4s）。

    所以流程是：先吃缓存 → 并行 DH → 串行导出+导入。实测 4 条 8.3s
    （全串行要 14s），而且 4/4 都成功。
    """
    if count <= 0:
        return []

    from telethon import functions as _functions
    from telethon.client.telegrambaseclient import LAYER
    from telethon.tl.functions import InvokeWithLayerRequest

    out: list = []
    pending = count

    # 1) 缓存命中直接连（不做 DH）
    if store is not None:
        for key_hex in store.keys(dc_id):
            if pending <= 0:
                break
            if used is not None:
                if key_hex in used:
                    continue
                used.add(key_hex)   # 先占位，别被并发的另一批抢走
            try:
                sender = await _open_with_key(client, dc_id, bytes.fromhex(key_hex))
            except Exception:  # noqa: BLE001
                if used is not None:
                    used.discard(key_hex)
                store.drop(dc_id, key_hex)   # 失效就丢掉
                continue
            out.append(sender)
            pending -= 1

    if pending <= 0:
        return out

    # 2) 并行握手（省掉 N 倍的 3.4s）
    results = await asyncio.gather(
        *[_connect_blank(client, dc_id) for _ in range(pending)],
        return_exceptions=True,
    )
    blanks = []
    for item in results:
        if isinstance(item, BaseException):
            print(f"[fast] 握手失败：{type(item).__name__}: {str(item)[:80]}")
            continue
        blanks.append(item)

    # 3) 串行导出 + 导入（并发会被服务端拒）
    for sender in blanks:
        try:
            auth = await asyncio.wait_for(
                client(_functions.auth.ExportAuthorizationRequest(dc_id)), RPC_TIMEOUT
            )
            init = copy.copy(client._init_request)
            init.query = _functions.auth.ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes
            )
            await asyncio.wait_for(
                sender.send(InvokeWithLayerRequest(LAYER, init)), RPC_TIMEOUT
            )
        except Exception as exc:  # noqa: BLE001
            await _safe_disconnect(sender)
            print(f"[fast] auth key 创建失败：{type(exc).__name__}: {str(exc)[:80]}")
            continue
        if store is not None and sender.auth_key is not None:
            store.add(dc_id, sender.auth_key.key)
            if used is not None:
                used.add(sender.auth_key.key.hex())
        out.append(sender)
    return out


async def _get_file(client, sender, location, offset: int, limit: int, *, cdn: bool) -> bytes:
    _, errors, functions, types = _telethon()
    request = functions.upload.GetFileRequest(
        location=location, offset=offset, limit=limit, cdn_supported=cdn
    )
    result = await client._call(sender, request, flood_sleep_threshold=0)
    if isinstance(result, types.upload.FileCdnRedirect):
        raise _CdnRedirect(result)
    return result.bytes


class _CdnRedirect(Exception):
    def __init__(self, redirect):
        self.redirect = redirect
        super().__init__("CDN redirect")


async def probe_size(client, location, *, dc_id: int | None = None,
                     limit: int = MAX_FILE_SIZE) -> int:
    """二分探测文件大小（每次只读 4 KiB），避免依赖 TDLib 元数据。"""
    async with _sender_for(client, dc_id) as sender:
        async def has_data(offset: int) -> bool:
            try:
                return len(await _get_file(client, sender, location, offset, ALIGN, cdn=False)) > 0
            except _CdnRedirect:
                return True

        if not await has_data(0):
            return 0
        lo, hi = 0, ALIGN
        while hi <= limit:
            if await has_data(hi):
                lo, hi = hi, hi * 2
            else:
                break
        else:
            raise FastError("文件超过 4 GiB，暂不支持")
        while hi - lo > ALIGN:
            mid = max(((lo + hi) // 2) // ALIGN * ALIGN, lo + ALIGN)
            if await has_data(mid):
                lo = mid
            else:
                hi = mid
        tail = await _get_file(client, sender, location, lo, ALIGN, cdn=False)
        return lo + len(tail)


# --------------------------------------------------------------------------- #
# 自适应限流
# --------------------------------------------------------------------------- #

class AdaptiveLimiter:
    """在途请求数的闸门。

    默认是**自适应**的（同 TDLib 的 ResourceManager）：

    1. 撞到 FLOOD_WAIT 就把在途额度砍半，并记一个全局冷却时间点，
       所有 worker 在这之前都不再发请求（避免睡醒后集体突发）；
    2. 连续成功若干次后，额度慢慢涨回上限。

    ``adaptive=False`` 时并发**钉死在给定值**：撞限流只做全局冷却、不降额度，
    也不会自动涨回去。想复现固定并发、或想确认某个值到底稳不稳时用这个。
    """

    def __init__(
        self,
        limit: int,
        *,
        grow_after: int = 16,
        start: int | None = None,
        adaptive: bool = True,
    ):
        self.max_limit = max(1, limit)
        # 砍半时留个地板，别一路掉到 1（掉下去再爬回来太慢，实测反而更慢）
        self.min_limit = max(1, self.max_limit // 4)
        self.adaptive = adaptive
        # 自适应模式直接从上限起步；固定模式当然就用给定的值
        self.limit = max(1, min(start if start is not None else self.max_limit, self.max_limit))
        self.active = 0
        self.flood_until = 0.0
        self.floods = 0
        self._ok_streak = 0
        self._grow_after = grow_after
        self._cond = asyncio.Condition()

    async def acquire(self) -> None:
        async with self._cond:
            while self.active >= self.limit:
                await self._cond.wait()
            self.active += 1

    async def release(self) -> None:
        async with self._cond:
            self.active -= 1
            self._cond.notify()

    def note_success(self) -> None:
        if not self.adaptive:
            return
        self._ok_streak += 1
        if self._ok_streak >= self._grow_after and self.limit < self.max_limit:
            self._ok_streak = 0
            self.limit = min(self.max_limit, self.limit + 1)

    async def note_flood(self, seconds: int) -> int:
        """记录一次限流，返回当前的额度（自适应模式下是砍半后的值）。"""
        self._ok_streak = 0
        self.floods += 1
        self.flood_until = max(self.flood_until, time.monotonic() + seconds + 0.5)
        if not self.adaptive:
            return self.limit
        async with self._cond:
            old = self.limit
            self.limit = max(self.min_limit, self.limit // 2)
            if self.limit != old:
                self._cond.notify_all()
            return self.limit

    async def wait_cooldown(self) -> None:
        delay = self.flood_until - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)


# --------------------------------------------------------------------------- #
# 并行下载
# --------------------------------------------------------------------------- #
# 连接池构建
# --------------------------------------------------------------------------- #

async def _build_pairs(
    client,
    clients: list,
    target: int,
    dc_id: int | None,
    auth_keys: int,
    key_store: "AuthKeyStore | None",
    stack,
) -> list:
    """建一组可并行的 (client, sender)。优先用缓存 auth key，不做多余 DH。"""
    need_extra = max(0, auth_keys - 1)
    cached_before = set(key_store.keys(target)) if key_store else set()

    # 文件就在会话自己的 DC 上：exportAuthorization 不允许导出本 DC，
    # 派生额外 key 必然失败（还会打一堆错），直接用会话自己的连接即可。
    if dc_id is not None and dc_id == client.session.dc_id:
        if auth_keys > 1:
            print(
                f"[fast] DC{dc_id} 是会话自身所在 DC，无法派生额外 auth key"
                " → 只用 1 条连接（速度会低于多 key 模式）"
            )
        pairs: list = []
        for one in clients:
            sender = await stack.enter_async_context(_sender_for(one, dc_id))
            pairs.append((one, sender))
        return pairs

    # 能不能用缓存 key 顶掉「借导出连接」那一条？
    # 借导出连接本身要做一次全新 DH（实测 ~3.4s），而缓存 key 直连只要 ~1.1s，
    # 所以只要缓存里有货、且文件在别的 DC（导出授权只对非本 DC 有效），就优先用缓存。
    cache_primary = (
        key_store is not None
        and bool(cached_before)
        and len(clients) == 1
        and dc_id is not None
        and dc_id != client.session.dc_id
    )
    slots = auth_keys if cache_primary else need_extra

    # 预检信息必须在任何慢操作（借导出连接 / DH 握手）之前打印出来
    if slots > 0:
        if key_store is None:
            print(f"[fast] 需要 {slots} 条 auth key（未启用缓存，全部现场生成）")
        elif len(cached_before) >= slots:
            print(
                f"[fast] auth key DC{target}：缓存 {len(cached_before)} 条，"
                f"需要 {slots} 条 → 全部复用，免 DH 握手"
            )
        else:
            print(
                f"[fast] auth key DC{target}：缓存 {len(cached_before)} 条，"
                f"需要 {slots} 条 → 复用 {len(cached_before)} 条，"
                f"新生成 {slots - len(cached_before)} 条"
            )

    pairs: list = []
    if slots > 0:
        used: set[str] = set()
        for sender in await _fresh_senders(client, target, slots, key_store, used):
            stack.push_async_callback(_safe_disconnect, sender)
            pairs.append((client, sender))

    # 一条都没建起来（或本来就不走缓存）→ 退回会话自己的连接
    if not pairs:
        for one in clients:
            sender = await stack.enter_async_context(_sender_for(one, dc_id))
            pairs.append((one, sender))
    elif len(clients) > 1:
        # 多登录会话：把它们各自的连接也加进来一起摊请求
        for one in clients[1:]:
            sender = await stack.enter_async_context(_sender_for(one, dc_id))
            pairs.append((one, sender))

    if len(pairs) > 1 and key_store is not None:
        cached_after = set(key_store.keys(target))
        fresh_count = len(cached_after - cached_before)
        print(
            f"[fast] 使用 {len(pairs)} 条独立 auth key 并行下载"
            f"（复用 {len(pairs) - fresh_count} 条缓存，新生成 {fresh_count} 条，"
            f"DC{target} 现有 {len(cached_after)} 条）"
        )
    elif len(pairs) > 1:
        print(f"[fast] 使用 {len(pairs)} 条独立 auth key 并行下载")
    return pairs


# --------------------------------------------------------------------------- #

async def download_location(
    client,
    location,
    size: int,
    dest: Path,
    *,
    dc_id: int | None = None,
    connections: int = 16,
    chunk: int = DEFAULT_CHUNK,
    retries: int = 6,
    adaptive: bool = True,
    auth_keys: int = 1,
    key_store: "AuthKeyStore | None" = None,
    extra_clients: list | None = None,
    resume_offsets: set[int] | None = None,
    on_chunk: Callable[[int, int], None] | None = None,
    senders: list | None = None,
    on_progress: ProgressCb | None = None,
) -> Path:
    """并行下载一个文件。

    ``extra_clients`` 是额外登录会话（每个都是一条独立 auth key）。
    Telegram 的 flood 预算按 auth key 算，所以多个会话能把请求摊开、叠加吞吐；
    ``connections`` 则是所有会话加起来的总在途请求数。

    ``senders`` 传入 ``[(client, sender), ...]`` 时直接复用这组连接，
    不再自己建（``tgdl shell`` 的连接池就是走这条路，省掉每次建连的开销）。
    """
    _, errors, functions, types = _telethon()
    if size <= 0:
        raise FastError("文件大小为 0")
    chunk = max(ALIGN, min(chunk, MAX_CHUNK) // ALIGN * ALIGN)
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)

    # 断点续传：文件大小对得上就保留已有数据，只补没下过的分片
    done_offsets: set[int] = set(resume_offsets or ())
    if done_offsets and (not dest.exists() or dest.stat().st_size != size):
        done_offsets = set()  # 文件被删过或大小不对，重头来
    if done_offsets:
        with dest.open("r+b") as fp:
            fp.truncate(size)
    else:
        with dest.open("wb") as fp:
            fp.truncate(size)  # 预分配，避免边下边扩导致碎片

    queue: asyncio.Queue[int] = asyncio.Queue()
    for offset in range(0, size, chunk):
        if offset not in done_offsets:
            queue.put_nowait(offset)

    # 已下载部分要算进进度，否则最后的截断逻辑会把文件截短
    resumed_bytes = sum(min(chunk, size - off) for off in done_offsets)
    state = {"done": resumed_bytes, "cdn": True}
    lock = asyncio.Lock()
    limiter = AdaptiveLimiter(connections, adaptive=adaptive)
    started = time.monotonic()

    clients = [client, *(extra_clients or [])]
    target = dc_id or client.session.dc_id

    async with contextlib.AsyncExitStack() as stack:
        if senders is not None:
            # 调用方（shell / serve）已经建好连接池，直接拿来用
            pairs = list(senders)
        else:
            pairs = await _build_pairs(
                client, clients, target, dc_id, auth_keys, key_store, stack
            )

        async def fetch(pair, offset: int) -> bytes:
            owner, sender = pair
            last: Exception | None = None
            for attempt in range(retries):
                await limiter.wait_cooldown()  # 全局冷却期内不发起任何请求
                try:
                    data = await _get_file(
                        owner, sender, location, offset, chunk, cdn=state["cdn"]
                    )
                    limiter.note_success()
                    return data
                except _CdnRedirect:
                    state["cdn"] = False  # 退回非 CDN 直连，下一轮重试
                    last = FastError("服务器要求走 CDN，已切换为直连重试")
                except errors.FileReferenceExpiredError as exc:
                    raise FastError(
                        "file_reference 已过期或无效。重新执行一次命令即可"
                        "（每次都会从消息里取最新的 file_reference）。"
                    ) from exc
                except errors.FileMigrateError as exc:
                    raise FastError(
                        f"文件在 DC{exc.new_dc} 上，导出连接失败。"
                        "可改用 `--engine tdlib`。"
                    ) from exc
                except errors.FloodWaitError as exc:
                    wait = int(getattr(exc, "seconds", 1))
                    new_limit = await limiter.note_flood(wait)
                    if limiter.adaptive:
                        detail = f"在途并发降到 {new_limit}"
                    else:
                        detail = f"保持并发 {new_limit}（固定模式）"
                    print(
                        f"[fast] FLOOD_WAIT {wait}s，{detail}"
                        f"（累计 {limiter.floods} 次）"
                    )
                    last = exc
                except (errors.RPCError, OSError) as exc:
                    last = exc
                    await asyncio.sleep(min(2 ** attempt * 0.5, 8))
            raise FastError(f"offset {offset} 重试 {retries} 次仍失败: {last}")

        async def worker(slot: int) -> None:
            pair = pairs[slot % len(pairs)]
            handle = dest.open("r+b", buffering=0)
            try:
                while True:
                    try:
                        offset = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    await limiter.wait_cooldown()
                    await limiter.acquire()
                    try:
                        data = await fetch(pair, offset)
                    finally:
                        await limiter.release()
                    if not data:
                        continue
                    handle.seek(offset)
                    view = memoryview(data)
                    while view:
                        written = handle.write(view)
                        if not written:
                            raise FastError(f"写盘失败 offset={offset}")
                        view = view[written:]
                    if on_chunk is not None:
                        on_chunk(offset, len(data))
                    async with lock:
                        state["done"] += len(data)
                        if on_progress:
                            on_progress(state["done"], size)
            finally:
                handle.close()

        workers = [
            asyncio.create_task(worker(i)) for i in range(max(1, connections))
        ]
        try:
            done, pending = await asyncio.wait(workers, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                if task.exception() is not None:
                    for rest in pending:
                        rest.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                    raise task.exception()  # type: ignore[misc]
            await asyncio.gather(*pending, return_exceptions=True)
        except asyncio.CancelledError:
            for task in workers:
                task.cancel()
            raise

    elapsed = time.monotonic() - started
    if limiter.floods:
        tail = (
            f"并发已自适应到 {limiter.limit}（上限 {limiter.max_limit}）"
            if limiter.adaptive
            else f"并发固定为 {limiter.limit}"
        )
        print(f"[fast] 本次共遇到 {limiter.floods} 次 FLOOD_WAIT，{tail}")
    if state["done"] < size:
        # 探测的大小可能偏大，按实际写入量截断
        os.truncate(dest, state["done"])
    if on_progress:
        on_progress(size, size)
    return dest


# --------------------------------------------------------------------------- #
# 从消息解析定位
# --------------------------------------------------------------------------- #

def _location_from_message(message, types) -> tuple[object, int, str, int | None]:
    media = getattr(message, "media", None)
    if isinstance(media, types.MessageMediaDocument):
        document = media.document
        if not isinstance(document, types.Document):
            raise FastError("这条消息里没有可下载的文档")
        name = next(
            (attr.file_name for attr in document.attributes
             if isinstance(attr, types.DocumentAttributeFilename)),
            f"doc_{document.id}",
        )
        location = types.InputDocumentFileLocation(
            id=document.id,
            access_hash=document.access_hash,
            file_reference=document.file_reference,
            thumb_size="",
        )
        return location, int(document.size), name, getattr(document, "dc_id", None)

    if isinstance(media, types.MessageMediaPhoto) and isinstance(media.photo, types.Photo):
        photo = media.photo
        sizes = [
            size for size in photo.sizes
            if isinstance(size, (types.PhotoSize, types.PhotoSizeProgressive))
        ]
        if not sizes:
            raise FastError("照片没有可用尺寸")
        best = max(
            sizes,
            key=lambda s: s.size if isinstance(s, types.PhotoSize) else max(s.sizes),
        )
        total = best.size if isinstance(best, types.PhotoSize) else max(best.sizes)
        location = types.InputPhotoFileLocation(
            id=photo.id,
            access_hash=photo.access_hash,
            file_reference=photo.file_reference,
            thumb_size=best.type,
        )
        return location, int(total), f"photo_{photo.id}.jpg", getattr(photo, "dc_id", None)

    raise FastError("这条消息里没有可下载的媒体文件")



async def _resolve_entity(client, chat_id: int):
    """把 chat_id 解析成 InputPeer；缓存里没有就扫一遍 dialogs 再试。"""
    from telethon import utils

    peer = utils.get_peer(chat_id)
    try:
        return await client.get_input_entity(peer)
    except ValueError:
        pass
    # Telethon 对只给了 id 的频道/群，必须先有 access_hash（缓存里才有）
    async for _ in client.iter_dialogs():
        pass
    try:
        return await client.get_input_entity(peer)
    except ValueError as exc:
        raise FastError(
            f"找不到 chat_id={chat_id}。请确认这个账号在该会话里，"
            "并且最近打开过（需要先同步一次会话列表）。"
        ) from exc


async def _fetch_message(client, chat_id: int, message_id: int):
    message_id = to_mtproto_message_id(message_id)
    entity = await _resolve_entity(client, chat_id)
    try:
        message = await client.get_messages(entity, ids=message_id)
    except Exception as exc:  # noqa: BLE001
        raise FastError(f"取消息失败 chat_id={chat_id} message_id={message_id}: {exc}") from exc
    if message is None:
        raise FastError(
            f"找不到消息 chat_id={chat_id} message_id={message_id}"
            "（可能已删除，或不在这个会话里）"
        )
    return message


async def resolve_chat(cfg: Config, text: str) -> int:
    """把 @username / t.me 链接解析成 chat_id（-100… 形式）。"""
    from telethon import utils

    client = make_client(cfg)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise FastError("fast 引擎未登录，先执行 `tgdl login --engine fast`")
        try:
            entity = await client.get_entity(text)
        except Exception as exc:  # noqa: BLE001
            raise FastError(f"解析 {text} 失败：{exc}") from exc
        return int(utils.get_peer_id(entity))
    finally:
        await client.disconnect()


async def message_info(cfg: Config, chat_id: int, message_id: int) -> dict:
    """读取消息里的文件信息（不下载）。"""
    _, _, _, types = _telethon()
    client = make_client(cfg)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise FastError("fast 引擎未登录，先执行 `tgdl login --engine fast`")
        message = await _fetch_message(client, chat_id, message_id)
        location, size, name, dc_id = _location_from_message(message, types)
        media = message.media
        mime = None
        if isinstance(media, types.MessageMediaDocument):
            mime = getattr(media.document, "mime_type", None)
        elif isinstance(media, types.MessageMediaPhoto):
            mime = "image/jpeg"
        return {
            "chat_id": chat_id, "message_id": message_id,
            "name": name, "size": int(size), "dc_id": dc_id, "mime": mime,
            "kind": type(location).__name__,
        }
    finally:
        await client.disconnect()


async def download_message(
    cfg: Config,
    chat_id: int,
    message_id: int,
    dest: Path | None,
    *,
    connections: int = 16,
    chunk: int = DEFAULT_CHUNK,
    limit: int = 0,
    adaptive: bool = True,
    sessions: int = 1,
    auth_keys: int = 1,
    resume_offsets: set[int] | None = None,
    on_chunk: Callable[[int, int], None] | None = None,
    on_info: Callable[[str, int], None] | None = None,
    on_progress: ProgressCb | None = None,
) -> tuple[Path, float, str, int]:
    """按 chat_id:message_id 下载消息里的文件。

    每次都从消息里现取 location，所以不存在 file_reference 过期的问题。
    ``limit`` 不为 0 时只下载前 limit 字节（benchmark 用）。
    """
    _, errors, _, types = _telethon()
    client = make_client(cfg)
    await client.connect()
    extra: list = []
    try:
        if not await client.is_user_authorized():
            raise FastError("fast 引擎未登录，先执行 `tgdl login --engine fast`")
        # 多会话：每个会话一条独立 auth key，flood 预算独立，能叠加吞吐
        for index in range(2, max(1, sessions) + 1):
            helper = make_client(cfg, index)
            await helper.connect()
            if not await helper.is_user_authorized():
                raise FastError(
                    f"第 {index} 个会话未登录：先执行 "
                    f"`tgdl login --engine fast --session {index}`"
                )
            extra.append(helper)
        message = await _fetch_message(client, chat_id, message_id)
        location, size, name, dc_id = _location_from_message(message, types)
        remember_dc(cfg, session_dc=client.session.dc_id, pool_dc=dc_id)
        size = int(size)
        if limit:
            size = min(size, int(limit))
        if on_info is not None:
            on_info(name, size)  # 一拿到消息就告诉调用方，省掉一次单独的 message_info 连接
        target = Path(dest) if dest is not None else Path.cwd()
        if target.is_dir():
            target = target / name
        started = time.monotonic()
        try:
            await download_location(
                client, location, size, target, dc_id=dc_id,
                connections=connections, chunk=chunk,
                adaptive=adaptive, auth_keys=auth_keys,
                key_store=AuthKeyStore(cfg.authkeys_file),
                extra_clients=extra,
                resume_offsets=resume_offsets, on_chunk=on_chunk,
                on_progress=on_progress,
            )
        except errors.FileReferenceExpiredError as exc:
            raise FastError(
                "file_reference 过期了，重新跑一次（每次都会从消息里取最新的）"
            ) from exc
        except errors.RPCError as exc:
            raise FastError(f"Telegram 返回错误: {exc}") from exc
        return target, time.monotonic() - started, name, size
    finally:
        for helper in extra:
            await helper.disconnect()
        await client.disconnect()
