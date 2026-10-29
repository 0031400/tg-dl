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
from .util import to_mtproto_message_id, to_tdlib_message_id

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

    # session 文件只当「实体缓存 + 同步游标」用，凭据一律从 authkeys.json 注入
    session = _make_session(cfg.fast_session_at(session_index))
    _attach_auth_key(session, AuthKeyStore(cfg.authkeys_file), session_index)

    client = TelegramClient(session, api_id, api_hash, device_model="tgdl")
    client.flood_sleep_threshold = 0  # 自己处理 FLOOD_WAIT，避免全局串行等待
    return client


# --------------------------------------------------------------------------- #
# 登录 / 登出
# --------------------------------------------------------------------------- #

async def login(
    cfg: Config,
    *,
    phone: str | None = None,
    ask_code: Callable[[int], str] | None = None,
    ask_password: Callable[[int], str] | None = None,
    session_index: int = 1,
) -> str:
    """登录一个会话。

    ``ask_code(attempt)`` / ``ask_password(attempt)`` 的 ``attempt`` 是第几次尝试
    （0 表示第一次）。多会话登录时上层可以借此复用第一次的输入，
    只有真正错了才重新问。
    """
    _, errors, _, _ = _telethon()
    client = make_client(cfg, session_index)
    await client.connect()
    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            save_primary_key(cfg, client, session_index=session_index)
            return f"已登录：{me.first_name or ''} @{me.username or me.id}"
        number = phone or input("手机号（含国家码，如 +8613800138000）: ").strip()

        # 连续登录多个会话时，sendCode 可能被限流，等一下就重试
        sent = None
        for attempt in range(4):
            try:
                sent = await client.send_code_request(number)
                break
            except errors.FloodWaitError as exc:
                wait = int(getattr(exc, "seconds", 3)) + 1
                print(f"[fast] 会话 #{session_index} 发码被限流，等待 {wait}s 后重试")
                await asyncio.sleep(wait)
        if sent is None:
            raise FastError(f"会话 #{session_index} 发码失败（多次被限流）")

        for attempt in range(5):
            code = ask_code(attempt) if ask_code else input("验证码: ").strip()
            try:
                await client.sign_in(number, code=code, phone_code_hash=sent.phone_code_hash)
                break
            except errors.SessionPasswordNeededError:
                password = (
                    ask_password(0) if ask_password else input("两步验证密码: ")
                )
                await client.sign_in(password=password)
                break
            except errors.PhoneCodeInvalidError:
                print("[fast] 验证码错误，请重试")
        me = await client.get_me()
        # 主 key 写进 authkeys.json（和派生 key 同一份文件、同等地位）
        save_primary_key(cfg, client, session_index=session_index)
        return f"登录成功：{me.first_name or ''} @{me.username or me.id}"
    finally:
        await client.disconnect()
        # 凭据只留在 authkeys.json，session 文件里抹掉
        with contextlib.suppress(Exception):
            client.session.auth_key = None


async def logout(cfg: Config, session_index: int = 1) -> str:
    client = make_client(cfg, session_index)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            return "fast 引擎当前未登录"
        store = AuthKeyStore(cfg.authkeys_file)
        await client.log_out()
        # 服务端已撤销这条授权，缓存里的 key 也要删掉
        store.drop_session(session_index)
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
    """**所有** auth key 的唯一存放处（`.tgdl/authkeys.json`）。

    每个**登录会话**（= 一个账号）是一个独立条目，它自己的主 key 和它派生出来的
    各 DC key 都挂在自己名下，互不串门：

        {
          "home_dc": 5, "home_ip": "149.154.171.5", "home_port": 443,
          "sessions": {
            "1": {"dc": 5, "ip": "149.154.171.5", "port": 443,
                  "key": "<hex>", "keys": {"4": ["<hex>", ...]}},
            "2": {"dc": 5, "ip": "149.154.171.5", "port": 443,
                  "key": "<hex>", "keys": {"4": ["<hex>"]}}
          }
        }

    顶层的 `home_*` 是给老版本读的兼容字段，等于 `sessions["1"]` 的 dc/ip/port。

    **为什么必须按会话分**：派生 key 是「从某个账号 exportAuthorization 出来的」，
    换一个账号导出得到的是另一条授权。全塞进一个池子里，多账号下载时就会拿 A 账号
    的 key 去顶 B 账号的连接——看起来是 N 个账号，实际只有 1 个账号在干活。

    安全提醒：里面是裸的 auth key，和密码等价，别外传。
    """

    MAX_PER_DC = 12

    def __init__(self, path: Path):
        self.path = Path(path)
        self._sessions: dict[str, dict] = {}
        self._load()

    # ---- 单条会话记录 ----
    def _rec(self, index: int = 1) -> dict:
        return self._sessions.setdefault(
            str(index), {"dc": None, "ip": None, "port": None, "key": None, "keys": {}}
        )

    # ---- 读写 ----
    @staticmethod
    def _norm_keys(raw) -> dict[str, list[str]]:
        if not isinstance(raw, dict):
            return {}
        return {
            str(dc): [str(x) for x in lst]
            for dc, lst in raw.items()
            if isinstance(lst, list) and lst
        }

    def _load(self) -> None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return
        if not isinstance(raw, dict):
            return

        home_dc = int(raw["home_dc"]) if raw.get("home_dc") else None
        home_ip = str(raw["home_ip"]) if raw.get("home_ip") else None
        home_port = int(raw["home_port"]) if raw.get("home_port") else None
        legacy_keys = raw.get("keys") if isinstance(raw.get("keys"), dict) else None
        sessions = raw.get("sessions")
        changed = False

        if isinstance(sessions, dict) and sessions:
            for key, val in sessions.items():
                idx = str(key)
                if isinstance(val, str):          # 老格式：{"1": "<hex>"}
                    self._sessions[idx] = {
                        "dc": home_dc, "ip": home_ip, "port": home_port,
                        "key": val, "keys": {},
                    }
                    changed = True
                elif isinstance(val, dict):
                    self._sessions[idx] = {
                        "dc": val.get("dc"),
                        "ip": val.get("ip"),
                        "port": val.get("port"),
                        "key": val.get("key"),
                        "keys": self._norm_keys(val.get("keys")),
                    }
            # 老格式的全局 key 池本来就是会话 1 派生出来的，归到它名下
            if legacy_keys and "1" in self._sessions and not self._sessions["1"]["keys"]:
                self._sessions["1"]["keys"] = self._norm_keys(legacy_keys)
                changed = True
        else:
            # 完全没有 sessions：更老的扁平格式 {"4": [...]}
            keys = self._norm_keys(legacy_keys if legacy_keys is not None else raw)
            rec = {"dc": home_dc, "ip": home_ip, "port": home_port,
                   "key": None, "keys": keys}
            # 主 key 原来混在 keys[home_dc] 的第一条里
            if rec["dc"] and rec["keys"].get(str(rec["dc"])):
                rec["key"] = rec["keys"][str(rec["dc"])].pop(0)
            self._sessions["1"] = rec
            changed = True

        if changed:
            self._flush()

    def _flush(self) -> None:
        first = self._sessions.get("1", {})
        data = {
            "home_dc": first.get("dc"),
            "home_ip": first.get("ip"),
            "home_port": first.get("port"),
            "sessions": {
                idx: {
                    "dc": rec.get("dc"),
                    "ip": rec.get("ip"),
                    "port": rec.get("port"),
                    "key": rec.get("key"),
                    "keys": {dc: lst for dc, lst in rec.get("keys", {}).items() if lst},
                }
                for idx, rec in sorted(self._sessions.items(), key=lambda kv: int(kv[0]))
            },
        }
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception:  # noqa: BLE001
            pass

    # ---- key（按会话隔离） ----
    def keys(self, dc_id: int, index: int = 1) -> list[str]:
        return list(self._sessions.get(str(index), {}).get("keys", {}).get(str(dc_id), []))

    def add(self, dc_id: int, key: bytes | None, index: int = 1) -> None:
        if not key:
            return
        bucket = self._rec(index)["keys"].setdefault(str(dc_id), [])
        hexed = key.hex()
        if hexed not in bucket:
            bucket.append(hexed)
            del bucket[: -self.MAX_PER_DC]
            self._flush()

    def drop(self, dc_id: int, key_hex: str, index: int = 1) -> None:
        bucket = self._sessions.get(str(index), {}).get("keys", {}).get(str(dc_id), [])
        if key_hex in bucket:
            bucket.remove(key_hex)
            self._flush()

    def count(self) -> int:
        return sum(len(lst) for rec in self._sessions.values() for lst in rec["keys"].values())

    def per_dc_counts(self) -> dict[int, int]:
        """所有会话加起来的「每个 DC 有多少条 key」。"""
        out: dict[int, int] = {}
        for rec in self._sessions.values():
            for dc, lst in rec["keys"].items():
                out[int(dc)] = out.get(int(dc), 0) + len(lst)
        return out

    # ---- home（每个会话自己的入口） ----
    def set_home(self, dc_id: int, ip: str | None, port: int | None,
                 index: int = 1) -> None:
        rec = self._rec(index)
        rec["dc"] = int(dc_id)
        if ip:
            rec["ip"] = str(ip)
        if port:
            rec["port"] = int(port)
        self._flush()

    def home(self, index: int = 1) -> tuple[int, str, int] | None:
        rec = self._sessions.get(str(index)) or {}
        if not rec.get("dc") or not rec.get("ip"):
            return None
        return int(rec["dc"]), str(rec["ip"]), rec.get("port") or 443

    @property
    def home_dc(self) -> int | None:
        """会话 1 的主 DC（给 `tgdl doctor` 之类的展示用）。"""
        return (self._sessions.get("1") or {}).get("dc")

    # ---- 会话主 key ----
    def session_key(self, index: int = 1) -> str | None:
        """第 index 个登录会话的 key（1=fast，2=fast2…）。"""
        return (self._sessions.get(str(index)) or {}).get("key")

    def set_session_key(self, index: int, key: bytes | None) -> None:
        if not key:
            return
        self._rec(index)["key"] = key.hex()
        self._flush()

    def drop_session(self, index: int) -> None:
        if self._sessions.pop(str(index), None) is not None:
            self._flush()

    def session_indices(self) -> list[int]:
        return sorted(int(i) for i in self._sessions)

    def primary(self, index: int = 1) -> str | None:
        rec = self._sessions.get(str(index)) or {}
        if not rec.get("dc"):
            return None
        bucket = rec["keys"].get(str(rec["dc"])) or []
        return bucket[0] if bucket else None


def _make_session(path: Path):
    """session 文件只当「实体缓存 + 同步游标」，**永不写入 auth key**。

    Telethon 的 `SQLiteSession.save()` 会把内存里的 key 落盘，所以这里覆盖
    `_update_session_table()`，无条件写 NULL —— 凭据只存在于 authkeys.json。
    """
    from telethon.sessions import SQLiteSession

    class KeylessSession(SQLiteSession):
        def _update_session_table(self) -> None:
            c = self._cursor()
            c.execute("delete from sessions")
            c.execute(
                "insert into sessions values (?,?,?,?,?,?)",
                (self._dc_id, self._server_address, self._port, None, self._takeout_id, None),
            )

    return KeylessSession(str(path))


def _attach_auth_key(session, store: AuthKeyStore, index: int = 1) -> None:
    """把 auth key 从 authkeys.json 注入 session（只放内存，不写回 session 文件）。

    顺便做两次数据自愈：
    1. 早期版本把 key 存在 `fast.session` 里 —— store 里没有就把 session 的搬过去；
    2. store 里有 key 却没有入口地址（登录时漏写，`sessions.3` 曾经就是这样）——
       用 session 文件里记着的 dc/ip 补回去，否则这条会话永远「未登录」。
    """
    from telethon.crypto import AuthKey

    if index == 1 and store.home(1) is None:
        old = session.auth_key
        if old is not None and old.key and session.dc_id:
            store.set_home(session.dc_id, session.server_address, session.port, index=1)
            store.set_session_key(1, old.key)

    if (
        store.home(index) is None
        and store.session_key(index)
        and session.dc_id
        and session.server_address
    ):
        store.set_home(session.dc_id, session.server_address, session.port, index=index)

    home = store.home(index)
    key_hex = store.session_key(index)
    if not home or not key_hex:
        return
    dc_id, ip, port = home
    # set_dc 会按数据库里的值重置 _auth_key（NULL → None），
    # 所以先设 DC，再把 key 只放进内存（KeylessSession 永远不会写盘）。
    session.set_dc(dc_id, ip, port)
    session._auth_key = AuthKey(bytes.fromhex(key_hex))


def save_primary_key(cfg: Config, client, *, session_index: int = 1) -> bool:
    """把当前连接用的 key 写进 authkeys.json（登录成功后调用）。

    每个会话一条记录：主 key 记在 `sessions.N.key`，它自己派生出来的各 DC key
    记在 `sessions.N.keys.DCx`。不同账号之间不会互相污染。
    """
    session = client.session
    key = session.auth_key
    if key is None or not key.key or not session.dc_id:
        return False
    store = AuthKeyStore(cfg.authkeys_file)
    # 每个会话都要记自己的入口地址（以前只给会话 1 记，导致 2/3 号会话
    # 下次连不上——`home(index)` 为空就直接放弃注入 key）
    store.set_home(session.dc_id, session.server_address, session.port,
                   index=session_index)
    store.set_session_key(session_index, key.key)
    return True


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
    index: int = 1,
) -> list:
    """批量拿 ``count`` 条**独立 auth key** 连接（只用 ``index`` 这个会话自己的 key）。

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
        for key_hex in store.keys(dc_id, index):
            if pending <= 0:
                break
            if used is not None:
                if key_hex in used:
                    continue
                used.add(key_hex)   # 先占位，别被并发的另一批抢走
            try:
                sender = await _open_with_key(client, dc_id, bytes.fromhex(key_hex))
            except Exception as exc:  # noqa: BLE001
                if used is not None:
                    used.discard(key_hex)
                # 只有「这条 key 真的作废」才删。flood / 网络抖动绝不能删——
                # 一次限流就能把辛苦攒的 key 池清空（实测从 12 条掉到 3 条）。
                name = type(exc).__name__
                dead = "AUTH_KEY" in str(exc).upper() or "AuthKeyUnregistered" in name \
                    or "AuthKeyDuplicated" in name
                if dead:
                    store.drop(dc_id, key_hex, index)
                else:
                    print(
                        f"[fast] 会话 {index}：key {key_hex[:8]} 这次连不上"
                        f"（{name}），先留着不删"
                    )
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
            store.add(dc_id, sender.auth_key.key, index)
            if used is not None:
                used.add(sender.auth_key.key.hex())
        out.append(sender)
    return out


def _key_tag(sender) -> str:
    """连接用的 auth key 前 8 位，用来区分不同连接。"""
    key = getattr(sender, "auth_key", None)
    raw = getattr(key, "key", None) if key is not None else None
    return raw.hex()[:8] if raw else "session"


def _report_per_conn(pairs: list, per_conn: dict, elapsed: float, workers: int) -> None:
    """打印每条连接实际搬了多少字节 —— 用来验证多 key / 多账号到底有没有生效。"""
    total = sum(v.get("total", 0) for v in per_conn.values()) or 1
    # 多个登录会话时标出每条连接属于哪个账号，否则看不出账号之间是否均衡
    accounts: dict[int, int] = {}
    for owner, _ in pairs:
        accounts.setdefault(id(owner), len(accounts) + 1)
    multi = len(accounts) > 1
    print(
        f"[fast] 每条连接的实际用量（{len(pairs)} 条连接"
        f"{f'，{len(accounts)} 个账号' if multi else ''}，"
        f"每条 {workers} 个在途，共 {total / 1048576:.1f} MB / {elapsed:.2f}s）"
    )
    for idx, (owner, sender) in enumerate(pairs):
        st = per_conn.get(idx, {"window": 0, "total": 0, "reqs": 0})
        moved = st.get("total", 0)
        share = moved / total * 100
        speed = moved / elapsed / 1048576 if elapsed > 0 else 0
        tag = _key_tag(sender)
        who = f"账号{accounts[id(owner)]}  " if multi else ""
        if moved == 0:
            print(f"  #{idx + 1}  {who}key={tag}  未使用（0 请求）")
        else:
            print(
                f"  #{idx + 1}  {who}key={tag}  {moved / 1048576:8.1f} MB  "
                f"{share:5.1f}%  {speed:6.2f} MB/s  {st['reqs']:4d} 请求"
            )
    if multi:
        per_account: dict[int, int] = {}
        for idx, (owner, _) in enumerate(pairs):
            per_account[accounts[id(owner)]] = (
                per_account.get(accounts[id(owner)], 0)
                + per_conn.get(idx, {}).get("total", 0)
            )
        detail = "，".join(
            f"账号{a} {b / 1048576:.1f} MB（{b / total * 100:.0f}%）"
            for a, b in sorted(per_account.items())
        )
        print(f"[fast] 按账号汇总：{detail}")


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

async def _pairs_for_session(
    one,
    index: int,
    target: int,
    dc_id: int | None,
    want: int,
    key_store: "AuthKeyStore | None",
    stack,
) -> list:
    """给**一个会话**建 ``want`` 条到 target DC 的连接，只用它自己的 key。

    每个会话是一个独立账号，派生 key 是「从它自己 exportAuthorization 出来的」，
    所以绝不能跨账号借用——那样看起来是 N 个账号，实际只有 1 个在干活。
    """
    cached = key_store.keys(target, index) if key_store else []
    own_dc = one.session.dc_id

    # 同 DC：exportAuthorization 不允许导出本 DC，派生不出**新** key，
    # 只能用它自己缓存里已有的（含登录时写进去的主 key）。
    if dc_id is not None and dc_id == own_dc:
        usable = min(want, len(cached)) if cached else 0
        if want > 1 and usable < want:
            print(
                f"[fast] 会话 {index}：DC{dc_id} 就是它自己的 DC，只能用它缓存里的 "
                f"{len(cached)} 条 key（无法现场派生）"
            )
        out: list = []
        if usable:
            for sender in await _fresh_senders(one, dc_id, usable, key_store, set(), index):
                stack.push_async_callback(_safe_disconnect, sender)
                out.append((one, sender))
        if not out:
            out.append((one, await stack.enter_async_context(_sender_for(one, dc_id))))
        return out

    # 只要一条、又没有缓存 → 直接用会话自己的连接，省掉一次 DH + export（约 4s）
    if want <= 1 and not cached:
        sender = await stack.enter_async_context(_sender_for(one, dc_id))
        # 借来的导出连接顺手存进**这个账号自己**的池子，下次就不用再 export
        if key_store is not None and dc_id is not None and dc_id != own_dc:
            key = getattr(sender, "auth_key", None)
            if key is not None and getattr(key, "key", None):
                key_store.add(dc_id, key.key, index)
        return [(one, sender)]

    if want > 1:
        if not cached:
            print(f"[fast] 会话 {index}：需要 {want} 条 DC{target} key → 全部现场生成")
        elif len(cached) >= want:
            print(
                f"[fast] 会话 {index}：DC{target} 缓存 {len(cached)} 条，需要 {want} 条 "
                f"→ 全部复用，免 DH 握手"
            )
        else:
            print(
                f"[fast] 会话 {index}：DC{target} 缓存 {len(cached)} 条，需要 {want} 条 "
                f"→ 复用 {len(cached)} 条，新生成 {want - len(cached)} 条"
            )

    senders = await _fresh_senders(one, target, want, key_store, set(), index)
    if not senders:
        # 一条都没建起来 → 退回会话自己的连接，至少别让这个会话缺席
        senders = [await stack.enter_async_context(_sender_for(one, dc_id))]
    out = []
    for sender in senders:
        stack.push_async_callback(_safe_disconnect, sender)
        out.append((one, sender))
    return out


async def _build_pairs(
    client,
    clients: list,
    target: int,
    dc_id: int | None,
    auth_keys: int,
    key_store: "AuthKeyStore | None",
    stack,
) -> list:
    """建一组可并行的 (client, sender)。

    ``auth_keys`` 是**每个会话**要几条连接，总连接数 = 会话数 × auth_keys。
    每个会话只从自己的 key 池里取，账号之间不串。
    """
    pairs: list = []
    for i, one in enumerate(clients):
        pairs.extend(
            await _pairs_for_session(one, i + 1, target, dc_id, auth_keys, key_store, stack)
        )

    if len(pairs) > 1:
        per_session = len(pairs) / max(1, len(clients))
        who = f"{len(clients)} 个账号" if len(clients) > 1 else "1 个账号"
        print(
            f"[fast] {who} / {len(pairs)} 条连接并行下载"
            f"（每个账号约 {per_session:.0f} 条）"
        )
    return pairs


# --------------------------------------------------------------------------- #
# 首发错峰（照抄 TDLib 的 DelayDispatcher）
# --------------------------------------------------------------------------- #

class Stagger:
    """把开局的请求错开发出去。

    TDLib 的 ``FileLoader`` 在 ``need_delay`` 时会建一个 ``DelayDispatcher``，
    每个分片发出前都过一道，间隔从 50ms 起按 0.8 衰减，下限 3ms
    （``FileLoader.cpp:140``、``FileLoader.cpp:229``）。

    目的不是限速，而是让服务端看到的是一条**斜坡**，而不是几十个查询在同一毫秒
    砸过去的尖峰——尖峰是最容易换来 FLOOD_WAIT 的形态。

    这里用「绝对时刻 + 小锁」实现：拿锁只算出该请求应该在哪一刻发出，睡眠在锁外，
    所以不会把 worker 串行化，只是把首发那一下摊开。
    """

    def __init__(self, *, first: float = 0.05, factor: float = 0.8,
                 floor: float = 0.003, count: int = 32):
        self._next = float(first)
        self._factor = float(factor)
        self._floor = float(floor)
        self._left = max(0, int(count))
        self._at = 0.0
        self._lock = asyncio.Lock()
        self.total = 0.0        # 累计被摊开的时间，跑完可以打出来

    async def wait(self) -> float:
        async with self._lock:
            if self._left <= 0:
                return 0.0
            delay = self._next
            self._left -= 1
            self._next = max(self._next * self._factor, self._floor)
            now = asyncio.get_running_loop().time()
            self._at = max(self._at, now) + delay
            target = self._at
            self.total += delay
        gap = target - asyncio.get_running_loop().time()
        if gap > 0:
            await asyncio.sleep(gap)
        return delay


# --------------------------------------------------------------------------- #

async def download_location(
    client,
    location,
    size: int,
    dest: Path,
    *,
    dc_id: int | None = None,
    connections: int = 4,   # 每条连接的并发请求数
    chunk: int = DEFAULT_CHUNK,
    retries: int = 6,
    adaptive: bool = True,
    auth_keys: int = 1,
    key_store: "AuthKeyStore | None" = None,
    extra_clients: list | None = None,
    resume_offsets: set[int] | None = None,
    on_chunk: Callable[[int, int], None] | None = None,
    senders: list | None = None,
    on_conns: Callable[[list[float]], None] | None = None,
    on_ready: Callable[[], None] | None = None,
    on_progress: ProgressCb | None = None,
    stagger: bool = False,
    locations: dict[int, object] | None = None,
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
    requested_keys = auth_keys
    total_inflight = max(1, auth_keys) * max(1, connections) * (1 + len(extra_clients or []))
    if total_inflight > 32:
        print(
            f"[fast] 注意：{1 + len(extra_clients or [])} 个账号 × {auth_keys} 条连接 "
            f"× 每条 {connections} 个在途 = {total_inflight} 个并发请求，"
            f"很容易触发 FLOOD_WAIT（建议总数 ≤ 32）"
        )
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
    per_conn: dict[int, dict[str, int]] = {}
    pairs_ref: list = []
    tick_last = [time.monotonic()]
    started = time.monotonic()

    def emit_conns(now: float) -> None:
        """每 0.5s 读一次各连接的窗口计数：速度 = 窗口字节 / 实际间隔，然后**置零**。

        计数是「这条 TCP 上一次采样以来写盘了多少字节」，显示完立刻清零，
        所以下一个窗口从头开始数，不会因为累计值越来越大而看不出变化。
        """
        dt = max(now - tick_last[0], 1e-6)
        tick_last[0] = now
        speeds: list[float] = []
        for i in range(len(pairs_ref)):
            st = per_conn.setdefault(i, {"window": 0, "total": 0, "reqs": 0})
            speeds.append(st["window"] / dt)
            st["window"] = 0          # 置零，下个窗口重新累计
        if on_conns is not None:
            on_conns(speeds)

    clients = [client, *(extra_clients or [])]
    target = dc_id or client.session.dc_id
    # 连接 -> 它属于第几个登录会话（出错时用来定位该删谁的 key）
    session_index_of = {id(c): i + 1 for i, c in enumerate(clients)}

    async with contextlib.AsyncExitStack() as stack:
        if senders is not None:
            # 调用方（shell / serve）已经建好连接池，直接拿来用
            pairs = list(senders)
        else:
            pairs = await _build_pairs(
                client, clients, target, dc_id, auth_keys, key_store, stack
            )
        pairs_ref[:] = pairs
        if on_ready is not None:
            on_ready()          # 连接全部就绪、即将开始传输

        # 每条连接一个独立的限流器：并发额度、flood 冷却都是它自己的
        limiters = [AdaptiveLimiter(connections, adaptive=adaptive) for _ in pairs]
        # 首发错峰：count 正好覆盖开局那一批 worker，摊完就彻底关掉，不拖累稳态
        gate = Stagger(count=len(pairs) * max(1, connections)) if stagger else None
        for i in range(len(pairs)):
            per_conn[i] = {"window": 0, "total": 0, "reqs": 0}
        tick_last[0] = time.monotonic()

        async def fetch(idx: int, offset: int) -> bytes:
            lim = limiters[idx]
            owner, sender = pairs[idx]
            # 每个账号用自己的 file_reference（跨账号复用会 FILE_REFERENCE_INVALID）
            loc = (locations or {}).get(id(owner), location)
            last: Exception | None = None
            for attempt in range(retries):
                await lim.wait_cooldown()      # 只有这条连接会被卡住
                if gate is not None:
                    await gate.wait()          # 开局那一批按 50ms→3ms 摊开
                try:
                    data = await _get_file(
                        owner, sender, loc, offset, chunk, cdn=state["cdn"]
                    )
                    lim.note_success()
                    return data
                except _CdnRedirect:
                    state["cdn"] = False  # 退回非 CDN 直连，下一轮重试
                    last = FastError("服务器要求走 CDN，已切换为直连重试")
                except errors.FileReferenceExpiredError as exc:
                    # 这条 key 多半不属于这个账号（早期全局 key 池迁移过来的残留）。
                    # 顺手从本会话的池子里剔掉，免得下次再撞同一条。
                    raw = getattr(getattr(sender, "auth_key", None), "key", None)
                    if key_store is not None and raw:
                        key_store.drop(target, raw.hex(), session_index_of.get(id(owner), 1))
                        print(
                            f"[fast] 会话 {session_index_of.get(id(owner), 1)} 的 key "
                            f"{raw.hex()[:8]} 读不了这条消息，已从池子里剔除"
                        )
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
                    new_limit = await lim.note_flood(wait)
                    tail = (
                        f"该连接并发降到 {new_limit}"
                        if lim.adaptive
                        else f"该连接并发保持 {new_limit}"
                    )
                    print(
                        f"[fast] 连接 #{idx + 1} FLOOD_WAIT {wait}s：暂停这条连接 "
                        f"{wait}s，其余连接继续（{tail}，累计 {lim.floods} 次）"
                    )
                    last = exc
                except (errors.RPCError, OSError) as exc:
                    last = exc
                    await asyncio.sleep(min(2 ** attempt * 0.5, 8))
            raise FastError(f"offset {offset} 重试 {retries} 次仍失败: {last}")

        async def worker(idx: int) -> None:
            lim = limiters[idx]
            handle = dest.open("r+b", buffering=0)
            try:
                while True:
                    try:
                        offset = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    await lim.wait_cooldown()
                    await lim.acquire()
                    try:
                        data = await fetch(idx, offset)
                    finally:
                        await lim.release()
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
                        st = per_conn[idx]
                        st["window"] += len(data)
                        st["total"] += len(data)
                        st["reqs"] += 1
                        if on_progress:
                            on_progress(state["done"], size)
            finally:
                handle.close()

        async def ticker() -> None:
            """严格每 0.5s：刷新写入进度 + 采样各连接窗口计数并置零。"""
            while True:
                await asyncio.sleep(0.5)
                now = time.monotonic()
                if on_progress:
                    on_progress(state["done"], size)
                emit_conns(now)

        # 每条连接开 connections 个 worker，它们共用一个「该连接的并发额度」
        workers = [
            asyncio.create_task(worker(i))
            for i in range(len(pairs))
            for _ in range(max(1, connections))
        ]
        tick_task = asyncio.create_task(ticker())
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
        finally:
            tick_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await tick_task

    elapsed = time.monotonic() - started
    if len(pairs) > 1 or requested_keys > 1:
        _report_per_conn(pairs, per_conn, elapsed, connections)
    if gate is not None:
        print(f"[fast] 首发错峰：用 {gate.total * 1000:.0f}ms 把开局请求摊开")
    total_floods = sum(l.floods for l in limiters)
    if total_floods:
        detail = "，".join(
            f"#{i + 1} 降到 {l.limit}/{l.max_limit}"
            for i, l in enumerate(limiters)
            if l.floods and l.adaptive
        ) or "全部保持原并发"
        print(f"[fast] 本次共 {total_floods} 次 FLOOD_WAIT（{detail}）")
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



async def _album_candidates(client, chat_id: int, message_id: int,
                            username: str | None = None) -> list[dict]:
    """把这条消息（以及它所在相册的其它条目）展开成候选文件列表。

    一条相册（media group）在服务端是**多条独立消息**，共享一个 ``grouped_id``：
    比如 114 是封面图（16 KB）、115 是正片（208 MB）。只给一个 id 时没法知道
    用户想要哪一个，所以这里全展开，由调用方按序号或体积挑。

    返回按消息 id 升序（= 相册里的显示顺序），每项含
    ``message_id``（TDLib 形式）/ ``location`` / ``size`` / ``name`` / ``dc_id``。
    """
    _, _, _, types = _telethon()
    message = await _fetch_message(client, chat_id, message_id, username=username)
    group = getattr(message, "grouped_id", None)

    msgs = {message.id: message}
    if group:
        entity = await _resolve_entity(client, chat_id, username=username)
        base = message.id
        ids = [i for i in range(max(1, base - 10), base + 11) if i != base]
        if ids:
            for item in await client.get_messages(entity, ids=ids):
                if item is not None and getattr(item, "grouped_id", None) == group:
                    msgs.setdefault(item.id, item)

    out: list[dict] = []
    for mid in sorted(msgs):
        try:
            location, size, name, dc_id = _location_from_message(msgs[mid], types)
        except FastError:
            continue          # 纯文字 / 不支持的类型，跳过
        out.append({
            "message_id": to_tdlib_message_id(mid),
            "location": location,
            "size": int(size),
            "name": name,
            "dc_id": dc_id,
        })
    return out


async def _resolve_entity(client, chat_id: int, username: str | None = None):
    """把 chat_id 解析成 InputPeer；缓存里没有就扫一遍 dialogs 再试。

    ``username`` 是给**公开频道**兜底的：一个账号没订阅某个公开频道时，
    手里只有数字 id 是解析不出 access_hash 的（Telethon 会 ValueError），
    但只要知道 @username 就能解析出来——所以多账号下载时由会话 1 把
    username 传下去，其它账号就不用先加入频道。
    """
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
        if username:
            name = username if username.startswith("@") else "@" + username
            try:
                # Telethon 只有带 @ 才会当成 username 去 ResolveUsername
                return await client.get_input_entity(name)
            except Exception:  # noqa: BLE001
                pass
        raise FastError(
            f"找不到 chat_id={chat_id}。请确认这个账号在该会话里，"
            "并且最近打开过（需要先同步一次会话列表）。"
        ) from exc


async def _fetch_message(client, chat_id: int, message_id: int,
                         username: str | None = None):
    message_id = to_mtproto_message_id(message_id)
    entity = await _resolve_entity(client, chat_id, username=username)
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
            raise FastError(
                "fast 引擎没有可用 auth key：执行 `tgdl login --engine fast`，"
                "或把另一台机器的 .tgdl/authkeys.json 拷过来"
            )
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
            raise FastError(
                "fast 引擎没有可用 auth key：执行 `tgdl login --engine fast`，"
                "或把另一台机器的 .tgdl/authkeys.json 拷过来"
            )
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
    connections: int = 4,   # 每条连接的并发请求数
    chunk: int = DEFAULT_CHUNK,
    limit: int = 0,
    adaptive: bool = True,
    sessions: int = 1,
    auth_keys: int = 1,
    resume_offsets: set[int] | None = None,
    on_chunk: Callable[[int, int], None] | None = None,
    on_info: Callable[[str, int], None] | None = None,
    on_conns: Callable[[list[float]], None] | None = None,
    on_ready: Callable[[], None] | None = None,
    on_progress: ProgressCb | None = None,
    stagger: bool = False,
    pick: int = 0,
) -> tuple[Path, float, str, int]:
    """按 chat_id:message_id 下载消息里的文件。

    每次都从消息里现取 location，所以不存在 file_reference 过期的问题。
    ``limit`` 不为 0 时只下载前 limit 字节（benchmark 用）。

    ``pick`` 用来处理**相册**（一条消息里挂多个文件）：1 表示相册里的第 1 个，
    不填（0）就自动挑体积最大的那个。
    """
    _, errors, _, types = _telethon()
    client = make_client(cfg)
    await client.connect()
    extra: list = []
    locations: dict[int, object] = {}
    try:
        if not await client.is_user_authorized():
            raise FastError(
                "fast 引擎没有可用 auth key：执行 `tgdl login --engine fast`，"
                "或把另一台机器的 .tgdl/authkeys.json 拷过来"
            )
        # 相册可能挂多个文件（封面图 + 正片），全展开后再挑
        candidates = await _album_candidates(client, chat_id, message_id)
        if not candidates:
            raise FastError("这条消息里没有可下载的媒体文件")
        if len(candidates) > 1:
            print(f"[fast] 这条消息是相册，共 {len(candidates)} 个文件：")
            for i, cand in enumerate(candidates, 1):
                print(
                    f"  {i}. {cand['name']}  {cand['size'] / 1048576:.1f} MB  "
                    f"DC{cand['dc_id']}  {cand['message_id']}"
                )
        if pick:
            if pick < 1 or pick > len(candidates):
                raise FastError(
                    f"--pick {pick} 超出范围：这条消息只有 {len(candidates)} 个文件"
                )
            chosen = candidates[pick - 1]
            print(f"[fast] 按 --pick 选第 {pick} 个：{chosen['name']}")
        else:
            chosen = max(candidates, key=lambda c: c["size"])
            if len(candidates) > 1:
                print(f"[fast] 没指定 --pick，自动选最大的：{chosen['name']}")

        location = chosen["location"]
        size = chosen["size"]
        name = chosen["name"]
        dc_id = chosen["dc_id"]
        locations[id(client)] = location

        # 公开频道的 @username：给没订阅这个频道的账号兜底解析用
        public_username = None
        try:
            from telethon import utils as _utils

            ent = await client.get_entity(_utils.get_peer(chat_id))
            public_username = getattr(ent, "username", None)
        except Exception:  # noqa: BLE001
            public_username = None

        # 多账号：每个账号必须**自己**取一次消息。file_reference 是账号作用域的，
        # 拿 A 账号的 file_reference 给 B 账号用，服务端直接回 FILE_REFERENCE_INVALID。
        for index in range(2, max(1, sessions) + 1):
            helper = make_client(cfg, index)
            await helper.connect()
            if not await helper.is_user_authorized():
                raise FastError(
                    f"第 {index} 个会话未登录：先执行 "
                    f"`tgdl login --engine fast --session {index}`"
                )
            try:
                own_message = await _fetch_message(
                    helper, chat_id, chosen["message_id"], username=public_username
                )
                own_location, own_size, _, _ = _location_from_message(own_message, types)
            except Exception as exc:  # noqa: BLE001
                print(
                    f"[fast] 会话 {index} 取不到这条消息"
                    f"（{type(exc).__name__}），跳过这个账号"
                )
                await helper.disconnect()
                continue
            if int(own_size) != int(size):
                print(
                    f"[fast] 会话 {index} 看到的大小是 {int(own_size)}，"
                    f"和会话 1 的 {int(size)} 不一致，按会话 1 的算"
                )
            locations[id(helper)] = own_location
            extra.append(helper)

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
                on_conns=on_conns, on_ready=on_ready, on_progress=on_progress,
                stagger=stagger, locations=locations,
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
