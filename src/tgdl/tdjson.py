"""TDLib 的 ctypes 绑定（tdjson 接口）+ 登录/登出/下载的高层封装。"""

from __future__ import annotations

import ctypes
import itertools
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable

from .config import Config
from .util import to_tdlib_message_id

Update = dict[str, Any]


class TdlibError(RuntimeError):
    """TDLib 返回的 error 对象。"""

    def __init__(self, payload: dict[str, Any]):
        self.code = int(payload.get("code") or 0)
        self.message = str(payload.get("message") or "")
        super().__init__(f"TDLib error {self.code}: {self.message}")


class TdlibLoadError(RuntimeError):
    pass


def _is_not_found(exc: "TdlibError") -> bool:
    message = (exc.message or "").lower()
    return exc.code == 404 or "not found" in message


class _Api:
    """对 tdjson.dll 的最小封装。"""

    def __init__(self, dll_path: Path):
        dll_path = Path(dll_path)
        if not dll_path.is_file():
            raise TdlibLoadError(
                f"找不到 tdjson 动态库: {dll_path}\n"
                "用 `tgdl init --tdjson <路径>` 或环境变量 TGDL_TDJSON 指定。"
            )
        # Windows 上 tdjson 依赖同目录的 libcrypto/libssl/zlib
        if sys.platform == "win32" and hasattr(os, "add_dll_directory"):
            self._dll_dir = os.add_dll_directory(str(dll_path.parent))
        self.lib = ctypes.CDLL(str(dll_path))
        self.lib.td_json_client_create.restype = ctypes.c_void_p
        self.lib.td_json_client_send.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        self.lib.td_json_client_receive.argtypes = [ctypes.c_void_p, ctypes.c_double]
        self.lib.td_json_client_receive.restype = ctypes.c_char_p
        self.lib.td_json_client_execute.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        self.lib.td_json_client_execute.restype = ctypes.c_char_p
        self.lib.td_json_client_destroy.argtypes = [ctypes.c_void_p]
        self.lib.td_set_log_verbosity_level.argtypes = [ctypes.c_int]

    def create(self) -> ctypes.c_void_p:
        handle = self.lib.td_json_client_create()
        if not handle:
            raise TdlibLoadError("td_json_client_create 失败")
        return handle

    def destroy(self, handle: ctypes.c_void_p) -> None:
        self.lib.td_json_client_destroy(handle)

    def send(self, handle: ctypes.c_void_p, request: dict[str, Any]) -> None:
        self.lib.td_json_client_send(handle, json.dumps(request, ensure_ascii=False).encode())

    def receive(self, handle: ctypes.c_void_p, timeout: float) -> Update | None:
        raw = self.lib.td_json_client_receive(handle, ctypes.c_double(timeout))
        if not raw:
            return None
        return json.loads(raw.decode())

    def execute(self, handle: ctypes.c_void_p, request: dict[str, Any]) -> Update | None:
        raw = self.lib.td_json_client_execute(handle, json.dumps(request).encode())
        if not raw:
            return None
        return json.loads(raw.decode())

    def set_log_verbosity(self, level: int) -> None:
        self.lib.td_set_log_verbosity_level(level)


_MESSAGE_FILE_FIELDS = {
    "messageDocument": ("document", "document"),
    "messageVideo": ("video", "video"),
    "messageAudio": ("audio", "audio"),
    "messageAnimation": ("animation", "animation"),
    "messageVoiceNote": ("voice_note", "voice"),
    "messageVideoNote": ("video_note", "video"),
    "messageSticker": ("sticker", "sticker"),
}


def file_from_message(message: Update) -> tuple[Update, str, str]:
    """从 td_api 的 message 对象里取出 (file, 文件名, 类型名)。"""
    content = message.get("content") or {}
    kind = content.get("@type", "")

    if kind == "messagePhoto":
        photo = content.get("photo") or {}
        sizes = [s for s in (photo.get("sizes") or []) if s.get("photo")]
        if not sizes:
            raise TdlibError({"code": 400, "message": "这条消息里的照片没有可用尺寸"})
        best = max(sizes, key=lambda s: int(s.get("width", 0)) * int(s.get("height", 0)))
        return best["photo"], f"photo_{best['photo']['id']}.jpg", "photo"

    fields = _MESSAGE_FILE_FIELDS.get(kind)
    if fields is None:
        raise TdlibError({
            "code": 400,
            "message": f"这条消息没有可下载的文件（content = {kind or '空'}）",
        })
    holder = content.get(fields[0]) or {}
    file = holder.get(fields[1])
    if not file:
        raise TdlibError({"code": 400, "message": f"消息里缺少 file 对象（{kind}）"})
    name = holder.get("file_name") or f"{fields[0]}_{file.get('id')}"
    return file, name, fields[0]


class Tdlib:
    """一个 TDLib 客户端实例。"""

    def __init__(self, cfg: Config, *, verbosity: int = 1):
        cfg.require_credentials()
        cfg.ensure_dirs()
        self.cfg = cfg
        self.api = _Api(cfg.tdjson)
        self.api.set_log_verbosity(verbosity)
        self.client = self.api.create()
        self.files: dict[int, Update] = {}
        self._extra = itertools.count(1)
        self._last_state: str | None = None
        # 最近一次收到的授权状态（完整对象）。getAuthorizationState 在 TDLib 里不是
        # 同步请求，execute() 拿不到，所以只能自己缓存。
        self.auth_state: Update | None = None
        self._closed = False

    # ------------------------------------------------------------------ #
    # 基础收发
    # ------------------------------------------------------------------ #
    def send(self, request: dict[str, Any]) -> None:
        self.api.send(self.client, request)

    def execute(self, request: dict[str, Any]) -> Update | None:
        """只对 TDLib 的同步请求有效（getOption / setOption 等）。"""
        return self.api.execute(self.client, request)

    def next_event(self, timeout: float = 1.0) -> Update | None:
        event = self.api.receive(self.client, timeout)
        if event is None:
            return None
        kind = event.get("@type")
        if kind == "updateFile":
            self.files[int(event["file"]["id"])] = event["file"]
        elif kind == "updateAuthorizationState":
            self.auth_state = event["authorization_state"]
        return event

    def call(self, request: dict[str, Any], timeout: float = 60.0) -> Update:
        """发送请求并等待带相同 @extra 的响应。"""
        extra = f"req{next(self._extra)}"
        self.send({**request, "@extra": extra})
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            event = self.next_event(0.5)
            if event is None or event.get("@extra") != extra:
                continue
            if event["@type"] == "error":
                raise TdlibError(event)
            return event
        raise TimeoutError(f"等待 {request['@type']} 响应超时（{timeout}s）")

    def fetch_auth_state(self, timeout: float = 15.0) -> Update | None:
        """异步查询当前授权状态（execute 对它是无效的）。"""
        if self.auth_state is not None:
            return self.auth_state
        try:
            state = self.call({"@type": "getAuthorizationState"}, timeout=timeout)
        except (TdlibError, TimeoutError):
            return self.auth_state
        self.auth_state = state
        return state

    # ------------------------------------------------------------------ #
    # 登录 / 登出
    # ------------------------------------------------------------------ #
    def _set_parameters(self) -> None:
        if getattr(self, "_params_sent", False):
            return
        self._params_sent = True
        api_id, api_hash = self.cfg.require_credentials()
        self.send({
            "@type": "setTdlibParameters",
            "use_message_database": True,
            "use_secret_chats": True,
            "api_id": api_id,
            "api_hash": api_hash,
            "system_language_code": "en",
            "device_model": "tgdl",
            "system_version": sys.platform,
            "application_version": "0.1.0",
            "enable_storage_optimizer": True,
            "ignore_file_names": False,
            "database_directory": str(self.cfg.tdlib_db),
            "files_directory": str(self.cfg.tdlib_files),
        })

    def _handle_state(
        self,
        state: dict[str, Any],
        *,
        phone: str | None,
        ask_code: Callable[[dict[str, Any]], str] | None,
        ask_password: Callable[[dict[str, Any]], str] | None,
        ask_name: Callable[[], tuple[str, str]] | None,
        on_state: Callable[[str], None] | None,
        force: bool = False,
    ) -> Update | None:
        name = state["@type"]
        if name == self._last_state and not force:
            return None
        self._last_state = name
        if on_state:
            on_state(name)

        if name == "authorizationStateWaitTdlibParameters":
            self._set_parameters()
        elif name == "authorizationStateWaitEncryptionKey":
            self.send({"@type": "checkDatabaseEncryptionKey", "encryption_key": ""})
        elif name == "authorizationStateWaitPhoneNumber":
            number = phone or input("手机号（含国家码，如 +8613800138000）: ").strip()
            self.send({"@type": "setAuthenticationPhoneNumber", "phone_number": number})
            print("[tdlib] 已提交手机号，等待 Telegram 下发验证码…")
        elif name == "authorizationStateWaitCode":
            info = state.get("code_info", {})
            kind = info.get("type", {}).get("@type", "")
            print(f"[tdlib] 验证码已发往 {info.get('phone_number', '')}（{kind}）")
            code = ask_code(state["code_info"]) if ask_code else input("验证码: ").strip()
            if code.strip().lower() in ("resend", "r"):
                self.send({"@type": "resendAuthenticationCode"})
                print("[tdlib] 已请求重新发送验证码")
                return None
            self.send({"@type": "checkAuthenticationCode", "code": code})
        elif name == "authorizationStateWaitPassword":
            hint = state.get("password_hint") or ""
            if hint:
                print(f"[tdlib] 两步验证密码提示: {hint}")
            password = ask_password(state) if ask_password else input("两步验证密码: ")
            self.send({"@type": "checkAuthenticationPassword", "password": password})
        elif name == "authorizationStateWaitRegistration":
            first, last = ask_name() if ask_name else ("tg", "user")
            self.send({"@type": "registerUser", "first_name": first, "last_name": last})
        elif name == "authorizationStateReady":
            return self.call({"@type": "getMe"})
        elif name == "authorizationStateClosed":
            raise TdlibError({"code": 500, "message": "TDLib 已关闭"})
        elif name == "authorizationStateWaitOtherDeviceConfirmation":
            print(f"请用已登录设备打开链接确认登录: {state.get('link', '')}")
        return None

    @staticmethod
    def _wait_seconds(message: str) -> int:
        """从 FLOOD_WAIT_12 / PHONE_CODE_WAIT_30 这类错误里取出需要等待的秒数。"""
        for token in message.replace(":", " ").replace("_", " ").split():
            if token.isdigit():
                return min(int(token), 120)
        return 0

    def authorize(
        self,
        *,
        phone: str | None = None,
        ask_code: Callable[[dict[str, Any]], str] | None = None,
        ask_password: Callable[[dict[str, Any]], str] | None = None,
        ask_name: Callable[[], tuple[str, str]] | None = None,
        timeout: float = 900.0,
        on_state: Callable[[str], None] | None = None,
    ) -> Update:
        """驱动 TDLib 的授权状态机，返回登录后的 user 对象。"""
        kwargs = dict(
            phone=phone, ask_code=ask_code, ask_password=ask_password,
            ask_name=ask_name, on_state=on_state,
        )

        # 关键：先把"已经收到过的状态"处理掉。
        # wait_ready() 可能已经消费了 waitPhoneNumber 这条 update，而
        # getAuthorizationState 在 TDLib 里不是同步请求（execute 拿不到），
        # 所以这里必须用缓存的 auth_state，否则会一直等一条永远不会再来的 update。
        current = self.fetch_auth_state()
        if current is not None:
            user = self._handle_state(current, force=True, **kwargs)
            if user is not None:
                return user

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            event = self.next_event(0.5)
            if event is None:
                continue

            if event["@type"] == "error":
                if event.get("@extra"):
                    continue
                message = str(event.get("message") or "")
                print(f"[tdlib] {message}")
                wait = self._wait_seconds(message)
                if wait:
                    print(f"[tdlib] 需要等待 {wait}s 才能重试…")
                    time.sleep(wait)
                # 输入错误（验证码/密码/手机号），重新走一遍当前状态
                self._last_state = None
                current = self.fetch_auth_state()
                if current is not None:
                    user = self._handle_state(current, force=True, **kwargs)
                    if user is not None:
                        return user
                continue

            if event["@type"] != "updateAuthorizationState":
                continue

            user = self._handle_state(event["authorization_state"], **kwargs)
            if user is not None:
                return user
        raise TimeoutError("登录超时")

    def is_authorized(self) -> bool:
        state = self.fetch_auth_state()
        return bool(state and state.get("@type") == "authorizationStateReady")

    def wait_ready(self, timeout: float = 90.0) -> bool:
        """恢复已有会话：跑到 authorizationStateReady 就返回 True。

        如果会话已失效（需要重新登录），返回 False，并把当前状态留在
        ``self.auth_state`` 里供 :meth:`authorize` 接着处理。
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            event = self.next_event(0.5)
            if event is None:
                continue
            if event["@type"] == "error":
                continue
            if event["@type"] != "updateAuthorizationState":
                continue
            state = event["authorization_state"]["@type"]
            if state == "authorizationStateWaitTdlibParameters":
                self._set_parameters()
            elif state == "authorizationStateWaitEncryptionKey":
                self.send({"@type": "checkDatabaseEncryptionKey", "encryption_key": ""})
            elif state == "authorizationStateReady":
                self._last_state = state
                return True
            elif state == "authorizationStateClosed":
                return False
            else:
                self._last_state = None
                return False
        return False

    def logout(self, timeout: float = 60.0) -> None:
        """退出登录：调用 logOut，等待 authorizationStateClosed。"""
        try:
            self.send({"@type": "logOut"})
        except TdlibError:
            pass
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            event = self.next_event(0.5)
            if event is None:
                continue
            if event["@type"] == "updateAuthorizationState":
                name = event["authorization_state"]["@type"]
                if name in ("authorizationStateLoggingOut", "authorizationStateClosing"):
                    continue
                if name == "authorizationStateClosed":
                    break
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.api.destroy(self.client)
        except Exception:  # noqa: BLE001
            pass

    def __enter__(self) -> "Tdlib":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # 文件
    # ------------------------------------------------------------------ #
    def get_message(self, chat_id: int, message_id: int, timeout: float = 60.0) -> Update:
        """按 chat_id:message_id 取消息。

        两个坑都要绕：
        1. TDLib 刚启动（或刚登录）时 chat 库还没加载完，会报 "Chat not found"，
           过一两秒就好 —— 所以这里退避重试，而不是直接失败；
        2. ``getMessage`` 只查本地库，本地没有就直接 404，不会回源 ——
           所以查不到时用 ``getChatHistory`` 从服务端拉这一条。
        """
        message_id = to_tdlib_message_id(int(message_id))
        request = {"@type": "getMessage", "chat_id": int(chat_id), "message_id": message_id}
        deadline = time.monotonic() + 15.0
        last: TdlibError | None = None

        while True:
            try:
                return self.call(request, timeout=timeout)
            except TdlibError as exc:
                last = exc
                if "chat not found" not in exc.message.lower():
                    if not _is_not_found(exc):
                        raise
                    break  # 消息级 404，交给下面的回源
                # chat 还没加载：试着建出来，然后等一会儿再问
                try:
                    self._create_chat(chat_id)
                except TdlibError:
                    pass
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.5)

        try:
            return self._fetch_message_remote(chat_id, message_id, timeout)
        except TdlibError:
            if last is not None:
                raise last
            raise

    def _fetch_message_remote(self, chat_id: int, message_id: int, timeout: float) -> Update:
        result = self.call(
            {
                "@type": "getChatHistory",
                "chat_id": int(chat_id),
                "from_message_id": int(message_id),
                "offset": 0,
                "limit": 1,
                "only_local": False,
            },
            timeout=timeout,
        )
        for message in result.get("messages", []):
            if int(message.get("id", 0)) == int(message_id):
                return message
        raise TdlibError({
            "code": 404,
            "message": f"消息不存在：chat_id={chat_id} message_id={message_id}"
                       "（可能已删除，或这个账号看不到它）",
        })

    def delete_file(self, file_id: int) -> None:
        """清掉 TDLib 的本地缓存（benchmark 时用来强制重新下载）。"""
        try:
            self.call({"@type": "deleteFile", "file_id": int(file_id)}, timeout=30)
        except (TdlibError, TimeoutError):
            pass

    def _create_chat(self, chat_id: int) -> None:
        if chat_id > 0:
            self.call({"@type": "createPrivateChat", "user_id": chat_id, "force": True}, timeout=30)
        elif str(chat_id).startswith("-100"):
            supergroup_id = -chat_id - 1_000_000_000_000
            self.call(
                {"@type": "createSupergroupChat", "supergroup_id": supergroup_id, "force": True},
                timeout=30,
            )
        else:
            raise TdlibError({
                "code": 400,
                "message": f"TDLib 里没有 chat_id={chat_id}，且普通群无法凭空创建；"
                           "请先在客户端打开一次这个会话，或改用 --engine fast",
            })

    def download(
        self,
        file: Update,
        dest: Path,
        *,
        timeout: float = 24 * 3600,
        on_progress: Callable[[int, int], None] | None = None,
        limit: int = 0,
    ) -> Path:
        """用 TDLib 自带的下载器下载（基准引擎）。

        ``limit`` 不为 0 时只下载前 limit 字节（benchmark 用）。
        """
        file_id = int(file["id"])
        dest = Path(dest)

        # 文件可能已经在 TDLib 的本地缓存里，那样 downloadFile 不会再发 updateFile，
        # 所以先查一次当前状态。
        try:
            current = self.call({"@type": "getFile", "file_id": file_id}, timeout=30)
            self.files[file_id] = current
            if current["local"]["is_downloading_completed"]:
                size = int(current["size"] or current["expected_size"])
                if on_progress:
                    on_progress(size, size)
                return _copy_to(Path(current["local"]["path"]), dest)
        except (TdlibError, TimeoutError):
            pass

        self.send({
            "@type": "downloadFile",
            "file_id": file_id,
            "priority": 32,
            "offset": 0,
            "limit": int(limit),
            "synchronous": False,
        })
        deadline = time.monotonic() + timeout
        last_report = 0.0
        last_size = -1
        stalled_since = time.monotonic()
        while time.monotonic() < deadline:
            event = self.next_event(0.25)
            if event is not None and event["@type"] == "error" and not event.get("@extra"):
                # 下载失败（文件不存在 / file_reference 过期等）会以独立 error 事件到达
                raise TdlibError(event)

            info = self.files.get(file_id)
            if info is None:
                continue
            local = info["local"]
            downloaded = int(local["downloaded_size"])
            if downloaded != last_size:
                last_size = downloaded
                stalled_since = time.monotonic()

            if on_progress:
                now = time.monotonic()
                if now - last_report > 0.2:
                    last_report = now
                    on_progress(downloaded, int(info["size"] or info["expected_size"]))

            if local["is_downloading_completed"]:
                return _copy_to(Path(local["path"]), dest)
            if limit and downloaded >= limit:
                # 部分下载（benchmark），不复制文件
                return dest

            if time.monotonic() - stalled_since > 30:
                raise TimeoutError(
                    f"TDLib 下载 30 秒无进展（已下载 {downloaded} 字节），已中止"
                )
        raise TimeoutError("TDLib 下载超时")


def _copy_to(source: Path, dest: Path) -> Path:
    if source.resolve() == dest.resolve():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    _copy_file(source, dest)
    return dest


def _copy_file(src: Path, dst: Path, chunk: int = 8 << 20) -> None:
    with src.open("rb") as fin, dst.open("wb") as fout:
        while True:
            block = fin.read(chunk)
            if not block:
                break
            fout.write(block)
