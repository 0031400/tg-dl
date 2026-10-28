"""本地 HTTP 服务：给油猴脚本提供下载 / 登录 / 设置接口。

只监听 127.0.0.1，并且要求一个 token（写在 ``.tgdl/server.token``），
避免随便哪个网页都能往你的下载队列里塞东西。

接口一览（除 /api/ping 外都要带 ``X-Tgdl-Token``）：
    GET  /api/ping                 健康检查
    GET  /api/state                配置 + 设置 + 任务列表 + 登录状态
    POST /api/settings             改设置（并发、auth key 数、输出目录、固定并发）
    POST /api/login                登录（分阶段：phone -> code -> password）
    POST /api/tasks                新建下载任务 {chat_id, message_id}
    POST /api/tasks/cancel         取消任务 {id}
    POST /api/tasks/clear          清掉已完成/失败的任务
"""

from __future__ import annotations

import json
import queue
import secrets
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from . import fast
from .config import Config, ConfigError
from .util import human_size, parse_target

VERSION = "1.0"


# --------------------------------------------------------------------------- #
# 设置
# --------------------------------------------------------------------------- #

@dataclass
class Settings:
    """全部是固定值，不做动态调整（--fixed 的语义）。"""

    download_dir: str = ""
    connections: int = 16      # 一条连接上并行多少个请求
    auth_keys: int = 3         # 几条独立 auth key
    fixed: bool = True         # 固定并发，关掉自适应
    chunk: str = "512K"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _load_settings(cfg: Config) -> Settings:
    path = cfg.data_dir / "server.json"
    data: dict[str, Any] = {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        pass
    st = Settings()
    if isinstance(data, dict):
        for key in ("download_dir", "chunk"):
            if isinstance(data.get(key), str):
                setattr(st, key, data[key])
        for key in ("connections", "auth_keys"):
            if isinstance(data.get(key), int) and data[key] > 0:
                setattr(st, key, data[key])
        if isinstance(data.get("fixed"), bool):
            st.fixed = data["fixed"]
    if not st.download_dir:
        st.download_dir = str(cfg.data_dir / "downloads")
    return st


def _save_settings(cfg: Config, st: Settings) -> None:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    (cfg.data_dir / "server.json").write_text(
        json.dumps(st.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )


# --------------------------------------------------------------------------- #
# 任务
# --------------------------------------------------------------------------- #

@dataclass
class Task:
    id: str
    chat_id: int
    message_id: int
    status: str = "queued"        # queued | running | done | failed | cancelled
    name: str = ""
    size: int = 0
    done: int = 0
    speed: float = 0.0
    error: str = ""
    path: str = ""
    created: float = field(default_factory=time.time)
    updated: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["target"] = f"{self.chat_id}:{self.message_id}"
        return d


class TaskManager:
    """单线程顺序下载：每个任务自己已经开了 N 个在途请求，再并行只会互相抢额度。"""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._tasks: dict[str, Task] = {}
        self._order: list[str] = []
        self._lock = threading.Lock()
        self._queue: queue.Queue[str] = queue.Queue()
        self._worker = threading.Thread(target=self._loop, daemon=True)
        self._worker.start()

    # ---- 对外 ----
    def add(self, chat_id: int, message_id: int) -> Task:
        task = Task(id=uuid.uuid4().hex[:8], chat_id=chat_id, message_id=message_id)
        with self._lock:
            self._tasks[task.id] = task
            self._order.append(task.id)
        self._queue.put(task.id)
        return task

    def cancel(self, task_id: str) -> bool:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None or task.status in ("done", "failed", "cancelled"):
                return False
            task.status = "cancelled"
            task.updated = time.time()
            return True

    def clear(self) -> int:
        with self._lock:
            keep = []
            removed = 0
            for tid in self._order:
                if self._tasks[tid].status in ("done", "failed", "cancelled"):
                    del self._tasks[tid]
                    removed += 1
                else:
                    keep.append(tid)
            self._order = keep
            return removed

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._tasks[tid].to_dict() for tid in self._order if tid in self._tasks]

    # ---- 内部 ----
    def _update(self, task_id: str, **fields: Any) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return
            for key, value in fields.items():
                setattr(task, key, value)
            task.updated = time.time()

    def _get(self, task_id: str) -> Task | None:
        with self._lock:
            return self._tasks.get(task_id)

    def _loop(self) -> None:
        while True:
            task_id = self._queue.get()
            task = self._get(task_id)
            if task is None or task.status == "cancelled":
                continue
            self._run(task)

    def _run(self, task: Task) -> None:
        st = _load_settings(self.cfg)
        out_dir = Path(st.download_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        last = [0.0, time.time()]

        def on_progress(done: int, total: int) -> None:
            now = time.time()
            if now - last[1] >= 0.5:
                speed = (done - last[0]) / (now - last[1])
                last[0], last[1] = done, now
                self._update(task.id, done=done, size=total, speed=speed)

        self._update(task.id, status="running")

        # 先把文件名/大小拿到，列表里好看一点
        try:
            info = asyncio_run(
                fast.message_info(self.cfg, task.chat_id, task.message_id)
            )
            self._update(task.id, name=info["name"], size=int(info["size"]))
        except Exception:  # noqa: BLE001
            pass

        try:
            path, _elapsed, name, size = asyncio_run(
                fast.download_message(
                    self.cfg,
                    task.chat_id,
                    task.message_id,
                    out_dir,
                    connections=st.connections,
                    chunk=parse_size_safe(st.chunk),
                    adaptive=not st.fixed,
                    auth_keys=st.auth_keys,
                    on_progress=on_progress,
                )
            )
            self._update(
                task.id, status="done", path=str(path), name=name,
                size=size, done=size, speed=0.0,
            )
        except Exception as exc:  # noqa: BLE001
            self._update(task.id, status="failed", error=str(exc), speed=0.0)


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


def parse_size_safe(text: str) -> int:
    from .util import parse_size

    try:
        return parse_size(text)
    except Exception:  # noqa: BLE001
        return 512 * 1024


# --------------------------------------------------------------------------- #
# 登录（分阶段，用线程事件把回调桥接成 HTTP 请求）
# --------------------------------------------------------------------------- #

class LoginBridge:
    def __init__(self):
        self.phase = "idle"          # idle | phone | code | password | done | failed
        self.message = ""
        self.user = ""
        self._event = threading.Event()
        self._value = ""
        self._thread: threading.Thread | None = None

    def snapshot(self) -> dict[str, Any]:
        return {"phase": self.phase, "message": self.message, "user": self.user}

    def submit(self, value: str) -> bool:
        if self.phase not in ("code", "password"):
            return False
        self._value = value
        self._event.set()
        return True

    def start(self, cfg: Config, phone: str) -> None:
        if self.phase in ("phone", "code", "password"):
            raise RuntimeError("登录已经在进行中")
        self.phase = "phone"
        self.message = "正在发送验证码…"
        self.user = ""
        self._thread = threading.Thread(target=self._run, args=(cfg, phone), daemon=True)
        self._thread.start()

    def _ask(self, phase: str, message: str) -> str:
        self.phase = phase
        self.message = message
        self._event.clear()
        self._value = ""
        if not self._event.wait(300):
            raise TimeoutError("等待输入超时")
        return self._value

    def _run(self, cfg: Config, phone: str) -> None:
        try:
            user = asyncio_run(
                fast.login(
                    cfg,
                    phone=phone,
                    ask_code=lambda: self._ask("code", "请输入验证码"),
                    ask_password=lambda: self._ask("password", "请输入两步验证密码"),
                )
            )
            self.phase = "done"
            self.message = "登录成功"
            self.user = user
        except Exception as exc:  # noqa: BLE001
            self.phase = "failed"
            self.message = str(exc)


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #

class App:
    def __init__(self, cfg: Config, token: str):
        self.cfg = cfg
        self.token = token
        self.tasks = TaskManager(cfg)
        self.login = LoginBridge()

    def state(self) -> dict[str, Any]:
        cfg = self.cfg
        try:
            fast_user = asyncio_run(fast.whoami(cfg))
        except Exception:  # noqa: BLE001
            fast_user = None
        try:
            cached_keys = fast.AuthKeyStore(cfg.authkeys_file).count()
        except Exception:  # noqa: BLE001
            cached_keys = 0
        return {
            "version": VERSION,
            "config": {
                "api_id": cfg.api_id,
                "api_hash_set": bool(cfg.api_hash),
                "data_dir": str(cfg.data_dir),
            },
            "settings": _load_settings(cfg).to_dict(),
            "login": self.login.snapshot(),
            "account": {"fast": fast_user, "auth_keys_cached": cached_keys},
            "tasks": self.tasks.list(),
        }


def make_handler(app: App):
    class Handler(BaseHTTPRequestHandler):
        server_version = f"tgdl/{VERSION}"

        def log_message(self, fmt: str, *args: Any) -> None:  # 静音
            return

        # ---- 工具 ----
        def _send(self, payload: Any, code: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Tgdl-Token")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.end_headers()
            self.wfile.write(body)

        def _body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if not length:
                return {}
            try:
                data = json.loads(self.rfile.read(length).decode("utf-8"))
                return data if isinstance(data, dict) else {}
            except Exception:  # noqa: BLE001
                return {}

        def _authed(self) -> bool:
            if not app.token:
                return True
            got = self.headers.get("X-Tgdl-Token") or ""
            return secrets.compare_digest(got, app.token)

        def do_OPTIONS(self) -> None:  # noqa: N802
            self._send({"ok": True})

        def _send_js(self, text: str) -> None:
            body = text.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            # 让油猴可以直接从这个地址安装/更新脚本
            if path in ("/tgdl.user.js", "/tgdl.meta.js"):
                script = Path(__file__).resolve().parents[2] / "extension" / "monkey" / "tgdl.user.js"
                if not script.is_file():
                    self._send({"error": "脚本文件不存在"}, 404)
                    return
                self._send_js(script.read_text(encoding="utf-8"))
                return
            if path == "/api/ping":
                self._send({"ok": True, "version": VERSION, "need_token": bool(app.token)})
                return
            if not self._authed():
                self._send({"error": "token 不对"}, 401)
                return
            if path == "/api/state":
                self._send(app.state())
                return
            self._send({"error": "not found"}, 404)

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if not self._authed():
                self._send({"error": "token 不对"}, 401)
                return
            body = self._body()

            if path == "/api/settings":
                st = _load_settings(app.cfg)
                if isinstance(body.get("connections"), int) and body["connections"] > 0:
                    st.connections = min(64, body["connections"])
                if isinstance(body.get("auth_keys"), int) and body["auth_keys"] > 0:
                    st.auth_keys = min(8, body["auth_keys"])
                if isinstance(body.get("fixed"), bool):
                    st.fixed = body["fixed"]
                if isinstance(body.get("download_dir"), str) and body["download_dir"].strip():
                    st.download_dir = body["download_dir"].strip()
                if isinstance(body.get("chunk"), str) and body["chunk"].strip():
                    st.chunk = body["chunk"].strip()
                _save_settings(app.cfg, st)
                self._send({"ok": True, "settings": st.to_dict()})
                return

            if path == "/api/login":
                try:
                    if app.login.phase in ("code", "password"):
                        value = str(body.get("value") or "")
                        ok = app.login.submit(value)
                        self._send({"ok": ok, "login": app.login.snapshot()})
                        return
                    phone = str(body.get("phone") or "").strip()
                    if not phone:
                        self._send({"error": "需要 phone"}, 400)
                        return
                    app.login.start(app.cfg, phone)
                    time.sleep(0.3)
                    self._send({"ok": True, "login": app.login.snapshot()})
                except (RuntimeError, ConfigError) as exc:
                    self._send({"error": str(exc)}, 400)
                return

            if path == "/api/resolve":
                username = str(body.get("username") or "").strip()
                if not username:
                    self._send({"error": "需要 username"}, 400)
                    return
                try:
                    chat_id = asyncio_run(fast.resolve_chat(app.cfg, username))
                except Exception as exc:  # noqa: BLE001
                    self._send({"error": str(exc)}, 400)
                    return
                self._send({"ok": True, "chat_id": chat_id})
                return

            if path == "/api/tasks":
                target = str(body.get("target") or "").strip()
                try:
                    if target:
                        chat_id, message_id = parse_target(target)
                    else:
                        chat_id = int(body["chat_id"])
                        message_id = int(body["message_id"])
                except (ValueError, KeyError) as exc:
                    self._send({"error": f"目标不合法: {exc}"}, 400)
                    return
                task = app.tasks.add(chat_id, message_id)
                self._send({"ok": True, "task": task.to_dict()})
                return

            if path == "/api/tasks/cancel":
                ok = app.tasks.cancel(str(body.get("id") or ""))
                self._send({"ok": ok})
                return

            if path == "/api/tasks/clear":
                self._send({"ok": True, "removed": app.tasks.clear()})
                return

            self._send({"error": "not found"}, 404)

    return Handler


def serve(cfg: Config, *, host: str = "127.0.0.1", port: int = 8765,
          token: str | None = None) -> None:
    """阻塞运行本地服务。"""
    cfg.ensure_dirs()
    token_file = cfg.data_dir / "server.token"
    if token is None:
        if token_file.is_file():
            token = token_file.read_text(encoding="utf-8").strip()
        else:
            token = secrets.token_urlsafe(18)
            token_file.write_text(token + "\n", encoding="utf-8")

    app = App(cfg, token)
    httpd = ThreadingHTTPServer((host, port), make_handler(app))
    print(f"tgdl 本地服务已启动: http://{host}:{port}")
    print(f"token: {token}")
    print(f"（也写在 {token_file}）")
    print("把这个 token 填进油猴面板的设置里；Ctrl+C 停止。")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
    finally:
        httpd.server_close()
