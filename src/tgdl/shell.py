"""常驻连接池 + 交互式下载（用来测「服务态」速度）。

和 `tgdl download` 的区别：

* `download` 每次都要连会话 DC、取消息、建 auth key 连接，启动 2~4 秒；
* `shell` 只建一次连接池，之后每次下载直接复用那几条连接，
  所以能测出「纯传输」的速度，也更接近常驻服务的真实表现。

命令一览见 `help`。
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import fast
from .config import Config, remember_dc
from .util import human_size, parse_target


@dataclass
class PoolOptions:
    auth_keys: int = 3
    connections: int = 16
    chunk: int = fast.DEFAULT_CHUNK
    adaptive: bool = False
    dc_id: int | None = None


class FastPool:
    """一次建好、长期复用的连接池。"""

    def __init__(self, cfg: Config, opts: PoolOptions):
        self.cfg = cfg
        self.opts = opts
        self.client: Any = None
        self.senders: list = []
        self.dc_id: int | None = opts.dc_id
        self.store = fast.AuthKeyStore(cfg.authkeys_file)
        self.build_seconds = 0.0
        self.downloads = 0
        self.bytes_total = 0
        self._stack: contextlib.AsyncExitStack | None = None

    # ---- 生命周期 ----
    async def start(self) -> None:
        fast.check_crypto()
        self.client = fast.make_client(self.cfg)
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise fast.FastError("fast 引擎未登录，先执行 `tgdl login --engine fast`")
        # 会话 DC 记进配置，下次不用连上去才知道
        remember_dc(self.cfg, session_dc=self.client.session.dc_id)
        if self.dc_id is None:
            self.dc_id = self.cfg.pool_dc   # 上次下载用的 DC，直接预热
        if self.dc_id is not None:
            await self._build(self.dc_id)

    async def close(self) -> None:
        if self._stack is not None:
            with contextlib.suppress(Exception):
                await self._stack.aclose()
            self._stack = None
        self.senders = []
        if self.client is not None:
            with contextlib.suppress(Exception):
                await self.client.disconnect()
            self.client = None

    async def _build(self, dc_id: int) -> None:
        if self._stack is not None:
            with contextlib.suppress(Exception):
                await self._stack.aclose()
        self._stack = contextlib.AsyncExitStack()
        started = time.monotonic()
        self.senders = await fast._build_pairs(
            self.client,
            [self.client],
            dc_id,
            dc_id,
            self.opts.auth_keys,
            self.store,
            self._stack,
        )
        self.dc_id = dc_id
        self.build_seconds = time.monotonic() - started

    async def ensure_dc(self, dc_id: int) -> None:
        """文件在别的 DC 时重建连接池（同 DC 直接复用）。"""
        if self.senders and self.dc_id == dc_id:
            return
        await self._build(dc_id)

    # ---- 下载 ----
    async def fetch_meta(self, chat_id: int, message_id: int):
        _, _, _, types = fast._telethon()
        message = await fast._fetch_message(self.client, chat_id, message_id)
        location, size, name, dc_id = fast._location_from_message(message, types)
        return location, int(size), name, dc_id

    async def download(
        self,
        chat_id: int,
        message_id: int,
        dest: Path,
        *,
        resume_offsets: set[int] | None = None,
        on_progress=None,
        on_chunk=None,
    ) -> tuple[Path, int, str, float]:
        location, size, name, dc_id = await self.fetch_meta(chat_id, message_id)
        dest = Path(dest)
        if dest.is_dir():
            dest = dest / name          # 没给 -o 时 dest 就是下载目录
        await self.ensure_dc(dc_id)
        remember_dc(self.cfg, pool_dc=dc_id)
        started = time.monotonic()
        path = await fast.download_location(
            self.client,
            location,
            size,
            dest,
            dc_id=dc_id,
            connections=self.opts.connections,
            chunk=self.opts.chunk,
            adaptive=self.opts.adaptive,
            senders=self.senders,
            resume_offsets=resume_offsets,
            on_chunk=on_chunk,
            on_progress=on_progress,
        )
        elapsed = time.monotonic() - started
        self.downloads += 1
        self.bytes_total += size
        return Path(path), size, name, elapsed

    # ---- 展示 ----
    def describe(self) -> str:
        if not self.senders:
            return "连接池未建立（第一次 dl 时会按文件所在 DC 建）"
        return (
            f"DC{self.dc_id}｜{len(self.senders)} 条 auth key 连接"
            f"｜在途 {self.opts.connections}"
            f"｜分片 {human_size(self.opts.chunk)}"
            f"｜{'自适应' if self.opts.adaptive else '固定并发'}"
            f"｜建池 {self.build_seconds:.2f}s"
            f"｜已完成 {self.downloads} 个，共 {human_size(self.bytes_total)}"
        )


@dataclass
class Job:
    id: str
    target: str
    chat_id: int
    message_id: int
    dest: Path
    status: str = "running"          # running | done | stopped | failed
    done: int = 0
    size: int = 0
    name: str = ""
    path: Path | None = None
    error: str = ""
    started: float = field(default_factory=time.monotonic)
    elapsed: float = 0.0
    samples: list[tuple[float, int]] = field(default_factory=list)
    offsets: set[int] = field(default_factory=set)
    resume: set[int] | None = None
    task: asyncio.Task | None = None

    def speed(self, window: float = 2.0) -> float:
        now = time.monotonic()
        recent = [(t, d) for t, d in self.samples if now - t <= window]
        if len(recent) < 2:
            return 0.0
        (t0, d0), (t1, d1) = recent[0], recent[-1]
        return (d1 - d0) / max(t1 - t0, 1e-6)

    def progress_line(self) -> str:
        if self.status == "done":
            avg = self.size / self.elapsed if self.elapsed > 0 else 0
            return (
                f"  ✔ {self.target}  {human_size(self.size)}  "
                f"{self.elapsed:.2f}s  平均 {human_size(avg)}/s  → {self.path}"
            )
        if self.status == "stopped":
            return f"  ■ {self.target}  已停止于 {human_size(self.done)}（分片已保留）"
        if self.status == "failed":
            return f"  ✘ {self.target}  失败：{self.error}"
        pct = (self.done / self.size * 100) if self.size else 0
        return (
            f"  … {self.target}  {human_size(self.done)}/{human_size(self.size)}"
            f"  {pct:5.1f}%  {human_size(self.speed())}/s"
        )


HELP = """\
命令：
  dl <chat:mid> [-o 路径]   后台下载（连接池复用，立即返回）
  dlw <chat:mid> [-o 路径]  前台下载（带实时进度，Ctrl+C 中断）
  st                        查看所有任务状态（含瞬时速度）
  stop [任务号]             中断下载（保留已下分片，可再 dl 续传）
  rm <任务号|chat:mid|路径> 中断（如果还在跑）并删除文件
  ls                        列出下载目录
  pool                      连接池信息（DC / key 数 / 建池耗时）
  help                      显示本帮助
  q / quit                  退出（会关掉连接池）
"""


class Shell:
    def __init__(self, cfg: Config, opts: PoolOptions, out_dir: Path):
        self.cfg = cfg
        self.opts = opts
        self.out_dir = Path(out_dir)
        self.pool = FastPool(cfg, opts)
        self.jobs: dict[str, Job] = {}
        self.by_target: dict[str, Job] = {}

    # ---- 任务 ----
    def _new_job(self, target: str, dest: Path | None) -> Job:
        chat_id, message_id = parse_target(target)
        job = Job(
            id=uuid.uuid4().hex[:6],
            target=f"{chat_id}:{message_id}",
            chat_id=chat_id,
            message_id=message_id,
            dest=dest or self.out_dir,
        )
        self.jobs[job.id] = job
        self.by_target[job.target] = job
        return job

    async def _run_job(self, job: Job) -> None:
        def on_progress(done: int, total: int) -> None:
            job.done = done
            job.size = total
            job.samples.append((time.monotonic(), done))
            if len(job.samples) > 400:
                del job.samples[:200]

        def on_chunk(offset: int, _length: int) -> None:
            job.offsets.add(offset)

        try:
            path, size, name, elapsed = await self.pool.download(
                job.chat_id,
                job.message_id,
                job.dest,
                resume_offsets=job.resume,
                on_progress=on_progress,
                on_chunk=on_chunk,
            )
        except asyncio.CancelledError:
            job.status = "stopped"
            job.elapsed = time.monotonic() - job.started
            raise
        except Exception as exc:  # noqa: BLE001
            job.status = "failed"
            job.error = str(exc)
            job.elapsed = time.monotonic() - job.started
            print(f"\n[job {job.id}] 失败：{exc}", flush=True)
            return
        job.status = "done"
        job.path = path
        job.name = name
        job.size = size
        job.done = size
        job.elapsed = elapsed
        print("\n" + job.progress_line(), flush=True)

    def start_job(self, target: str, dest: Path | None, *, wait: bool) -> Job:
        # 同一个 target 之前被中断过 → 从它的分片继续
        chat_id, message_id = parse_target(target)
        prev = self.by_target.get(f"{chat_id}:{message_id}")
        resume = set(prev.offsets) if prev is not None and prev.offsets else None
        job = self._new_job(target, dest)
        job.resume = resume
        job.task = asyncio.create_task(self._run_job(job))
        if wait:
            return job
        print(f"[job {job.id}] 已开始：{job.target}（用 st 看进度，stop 中断）", flush=True)
        return job

    # ---- 命令 ----
    async def cmd_dl(self, args: list[str], *, wait: bool) -> None:
        if not args:
            print("用法：dl <chat_id:message_id> [-o 路径]")
            return
        target = args[0]
        dest = None
        if "-o" in args:
            idx = args.index("-o")
            if idx + 1 < len(args):
                dest = Path(args[idx + 1])
        try:
            parse_target(target)
        except ValueError as exc:
            print(f"参数错误：{exc}")
            return
        job = self.start_job(target, dest, wait=wait)
        if wait:
            try:
                await job.task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                pass
            if job.status != "done":
                print("  " + job.progress_line().strip(), flush=True)

    def cmd_st(self) -> None:
        if not self.jobs:
            print("  还没有任务")
            return
        for job in self.jobs.values():
            print(job.progress_line())

    def _pick(self, args: list[str]) -> Job | None:
        if args:
            key = args[0]
            if key in self.jobs:
                return self.jobs[key]
            if key in self.by_target:
                return self.by_target[key]
            for job in self.jobs.values():
                if job.path and str(job.path) == key:
                    return job
            print(f"  找不到任务：{key}")
            return None
        running = [j for j in self.jobs.values() if j.status == "running"]
        if not running:
            print("  当前没有正在下载的任务")
            return None
        return running[-1]

    def cmd_stop(self, args: list[str]) -> None:
        job = self._pick(args)
        if job is None:
            return
        if job.status != "running" or job.task is None:
            print(f"  [job {job.id}] 当前状态 {job.status}，无需中断")
            return
        job.task.cancel()
        print(f"  [job {job.id}] 已发送中断，分片保留（再 dl 会续传）")

    async def cmd_rm(self, args: list[str]) -> None:
        if not args:
            print("用法：rm <任务号|chat:mid|路径>")
            return
        key = args[0]
        job = self.jobs.get(key) or self.by_target.get(key)
        if job is not None:
            if job.status == "running" and job.task is not None:
                job.task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await job.task
                print(f"  [job {job.id}] 已中断")
            target = job.path or job.dest
            removed = _unlink(target)
            self.jobs.pop(job.id, None)
            self.by_target.pop(job.target, None)
            print(f"  {'已删除 ' + str(target) if removed else '文件不存在：' + str(target)}")
            return
        path = Path(key)
        if _unlink(path):
            print(f"  已删除 {path}")
        else:
            print(f"  文件不存在：{path}")

    def cmd_ls(self) -> None:
        if not self.out_dir.is_dir():
            print(f"  {self.out_dir} 不存在")
            return
        items = sorted(
            (p for p in self.out_dir.iterdir() if p.is_file()),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not items:
            print("  （空）")
            return
        for p in items[:30]:
            print(f"  {human_size(p.stat().st_size):>10}  {p.name}")

    async def loop(self) -> None:
        took = time.monotonic()
        await self.pool.start()
        took = time.monotonic() - took
        print(f"连接池就绪（{took:.2f}s）。输入 help 看命令。", flush=True)
        if self.pool.senders:
            print("  " + self.pool.describe(), flush=True)
        else:
            print("  提示：连接池会在第一次 dl 时按文件所在 DC 建立", flush=True)

        while True:
            try:
                line = await asyncio.to_thread(input, "tgdl> ")
            except (EOFError, KeyboardInterrupt):
                print()
                break
            parts = line.strip().split()
            if not parts:
                continue
            cmd, args = parts[0].lower(), parts[1:]
            try:
                if cmd in ("q", "quit", "exit"):
                    break
                if cmd == "help":
                    print(HELP)
                elif cmd == "dl":
                    await self.cmd_dl(args, wait=False)
                elif cmd in ("dlw", "dl-wait"):
                    await self.cmd_dl(args, wait=True)
                elif cmd in ("st", "status", "ls-tasks"):
                    self.cmd_st()
                elif cmd == "stop":
                    self.cmd_stop(args)
                elif cmd == "rm":
                    await self.cmd_rm(args)
                elif cmd == "ls":
                    self.cmd_ls()
                elif cmd == "pool":
                    print("  " + self.pool.describe())
                else:
                    print(f"  未知命令：{cmd}（help 看用法）")
            except Exception as exc:  # noqa: BLE001
                print(f"  出错：{exc}")

        for job in self.jobs.values():
            if job.status == "running" and job.task is not None:
                job.task.cancel()
        await asyncio.sleep(0)
        await self.pool.close()


def _unlink(path: Path) -> bool:
    try:
        Path(path).unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


async def run(cfg: Config, opts: PoolOptions, out_dir: Path) -> None:
    shell = Shell(cfg, opts, out_dir)
    await shell.loop()
