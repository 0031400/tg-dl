"""tgdl 命令行入口。"""

from __future__ import annotations

import asyncio
import tempfile
import time
from pathlib import Path
from typing import Any, Optional

import typer
from rich import box
from rich.table import Table

from . import __version__, fast
from .config import DEFAULT_TDJSON, Config, ConfigError, load_config, write_config
from .tdjson import Tdlib, TdlibError, TdlibLoadError, file_from_message
from .util import ProgressBar, console, err_console, human_size, parse_size, parse_target

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Telegram 文件下载器：TDLib 引擎 + 更快的原生 MTProto 并行引擎。",
)

_OPTIONS: dict[str, Any] = {"config": None}


@app.callback()
def _root(
    config: Optional[Path] = typer.Option(
        None, "--config", "-c", envvar="TGDL_CONFIG", help="配置文件路径"
    ),
) -> None:
    _OPTIONS["config"] = config


def _load() -> Config:
    try:
        return load_config(_OPTIONS["config"])
    except ConfigError as exc:
        err_console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc


def _run(coro):
    return asyncio.run(coro)


def _user_label(user: dict[str, Any]) -> str:
    name = " ".join(filter(None, [user.get("first_name"), user.get("last_name")]))
    username = user.get("username")
    handle = f"@{username}" if username else f"id={user.get('id')}"
    return f"{name} {handle}".strip()


def _fail(message: str, code: int = 1) -> None:
    err_console.print(f"[red]{message}[/red]")
    raise typer.Exit(code)


# --------------------------------------------------------------------------- #
# init
# --------------------------------------------------------------------------- #

@app.command()
def init(
    api_id: Optional[int] = typer.Option(None, "--api-id", help="my.telegram.org 的 api_id"),
    api_hash: Optional[str] = typer.Option(None, "--api-hash", help="my.telegram.org 的 api_hash"),
    tdjson: Optional[Path] = typer.Option(None, "--tdjson", help="tdjson 动态库路径"),
    data_dir: Optional[Path] = typer.Option(None, "--data-dir", help="数据目录（会话/缓存）"),
    force: bool = typer.Option(False, "--force", help="覆盖已有配置"),
) -> None:
    """写入配置文件（默认 ./.tgdl/config.toml）。"""
    path = _OPTIONS["config"] or (Path.cwd() / ".tgdl" / "config.toml")
    if path.exists() and not force:
        _fail(f"{path} 已存在，加 --force 覆盖")
    write_config(
        path,
        api_id=api_id,
        api_hash=api_hash,
        tdjson=tdjson or DEFAULT_TDJSON,
        data_dir=data_dir or path.parent,
    )
    console.print(f"[green]已写入配置[/green] {path}")
    if not api_id or not api_hash:
        console.print("[yellow]还没填 api_id/api_hash，可从 https://my.telegram.org 申请[/yellow]")


# --------------------------------------------------------------------------- #
# login / logout / whoami
# --------------------------------------------------------------------------- #

def _login_tdlib(cfg: Config, phone: str | None, verbosity: int) -> None:
    with Tdlib(cfg, verbosity=verbosity) as td:
        if td.wait_ready():
            console.print(f"[green]tdlib 已登录[/green] {_user_label(td.call({'@type': 'getMe'}))}")
            return
        user = td.authorize(
            phone=phone,
            ask_code=lambda info: typer.prompt(f"验证码（{info.get('phone_number', '')}）"),
            ask_password=lambda _state: typer.prompt("两步验证密码", hide_input=True),
        )
        console.print(f"[green]tdlib 登录成功[/green] {_user_label(user)}")


@app.command()
def login(
    engine: str = typer.Option("all", "--engine", help="tdlib | fast | all"),
    phone: Optional[str] = typer.Option(None, "--phone", help="手机号（含国家码）"),
    session: int = typer.Option(
        1, "--session",
        help="fast 引擎的会话序号（1=fast，2=fast2…）。多会话可叠加吞吐",
    ),
    verbosity: int = typer.Option(1, "--verbosity", help="TDLib 日志级别"),
) -> None:
    """登录（tdlib 引擎 / fast 引擎，各自维护自己的会话）。"""
    cfg = _load()
    engine = engine.lower()
    if engine not in ("all", "tdlib", "fast"):
        _fail("--engine 只能是 tdlib / fast / all")
    try:
        if engine in ("all", "tdlib"):
            _login_tdlib(cfg, phone, verbosity)
        if engine in ("all", "fast"):
            message = _run(
                fast.login(
                    cfg,
                    phone=phone,
                    ask_code=lambda: typer.prompt("验证码"),
                    ask_password=lambda: typer.prompt("两步验证密码", hide_input=True),
                    session_index=session,
                )
            )
            console.print(f"[green]{message}[/green]")
    except (TdlibLoadError, TdlibError, fast.FastError, TimeoutError) as exc:
        _fail(str(exc))


@app.command()
def logout(
    engine: str = typer.Option("all", "--engine", help="tdlib | fast | all"),
    verbosity: int = typer.Option(1, "--verbosity"),
) -> None:
    """退出登录并清理本地会话。"""
    cfg = _load()
    engine = engine.lower()
    try:
        if engine in ("all", "tdlib"):
            with Tdlib(cfg, verbosity=verbosity) as td:
                if td.wait_ready(60):
                    td.logout()
                    console.print("[green]tdlib 已退出登录[/green]")
                else:
                    console.print("[yellow]tdlib 当前未登录[/yellow]")
        if engine in ("all", "fast"):
            console.print(f"[green]{_run(fast.logout(cfg))}[/green]")
    except (TdlibLoadError, TdlibError, fast.FastError) as exc:
        _fail(str(exc))


@app.command()
def whoami(verbosity: int = typer.Option(1, "--verbosity")) -> None:
    """查看两个引擎的登录状态。"""
    cfg = _load()
    table = Table(box=box.SIMPLE, show_header=True, header_style="bold")
    table.add_column("引擎")
    table.add_column("状态")
    try:
        with Tdlib(cfg, verbosity=verbosity) as td:
            if td.wait_ready(60):
                table.add_row("tdlib", _user_label(td.call({"@type": "getMe"})))
            else:
                table.add_row("tdlib", "[yellow]未登录[/yellow]")
    except (TdlibLoadError, TdlibError) as exc:
        table.add_row("tdlib", f"[red]{exc}[/red]")
    try:
        me = _run(fast.whoami(cfg))
        table.add_row("fast", me or "[yellow]未登录[/yellow]")
    except fast.FastError as exc:
        table.add_row("fast", f"[red]{exc}[/red]")
    console.print(table)


# --------------------------------------------------------------------------- #
# info
# --------------------------------------------------------------------------- #

def _info_fast(cfg: Config, chat_id: int, message_id: int) -> dict:
    return _run(fast.message_info(cfg, chat_id, message_id))


def _info_tdlib(cfg: Config, chat_id: int, message_id: int, verbosity: int) -> dict:
    with Tdlib(cfg, verbosity=verbosity) as td:
        if not td.wait_ready():
            raise TdlibError({"code": 401, "message": "tdlib 未登录"})
        message = td.get_message(chat_id, message_id)
        file, name, kind = file_from_message(message)
        return {
            "chat_id": chat_id,
            "message_id": message_id,
            "name": name,
            "size": int(file.get("size") or file.get("expected_size") or 0),
            "kind": kind,
            "dc_id": None,
            "mime": None,
        }


@app.command()
def info(
    target: str = typer.Argument(..., help="chat_id:message_id"),
    engine: str = typer.Option("auto", "--engine", help="auto | fast | tdlib"),
    verbosity: int = typer.Option(1, "--verbosity"),
) -> None:
    """查看 chat_id:message_id 里那个文件的信息（不下载）。"""
    cfg = _load()
    try:
        chat_id, message_id = parse_target(target)
    except ValueError as exc:
        _fail(str(exc), 2)
        return

    engine = engine.lower()
    if engine == "auto":
        engine = "fast" if _run(fast.whoami(cfg)) else "tdlib"

    try:
        data = (
            _info_fast(cfg, chat_id, message_id)
            if engine == "fast"
            else _info_tdlib(cfg, chat_id, message_id, verbosity)
        )
    except (fast.FastError, TdlibError, TdlibLoadError, TimeoutError) as exc:
        _fail(str(exc))
        return

    table = Table(box=box.SIMPLE, show_header=False)
    table.add_column("字段", style="cyan", no_wrap=True)
    table.add_column("值")
    for key, value in [
        ("chat_id", data["chat_id"]),
        ("message_id", data["message_id"]),
        ("文件名", data["name"]),
        ("大小", f"{human_size(data['size'])}  ({data['size']} 字节)"),
        ("类型", data["kind"]),
        ("mime", data["mime"]),
        ("DC", data["dc_id"]),
    ]:
        if value not in (None, ""):
            table.add_row(key, str(value))
    table.add_row("引擎", engine)
    console.print(table)


# --------------------------------------------------------------------------- #
# download
# --------------------------------------------------------------------------- #

@app.command()
def download(
    target: str = typer.Argument(..., help="chat_id:message_id"),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o", help="输出文件路径，或已存在的目录"
    ),
    engine: str = typer.Option("auto", "--engine", help="auto | fast | tdlib"),
    connections: int = typer.Option(
        4, "--connections", "-c",
        help="每条 TCP 连接的并发请求数（总并发 = auth-keys × 该值）",
    ),
    chunk: str = typer.Option("512K", "--chunk", help="fast 引擎分片大小（最大 1M）"),
    fixed: bool = typer.Option(
        False, "--fixed",
        help="固定并发，不做自适应调节（撞限流只等待、不降并发）",
    ),
    auth_keys: int = typer.Option(
        1, "--auth-keys",
        help=(
            "并行使用的独立 auth key 总数（含主会话）。优先用 .tgdl/authkeys.json 里缓存的，"
            "不够的部分现场 DH 生成并写回缓存；无需额外登录"
        ),
    ),
    verbosity: int = typer.Option(1, "--verbosity"),
) -> None:
    """下载 chat_id:message_id 指向的消息里的文件。"""
    cfg = _load()
    try:
        chat_id, message_id = parse_target(target)
    except ValueError as exc:
        _fail(str(exc), 2)
        return

    engine = engine.lower()
    chunk_bytes = parse_size(chunk)
    if engine == "auto":
        engine = "fast" if _run(fast.whoami(cfg)) else "tdlib"

    if engine == "fast":
        total_inflight = auth_keys * connections
        if total_inflight > 32:
            console.print(
                f"[yellow]提示：{auth_keys} 条连接 × 每条 {connections} 个在途 = "
                f"{total_inflight} 个并发请求，容易触发 FLOOD_WAIT（建议 ≤ 32）[/yellow]"
            )
        if auth_keys > 8:
            console.print(
                f"[yellow]提示：--auth-keys {auth_keys} 收益递减——新建的授权额度更紧，"
                "实测 3~5 条最稳，超过 8 条反而 flood 更多、更慢[/yellow]"
            )
        try:
            with ProgressBar("fast") as bar:
                dest, elapsed, _name, size = _run(
                    fast.download_message(
                        cfg, chat_id, message_id, output,
                        connections=connections, chunk=chunk_bytes,
                        adaptive=not fixed, auth_keys=auth_keys,
                        on_conns=bar.update_conns, on_progress=bar.update,
                    )
                )
        except fast.FastError as exc:
            _fail(str(exc))
            return
        _report(dest, size, elapsed)
        return

    _download_with_tdlib(cfg, chat_id, message_id, output, verbosity)


def _download_with_tdlib(
    cfg: Config,
    chat_id: int,
    message_id: int,
    output: Path | None,
    verbosity: int,
) -> None:
    try:
        with Tdlib(cfg, verbosity=verbosity) as td:
            if not td.wait_ready():
                _fail("tdlib 未登录，先执行 `tgdl login --engine tdlib`")
                return
            message = td.get_message(chat_id, message_id)
            file, name, _kind = file_from_message(message)
            size = int(file.get("size") or file.get("expected_size") or 0)
            dest = output
            if dest is None or dest.is_dir():
                dest = (dest or Path.cwd()) / name
            started = time.monotonic()
            with ProgressBar("tdlib") as bar:
                td.download(file, dest, on_progress=bar.update)
            _report(Path(dest), size or Path(dest).stat().st_size, time.monotonic() - started)
    except (TdlibLoadError, TdlibError, TimeoutError) as exc:
        _fail(str(exc))


def _report(dest: Path, size: int, elapsed: float) -> None:
    speed = size / elapsed if elapsed > 0 else 0
    console.print(
        f"[green]完成[/green] {dest}  {human_size(size)}  "
        f"耗时 {elapsed:.2f}s  平均 {human_size(speed)}/s"
    )


# --------------------------------------------------------------------------- #
# bench
# --------------------------------------------------------------------------- #

@app.command()
def bench(
    target: str = typer.Argument(..., help="chat_id:message_id"),
    connections: str = typer.Option("2,4,8", "--connections", "-c", help="每条连接的并发数列表"),
    chunk: str = typer.Option("512K", "--chunk"),
    keep: bool = typer.Option(False, "--keep", help="保留下载的临时文件"),
    mb: int = typer.Option(0, "--mb", help="只测前 N MB（0 = 整个文件），大文件快速对比用"),
    verbosity: int = typer.Option(1, "--verbosity"),
) -> None:
    """同一个文件，对比 TDLib 自带下载器与 fast 引擎。"""
    cfg = _load()
    try:
        chat_id, message_id = parse_target(target)
    except ValueError as exc:
        _fail(str(exc), 2)
        return

    conns = [int(x) for x in connections.split(",") if x.strip()]
    chunk_bytes = parse_size(chunk)
    limit_bytes = mb * 1024 * 1024 if mb else 0
    workdir = Path(tempfile.mkdtemp(prefix="tgdl-bench-"))
    rows: list[tuple[str, int, float]] = []
    size = 0

    try:
        console.print("[bold]1) TDLib 自带下载器（基线）[/bold]")
        with Tdlib(cfg, verbosity=verbosity) as td:
            if not td.wait_ready():
                _fail("tdlib 未登录，先执行 `tgdl login --engine tdlib`")
                return
            message = td.get_message(chat_id, message_id)
            file, name, _kind = file_from_message(message)
            size = int(file.get("size") or file.get("expected_size") or 0)
            td.delete_file(int(file["id"]))  # 强制重新下载，否则会直接命中缓存
            measured = min(size, limit_bytes) if limit_bytes else size
            started = time.monotonic()
            with ProgressBar("tdlib") as bar:
                td.download(file, workdir / name, on_progress=bar.update, limit=limit_bytes)
            elapsed = max(time.monotonic() - started, 1e-6)
            rows.append(("tdlib (自带)", measured, elapsed))
            console.print(f"  {human_size(measured)} / {elapsed:.2f}s = {human_size(measured / elapsed)}/s")

        for conn in conns:
            console.print(f"[bold]2) fast 引擎，{conn} 并发[/bold]")
            started = time.monotonic()
            with ProgressBar(f"fast x{conn}") as bar:
                _run(
                    fast.download_message(
                        cfg, chat_id, message_id, workdir / f"fast{conn}.bin",
                        connections=conn, chunk=chunk_bytes,
                        limit=limit_bytes, on_progress=bar.update,
                    )
                )
            elapsed = max(time.monotonic() - started, 1e-6)
            measured = min(size, limit_bytes) if limit_bytes else size
            rows.append((f"fast x{conn}", measured, elapsed))
            console.print(f"  {human_size(measured)} / {elapsed:.2f}s = {human_size(measured / elapsed)}/s")
    finally:
        if not keep:
            for path in workdir.glob("*"):
                path.unlink(missing_ok=True)
            workdir.rmdir()
        else:
            console.print(f"[dim]临时文件保留在 {workdir}[/dim]")

    if not rows:
        return
    baseline = rows[0][2]
    table = Table(box=box.SIMPLE_HEAVY, header_style="bold")
    table.add_column("引擎")
    table.add_column("大小", justify="right")
    table.add_column("耗时", justify="right")
    table.add_column("速度", justify="right")
    table.add_column("相对 TDLib", justify="right")
    for name, total, elapsed in rows:
        table.add_row(
            name,
            human_size(total),
            f"{elapsed:.2f}s",
            f"{human_size(total / elapsed)}/s",
            "1.00x" if elapsed == baseline else f"[green]{baseline / elapsed:.2f}x[/green]",
        )
    console.print(table)


# --------------------------------------------------------------------------- #
# doctor
# --------------------------------------------------------------------------- #

@app.command()
def shell(
    auth_keys: int = typer.Option(
        3, "--auth-keys", help="连接池里的独立 auth key 总数（含主会话）"
    ),
    connections: int = typer.Option(
        4, "--connections", "-c", help="每条连接的并发请求数（总数 = auth-keys × 该值）"
    ),
    chunk: str = typer.Option("512K", "--chunk", help="分片大小（最大 1M）"),
    fixed: bool = typer.Option(
        False, "--fixed", help="固定并发，不做自适应（测速建议开，数据更干净）"
    ),
    dc: Optional[int] = typer.Option(
        None, "--dc", help="预先为某个 DC 建池；不填则第一次 dl 时按文件所在 DC 建"
    ),
    output_dir: Optional[Path] = typer.Option(
        None, "--dir", "-o", help="下载目录（默认 .tgdl/downloads）"
    ),
) -> None:
    """交互模式：先建好连接池，再反复下载 / 中断 / 删除（测「服务态」速度用）。"""
    cfg = _load()
    from .shell import PoolOptions, run as run_shell

    if auth_keys > 8:
        console.print(
            f"[yellow]提示：--auth-keys {auth_keys} 收益递减，实测 3~5 条最稳[/yellow]"
        )
    out_dir = output_dir or (cfg.data_dir / "downloads")
    out_dir.mkdir(parents=True, exist_ok=True)
    opts = PoolOptions(
        auth_keys=auth_keys,
        connections=connections,
        chunk=parse_size(chunk),
        adaptive=not fixed,
        dc_id=dc,
    )
    try:
        _run(run_shell(cfg, opts, out_dir))
    except KeyboardInterrupt:
        console.print("[dim]已退出[/dim]")


@app.command()
def doctor(verbosity: int = typer.Option(1, "--verbosity")) -> None:
    """自检：配置、tdjson、会话状态。"""
    table = Table(box=box.SIMPLE, show_header=False)
    table.add_column("检查项", style="cyan", no_wrap=True)
    table.add_column("结果")

    try:
        cfg = load_config(_OPTIONS["config"])
    except ConfigError as exc:
        _fail(str(exc))
        return

    table.add_row("配置文件", str(cfg.config_path or "(未使用，来自环境变量)"))
    table.add_row("数据目录", str(cfg.data_dir))
    table.add_row("api_id", str(cfg.api_id or "[red]缺失[/red]"))
    table.add_row("tdjson", str(cfg.tdjson) if cfg.tdjson.is_file() else f"[red]不存在: {cfg.tdjson}[/red]")

    try:
        import telethon

        table.add_row("telethon", telethon.__version__)
    except Exception:  # noqa: BLE001
        table.add_row("telethon", "[red]未安装（uv sync）[/red]")

    table.add_row(
        "cryptg",
        "[green]已启用（C 实现 AES）[/green]" if fast.check_crypto(force=True)
        else "[red]缺失，速度会被限制在几百 KB/s —— 执行 uv add cryptg[/red]",
    )

    try:
        store = fast.AuthKeyStore(cfg.authkeys_file)
        per_dc = {dc: len(v) for dc, v in store._keys.items() if v}
    except Exception:  # noqa: BLE001
        store, per_dc = None, {}
    if per_dc:
        detail = "，".join(f"DC{dc} {n} 条" for dc, n in sorted(per_dc.items(), key=lambda kv: int(kv[0])))
        home = f"｜主 DC{store.home_dc}" if store and store.home_dc else ""
        table.add_row("auth key", f"{sum(per_dc.values())} 条（{detail}{home}）")
    else:
        table.add_row(
            "auth key 缓存",
            "[dim]无（首次用 --auth-keys 会自动生成并落盘）[/dim]",
        )

    try:
        with Tdlib(cfg, verbosity=verbosity) as td:
            if td.wait_ready(60):
                table.add_row("tdlib 会话", f"[green]已登录[/green] {_user_label(td.call({'@type': 'getMe'}))}")
            else:
                table.add_row("tdlib 会话", "[yellow]未登录[/yellow]")
    except (TdlibLoadError, TdlibError) as exc:
        table.add_row("tdlib 会话", f"[red]{exc}[/red]")

    try:
        table.add_row("fast 会话", _run(fast.whoami(cfg)) or "[yellow]未登录[/yellow]")
    except fast.FastError as exc:
        table.add_row("fast 会话", f"[red]{exc}[/red]")

    table.add_row(
        "记录的 DC",
        f"会话 DC{cfg.session_dc}｜下载池 DC{cfg.pool_dc}"
        if cfg.session_dc or cfg.pool_dc
        else "[dim]暂无（跑一次 download 或 shell 就会写入）[/dim]",
    )

    console.print(table)


@app.command()
def serve(
    port: int = typer.Option(8765, "--port", "-p", help="监听端口"),
    token: Optional[str] = typer.Option(
        None, "--token", help="访问令牌；不填会自动生成并写入 .tgdl/server.token"
    ),
) -> None:
    """启动本地服务，给油猴脚本用（只监听 127.0.0.1）。"""
    cfg = _load()
    from .server import serve as run_server

    try:
        run_server(cfg, port=port, token=token)
    except OSError as exc:
        _fail(f"启动失败：{exc}")


@app.command()
def version() -> None:
    """显示版本。"""
    console.print(f"tgdl {__version__}")


if __name__ == "__main__":  # pragma: no cover
    app()
