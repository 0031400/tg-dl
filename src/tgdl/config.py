"""配置加载：config.toml + 环境变量。"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_TDJSON = Path(r"C:\Users\Administrator\file\tdlib\bin\tdjson.dll")


class ConfigError(RuntimeError):
    """配置缺失或非法。"""


@dataclass(slots=True)
class Config:
    api_id: int | None = None
    api_hash: str | None = None
    tdjson: Path = field(default=DEFAULT_TDJSON)
    data_dir: Path = field(default=Path(".tgdl"))
    config_path: Path | None = None
    # 会话所在 DC（登录后记录），以及上次下载用的 DC（连接池预热用）
    session_dc: int | None = None
    pool_dc: int | None = None

    # ---- 派生路径 ----
    @property
    def tdlib_dir(self) -> Path:
        return self.data_dir / "tdlib"

    @property
    def tdlib_db(self) -> Path:
        return self.tdlib_dir / "db"

    @property
    def tdlib_files(self) -> Path:
        return self.tdlib_dir / "files"

    @property
    def fast_session(self) -> Path:
        return self.fast_session_at(1)

    def fast_session_at(self, index: int = 1) -> Path:
        """第 index 个 fast 会话文件（1 -> fast，2 -> fast2 …）。

        多个会话 = 多个 auth key，Telegram 的 flood 预算按 auth key 算，
        所以并发用多个会话能叠加吞吐。
        """
        if index <= 1:
            return self.data_dir / "fast"
        return self.data_dir / f"fast{index}"

    @property
    def authkeys_file(self) -> Path:
        """缓存为其他 DC 生成的 auth key，省掉每次 ~3 秒的 DH 握手。"""
        return self.data_dir / "authkeys.json"

    def require_credentials(self) -> tuple[int, str]:
        if not self.api_id or not self.api_hash:
            raise ConfigError(
                "缺少 api_id / api_hash。先运行 `tgdl init --api-id 123456 --api-hash xxxx`，"
                "或设置环境变量 TGDL_API_ID / TGDL_API_HASH。"
            )
        return self.api_id, self.api_hash

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.tdlib_db.mkdir(parents=True, exist_ok=True)
        self.tdlib_files.mkdir(parents=True, exist_ok=True)


def _search_paths(explicit: Path | None) -> list[Path]:
    if explicit is not None:
        return [explicit]
    candidates: list[Path] = []
    env = os.environ.get("TGDL_CONFIG")
    if env:
        candidates.append(Path(env))
    candidates.append(Path.cwd() / ".tgdl" / "config.toml")
    candidates.append(Path.home() / ".tgdl" / "config.toml")
    return candidates


def load_config(explicit: Path | None = None) -> Config:
    """按 显式路径 -> $TGDL_CONFIG -> ./.tgdl -> ~/.tgdl 的顺序查找配置。"""
    for path in _search_paths(explicit):
        if path.is_file():
            return _from_toml(path)
    if explicit is not None:
        raise ConfigError(f"配置文件不存在: {explicit}")
    return _from_env(Config())


def _from_env(cfg: Config) -> Config:
    api_id = os.environ.get("TGDL_API_ID")
    api_hash = os.environ.get("TGDL_API_HASH")
    if api_id:
        cfg.api_id = int(api_id)
    if api_hash:
        cfg.api_hash = api_hash
    if tdjson := os.environ.get("TGDL_TDJSON"):
        cfg.tdjson = Path(tdjson)
    if data_dir := os.environ.get("TGDL_DATA_DIR"):
        cfg.data_dir = Path(data_dir)
    return cfg


def _from_toml(path: Path) -> Config:
    with path.open("rb") as fp:
        raw = tomllib.load(fp)

    base = path.parent
    cfg = Config(config_path=path)
    if raw.get("api_id"):
        cfg.api_id = int(raw["api_id"])
    if raw.get("api_hash"):
        cfg.api_hash = str(raw["api_hash"])
    if raw.get("tdjson"):
        p = Path(str(raw["tdjson"]))
        cfg.tdjson = p if p.is_absolute() else (base / p).resolve()
    if raw.get("session_dc"):
        cfg.session_dc = int(raw["session_dc"])
    if raw.get("pool_dc"):
        cfg.pool_dc = int(raw["pool_dc"])
    if raw.get("data_dir"):
        p = Path(str(raw["data_dir"]))
        cfg.data_dir = p if p.is_absolute() else (base / p).resolve()
    else:
        cfg.data_dir = base.resolve()
    return _from_env(cfg)


def write_config(path: Path, *, api_id: int | None, api_hash: str | None,
                 tdjson: Path | None, data_dir: Path | None,
                 session_dc: int | None = None, pool_dc: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# tgdl 配置", ""]
    if api_id:
        lines.append(f"api_id = {int(api_id)}")
    if api_hash:
        lines.append(f'api_hash = "{api_hash}"')
    if tdjson:
        lines.append(f'tdjson = {str(tdjson).replace(chr(92), "/")!r}')
    if data_dir:
        lines.append(f'data_dir = {str(data_dir).replace(chr(92), "/")!r}')
    if session_dc:
        lines.append(f"session_dc = {int(session_dc)}")
    if pool_dc:
        lines.append(f"pool_dc = {int(pool_dc)}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def remember_dc(cfg: Config, *, session_dc: int | None = None,
                pool_dc: int | None = None) -> None:
    """把 DC 写回 config.toml（值没变就不写，避免每次运行都碰磁盘）。

    只改这两行，文件里的注释和其它键都原样保留。
    """
    path = cfg.config_path
    if path is None:
        return
    updates: dict[str, int] = {}
    if session_dc is not None and session_dc != cfg.session_dc:
        cfg.session_dc = int(session_dc)
        updates["session_dc"] = int(session_dc)
    if pool_dc is not None and pool_dc != cfg.pool_dc:
        cfg.pool_dc = int(pool_dc)
        updates["pool_dc"] = int(pool_dc)
    if not updates:
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        lines = ["# tgdl 配置", ""]
    for key, value in updates.items():
        for i, line in enumerate(lines):
            if line.strip().startswith(f"{key} "):
                lines[i] = f"{key} = {value}"
                break
        else:
            lines.append(f"{key} = {value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _search_paths(explicit: Path | None) -> list[Path]:
    if explicit is not None:
        return [explicit]
    candidates: list[Path] = []
    env = os.environ.get("TGDL_CONFIG")
    if env:
        candidates.append(Path(env))
    candidates.append(Path.cwd() / ".tgdl" / "config.toml")
    candidates.append(Path.home() / ".tgdl" / "config.toml")
    return candidates


def load_config(explicit: Path | None = None) -> Config:
    """按 显式路径 -> $TGDL_CONFIG -> ./.tgdl -> ~/.tgdl 的顺序查找配置。"""
    for path in _search_paths(explicit):
        if path.is_file():
            return _from_toml(path)
    if explicit is not None:
        raise ConfigError(f"配置文件不存在: {explicit}")
    return _from_env(Config())


def _from_env(cfg: Config) -> Config:
    api_id = os.environ.get("TGDL_API_ID")
    api_hash = os.environ.get("TGDL_API_HASH")
    if api_id:
        cfg.api_id = int(api_id)
    if api_hash:
        cfg.api_hash = api_hash
    if tdjson := os.environ.get("TGDL_TDJSON"):
        cfg.tdjson = Path(tdjson)
    if data_dir := os.environ.get("TGDL_DATA_DIR"):
        cfg.data_dir = Path(data_dir)
    return cfg


def _from_toml(path: Path) -> Config:
    with path.open("rb") as fp:
        raw = tomllib.load(fp)

    base = path.parent
    cfg = Config(config_path=path)
    if raw.get("api_id"):
        cfg.api_id = int(raw["api_id"])
    if raw.get("api_hash"):
        cfg.api_hash = str(raw["api_hash"])
    if raw.get("tdjson"):
        p = Path(str(raw["tdjson"]))
        cfg.tdjson = p if p.is_absolute() else (base / p).resolve()
    if raw.get("session_dc"):
        cfg.session_dc = int(raw["session_dc"])
    if raw.get("pool_dc"):
        cfg.pool_dc = int(raw["pool_dc"])
    if raw.get("data_dir"):
        p = Path(str(raw["data_dir"]))
        cfg.data_dir = p if p.is_absolute() else (base / p).resolve()
    else:
        cfg.data_dir = base.resolve()
    return _from_env(cfg)


def write_config(path: Path, *, api_id: int | None, api_hash: str | None,
                 tdjson: Path | None, data_dir: Path | None,
                 session_dc: int | None = None, pool_dc: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# tgdl 配置", ""]
    if api_id:
        lines.append(f"api_id = {int(api_id)}")
    if api_hash:
        lines.append(f'api_hash = "{api_hash}"')
    if tdjson:
        lines.append(f'tdjson = {str(tdjson).replace(chr(92), "/")!r}')
    if data_dir:
        lines.append(f'data_dir = {str(data_dir).replace(chr(92), "/")!r}')
    if session_dc:
        lines.append(f"session_dc = {int(session_dc)}")
    if pool_dc:
        lines.append(f"pool_dc = {int(pool_dc)}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def remember_dc(cfg: Config, *, session_dc: int | None = None,
                pool_dc: int | None = None) -> None:
    """把 DC 写回 config.toml（值没变就不写，避免每次运行都碰磁盘）。"""
    path = cfg.config_path
    if path is None:
        return
    changed = False
    if session_dc is not None and session_dc != cfg.session_dc:
        cfg.session_dc = int(session_dc)
        changed = True
    if pool_dc is not None and pool_dc != cfg.pool_dc:
        cfg.pool_dc = int(pool_dc)
        changed = True
    if not changed:
        return
    raw: dict = {}
    if path.is_file():
        try:
            with path.open("rb") as fp:
                raw = tomllib.load(fp)
        except Exception:  # noqa: BLE001
            raw = {}
    raw["session_dc"] = cfg.session_dc
    raw["pool_dc"] = cfg.pool_dc
    write_config(
        path,
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        tdjson=cfg.tdjson,
        data_dir=cfg.data_dir,
        session_dc=cfg.session_dc,
        pool_dc=cfg.pool_dc,
    )
