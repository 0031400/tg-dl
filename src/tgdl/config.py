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
    if raw.get("data_dir"):
        p = Path(str(raw["data_dir"]))
        cfg.data_dir = p if p.is_absolute() else (base / p).resolve()
    else:
        cfg.data_dir = base.resolve()
    return _from_env(cfg)


def write_config(path: Path, *, api_id: int | None, api_hash: str | None,
                 tdjson: Path | None, data_dir: Path | None) -> None:
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
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
