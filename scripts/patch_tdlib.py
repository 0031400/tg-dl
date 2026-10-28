"""给 TDLib 源码打"提速补丁"：放宽下载并发上限，然后需要你自己重编 tdjson。

用法：
    uv run python scripts/patch_tdlib.py --td-src D:/file/cpp/td-1.8.0
    uv run python scripts/patch_tdlib.py --td-src D:/file/cpp/td-1.8.0 --revert

改动只有两处（详见 README「想让 TDLib 自己也变快？」）：
    1. td/telegram/files/ResourceManager.h  MAX_RESOURCE_LIMIT 2MiB -> 64MiB
    2. td/telegram/net/ConnectionCreator.cpp MAX_CONNECTIONS 10 -> 64
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

PATCHES = [
    (
        Path("td/telegram/files/ResourceManager.h"),
        "static constexpr int64 MAX_RESOURCE_LIMIT = 1 << 21;",
        "static constexpr int64 MAX_RESOURCE_LIMIT = 1 << 26;",
    ),
    (
        Path("td/telegram/net/ConnectionCreator.cpp"),
        "const size_t MAX_CONNECTIONS = 10;",
        "const size_t MAX_CONNECTIONS = 64;",
    ),
]


def apply(td_src: Path, revert: bool) -> int:
    changed = 0
    for rel, old, new in PATCHES:
        path = td_src / rel
        if not path.is_file():
            print(f"[跳过] 找不到 {path}")
            continue
        text = path.read_text(encoding="utf-8")
        src, dst = (new, old) if revert else (old, new)
        if dst in text and src not in text:
            print(f"[已是目标状态] {rel}")
            continue
        if src not in text:
            print(f"[跳过] {rel} 里没有预期内容，可能版本不同")
            continue
        backup = path.with_suffix(path.suffix + ".bak")
        if not backup.exists() and not revert:
            shutil.copy2(path, backup)
        path.write_text(text.replace(src, dst, 1), encoding="utf-8")
        print(f"[改好] {rel}")
        changed += 1
    return changed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--td-src", default=r"D:\file\cpp\td-1.8.0", help="TDLib 源码根目录")
    parser.add_argument("--revert", action="store_true", help="撤销改动")
    args = parser.parse_args()

    td_src = Path(args.td_src)
    if not (td_src / "CMakeLists.txt").is_file():
        print(f"不是 TDLib 源码目录: {td_src}", file=sys.stderr)
        return 1

    changed = apply(td_src, args.revert)
    print(f"共修改 {changed} 处。")
    if changed and not args.revert:
        print(
            "\n接下来重编（示例，MSVC + OpenSSL/zlib 已就绪的情况下）：\n"
            f'  cmake -S "{td_src}" -B "{td_src}/build" -A x64 -DDTDISABLE_TESTS=ON\n'
            f'  cmake --build "{td_src}/build" --config Release --target tdjson --parallel\n'
            "\n然后把新生成的 tdjson.dll 覆盖到 config.toml 里的 tdjson 路径，"
            "再用 `tgdl bench` 对比。"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
