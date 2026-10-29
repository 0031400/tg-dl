"""对比两种“多连接”来源的限流差异。

    A) 一次登录，导出 N 条 DC key          → --auth-keys N
    B) N 次登录，每次 1 条 key             → --sessions N（需先 login --session 2..N）

两者最终都是「同一个 DC 上的 N 条 auth key」，这个脚本用来验证它们的限流额度是否相同。

用法：
    uv run python scripts/compare_sessions_vs_keys.py --target 8913476869:6799 --mb 32

只测能跑的：`fast2` 没登录时 B 组会自动跳过并提示。
"""

from __future__ import annotations

import argparse
import asyncio
import builtins
import re
import time
from pathlib import Path

from tgdl import fast
from tgdl.config import load_config
from tgdl.util import human_size, parse_target, parse_size


async def run_arm(cfg, chat_id: int, message_id: int, *, sessions: int,
                  auth_keys: int, per_conn: int, chunk: int, limit: int) -> dict:
    """跑一次下载，顺便统计 FLOOD_WAIT 次数和累计等待秒数。"""
    floods: list[int] = []
    original = builtins.print

    def spy(*args, **kwargs):
        text = " ".join(str(a) for a in args)
        match = re.search(r"FLOOD_WAIT (\d+)s", text)
        if match:
            floods.append(int(match.group(1)))
        # 其余日志静音，只留结果表

    builtins.print = spy
    started = time.monotonic()
    try:
        _, _elapsed, _name, size = await fast.download_message(
            cfg, chat_id, message_id, Path("/tmp/tgdl-cmp.bin"),
            connections=per_conn, chunk=chunk, auth_keys=auth_keys,
            sessions=sessions, limit=limit, adaptive=False,
        )
    finally:
        builtins.print = original
        Path("/tmp/tgdl-cmp.bin").unlink(missing_ok=True)
    elapsed = time.monotonic() - started
    return {
        "size": size,
        "elapsed": elapsed,
        "speed": size / elapsed if elapsed > 0 else 0,
        "floods": len(floods),
        "waited": sum(floods),
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, help="chat_id:message_id")
    parser.add_argument("--mb", type=int, default=32, help="只测前 N MB")
    parser.add_argument("--chunk", default="1M")
    parser.add_argument("--per-conn", type=int, default=4)
    args = parser.parse_args()

    cfg = load_config()
    chat_id, message_id = parse_target(args.target)
    limit = args.mb * 1024 * 1024
    chunk = parse_size(args.chunk)

    arms = [
        ("A 一次登录 · 1 条 key", 1, 1),
        ("A 一次登录 · 2 条 key", 1, 2),
        ("A 一次登录 · 4 条 key", 1, 4),
        ("B 两次登录 · 各 1 条", 2, 1),
    ]

    rows = []
    for label, sessions, keys in arms:
        try:
            data = await run_arm(
                cfg, chat_id, message_id, sessions=sessions, auth_keys=keys,
                per_conn=args.per_conn, chunk=chunk, limit=limit,
            )
        except fast.FastError as exc:
            print(f"{label:24} 跳过：{exc}")
            continue
        except Exception as exc:  # noqa: BLE001
            print(f"{label:24} 失败：{type(exc).__name__}: {exc}")
            continue
        rows.append((label, sessions * keys, data))
        print(
            f"{label:24} 连接 {sessions * keys}｜{human_size(data['size'])} "
            f"{data['elapsed']:5.2f}s｜{human_size(data['speed'])}/s"
            f"｜FLOOD {data['floods']} 次（{data['waited']}s）"
        )
        await asyncio.sleep(3)   # 每轮之间歇一下，别让上一轮的限流影响下一轮

    if not rows:
        return
    print("\n对比（速度 / FLOOD）")
    base = rows[0][2]["speed"] or 1
    for label, conns, data in rows:
        print(
            f"  {label:24} {conns} 连接  {data['speed'] / 1048576:5.2f} MiB/s "
            f"({data['speed'] / base:4.2f}x)  flood={data['floods']}"
        )


if __name__ == "__main__":
    asyncio.run(main())
