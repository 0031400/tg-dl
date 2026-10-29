"""多账号同时下载同一条消息 —— 冒烟 + 测速。

    uv run python scripts/test_accounts.py 8913476869:6799 --mb 32 --sessions 2 --keys 1

每 0.5s 打一行总速度，结束打每条连接 / 每个账号的用量。
"""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path

from tgdl import fast
from tgdl.config import load_config
from tgdl.util import parse_size, parse_target

DEST = Path("bench_accounts.bin")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("target", help="chat_id:message_id")
    ap.add_argument("--mb", type=int, default=32)
    ap.add_argument("--sessions", type=int, default=2, help="用几个登录账号")
    ap.add_argument("--keys", type=int, default=1, help="每个账号几条连接")
    ap.add_argument("--per-conn", type=int, default=8, help="每条连接几个在途")
    ap.add_argument("--chunk", default="1M")
    ap.add_argument("--stagger", action="store_true")
    ap.add_argument("--pick", type=int, default=0, help="相册里选第几个（1 起），0=最大")
    args = ap.parse_args()

    cfg = load_config()
    chat_id, message_id = parse_target(args.target)
    state = {"t0": 0.0, "done": 0, "peak": 0.0}

    def on_conns(speeds: list[float]) -> None:
        total = sum(speeds)
        dt = time.monotonic() - state["t0"] if state["t0"] else 0.0
        state["peak"] = max(state["peak"], total)
        active = sum(1 for s in speeds if s > 0.05 * 1048576)
        print(
            f"  t={dt:5.1f}s  {total / 1048576:6.2f} MiB/s  "
            f"活跃 {active}/{len(speeds)}  已下 {state['done'] / 1048576:6.1f} MiB",
            flush=True,
        )

    def on_ready() -> None:
        state["t0"] = time.monotonic()

    started = time.monotonic()
    try:
        await fast.download_message(
            cfg, chat_id, message_id, DEST,
            sessions=args.sessions, auth_keys=args.keys,
            connections=args.per_conn, chunk=parse_size(args.chunk),
            limit=args.mb * 1024 * 1024, adaptive=False,
            on_conns=on_conns, on_ready=on_ready,
            on_progress=lambda d, t: state.__setitem__("done", d),
            stagger=args.stagger, pick=args.pick,
        )
    finally:
        DEST.unlink(missing_ok=True)

    wall = time.monotonic() - started
    transfer = time.monotonic() - state["t0"] if state["t0"] else wall
    print(
        f"\n{args.sessions} 账号 × {args.keys} 连接 × {args.per_conn} 在途  "
        f"| 传输 {transfer:.2f}s  {args.mb / transfer:.2f} MiB/s  "
        f"| 峰值 {state['peak'] / 1048576:.2f} MiB/s"
    )


if __name__ == "__main__":
    asyncio.run(main())
