"""A/B：首发错峰（模仿 TDLib 的 DelayDispatcher）到底有没有用。

    uv run python scripts/compare_stagger.py --target 8913476869:6799 --mb 32

A = 直接开火（现在的行为）
B = 开局请求按 50ms × 0.8 衰减到 3ms 摊开（--stagger）

两组都固定并发（adaptive=False），只差错峰这一个变量。
每 0.5s 打印一次瞬时总速度，方便直接看曲线形状。
"""

from __future__ import annotations

import argparse
import asyncio
import builtins
import re
import statistics
import time
from pathlib import Path

from tgdl import fast
from tgdl.config import load_config
from tgdl.util import parse_target, parse_size

REAL_PRINT = builtins.print
DEST = Path("bench_stagger.bin")


async def run_arm(cfg, chat_id, message_id, *, label: str, stagger: bool,
                  mb: int, chunk: int, per_conn: int, keys: int) -> dict:
    floods: list[int] = []
    flood_times: list[float] = []
    state = {"done": 0, "t0": 0.0, "peak": 0.0, "samples": []}

    def on_conns(speeds: list[float]) -> None:
        total = sum(speeds)
        dt = time.monotonic() - state["t0"]
        state["peak"] = max(state["peak"], total)
        state["samples"].append(total)
        active = sum(1 for s in speeds if s > 0.05 * 1048576)
        REAL_PRINT(
            f"  [{label}] t={dt:5.1f}s  {total / 1048576:6.2f} MiB/s  "
            f"活跃连接 {active}/{len(speeds)}  "
            f"已下 {state['done'] / 1048576:6.1f} MiB"
        )

    def on_progress(done: int, total: int) -> None:
        state["done"] = done

    def on_ready() -> None:
        state["t0"] = time.monotonic()   # 只算纯传输，不含建连

    def spy(*args, **kwargs):
        text = " ".join(str(a) for a in args)
        match = re.search(r"FLOOD_WAIT (\d+)s", text)
        if match:
            floods.append(int(match.group(1)))
            dt = time.monotonic() - state["t0"] if state["t0"] else 0.0
            flood_times.append(dt)
            REAL_PRINT(f"  [{label}] t={dt:5.1f}s  {text}")

    builtins.print = spy
    try:
        await fast.download_message(
            cfg, chat_id, message_id, DEST,
            connections=per_conn, chunk=chunk, auth_keys=keys,
            limit=mb * 1024 * 1024, adaptive=False,
            on_conns=on_conns, on_progress=on_progress, on_ready=on_ready,
            stagger=stagger,
        )
    finally:
        builtins.print = REAL_PRINT
        DEST.unlink(missing_ok=True)

    # 去掉建连后的前两个采样点，避开首批请求的 RTT 空窗
    tail = state["samples"][2:] or state["samples"]
    elapsed = time.monotonic() - state["t0"]
    return {
        "elapsed": elapsed,
        "speed": mb * 1048576 / elapsed if elapsed else 0,
        "steady": statistics.mean(tail) if tail else 0,
        "peak": state["peak"],
        "floods": len(floods),
        "waited": sum(floods),
        "first_flood": flood_times[0] if flood_times else None,
        "early_floods": sum(1 for t in flood_times if t <= 5.0),
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, help="chat_id:message_id")
    parser.add_argument("--mb", type=int, default=32)
    parser.add_argument("--chunk", default="1M")
    parser.add_argument("--per-conn", type=int, default=16)
    parser.add_argument("--keys", type=int, default=5)
    parser.add_argument("--rounds", type=int, default=2)
    args = parser.parse_args()

    cfg = load_config()
    chat_id, message_id = parse_target(args.target)
    chunk = parse_size(args.chunk)

    runs: dict[str, list[dict]] = {"A 直接开火": [], "B 首发错峰": []}
    order = [("A 直接开火", False), ("B 首发错峰", True)]
    for round_i in range(args.rounds):
        # 隔轮反转顺序，抵消「先跑的把限流水位抬高」这种顺序污染
        for label, flag in (order if round_i % 2 == 0 else order[::-1]):
            REAL_PRINT(f"\n=== 第 {round_i + 1} 轮 · {label} ===")
            try:
                data = await run_arm(
                    cfg, chat_id, message_id, label=label, stagger=flag,
                    mb=args.mb, chunk=chunk, per_conn=args.per_conn, keys=args.keys,
                )
            except Exception as exc:  # noqa: BLE001
                REAL_PRINT(f"  [{label}] 失败：{type(exc).__name__}: {exc}")
                continue
            runs[label].append(data)
            first = "—" if data["first_flood"] is None else f"{data['first_flood']:.1f}s"
            REAL_PRINT(
                f"  [{label}] 用时 {data['elapsed']:.2f}s｜平均 "
                f"{data['speed'] / 1048576:.2f} MiB/s｜稳态 "
                f"{data['steady'] / 1048576:.2f} MiB/s｜峰值 "
                f"{data['peak'] / 1048576:.2f} MiB/s｜FLOOD {data['floods']} 次"
                f"（{data['waited']}s，首个在 {first}，前 5s {data['early_floods']} 次）"
            )
            await asyncio.sleep(4)   # 让上一轮的限流冷却

    REAL_PRINT("\n=== 汇总 ===")
    for label, rows in runs.items():
        if not rows:
            REAL_PRINT(f"  {label}: 无数据")
            continue
        avg = statistics.mean(r["speed"] for r in rows) / 1048576
        steady = statistics.mean(r["steady"] for r in rows) / 1048576
        flood = sum(r["floods"] for r in rows)
        early = sum(r["early_floods"] for r in rows)
        firsts = [r["first_flood"] for r in rows if r["first_flood"] is not None]
        first = f"{min(firsts):.1f}s" if firsts else "无"
        REAL_PRINT(
            f"  {label:12} 平均 {avg:5.2f} MiB/s｜稳态 {steady:5.2f} MiB/s｜"
            f"FLOOD 合计 {flood} 次（前 5s {early} 次，首个 {first}）"
        )


if __name__ == "__main__":
    asyncio.run(main())
