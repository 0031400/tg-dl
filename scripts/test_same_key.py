"""同一个 auth key 能不能同时开多条 TCP —— 实测。

    uv run python scripts/test_same_key.py -1004389561712:114 --conns 4 --mb 48 --same
    uv run python scripts/test_same_key.py -1004389561712:114 --conns 4 --mb 48 --distinct

两条路走的是同一个 `download_location()`，唯一的差别是那 N 条连接的 auth key
是「同一把」还是「N 把不同的」。
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import time
from pathlib import Path

from tgdl import fast
from tgdl.config import load_config
from tgdl.util import parse_size, parse_target

DEST = Path("bench_same_key.bin")


async def run(cfg, chat_id, message_id, *, conns: int, inflight: int,
              chunk: int, limit: int, same: bool) -> dict:
    client = fast.make_client(cfg)
    await client.connect()
    floods: list[int] = []
    try:
        _, _, _, types = fast._telethon()
        message = await fast._fetch_message(client, chat_id, message_id)
        location, size, name, dc_id = fast._location_from_message(message, types)
        size = min(int(size), limit)
        store = fast.AuthKeyStore(cfg.authkeys_file)
        keys = store.keys(dc_id, 1)
        if not keys:
            raise SystemExit(f"会话 1 没有 DC{dc_id} 的缓存 key，先跑一次普通下载")

        print(f"目标 {name}  取前 {size / 1048576:.0f} MB  DC{dc_id}")
        if same:
            picked = [keys[0]] * conns
            print(f"用**同一把** key 开 {conns} 条连接：{keys[0][:8]}")
        else:
            need = conns - len(keys)
            if need > 0:
                print(f"缓存只有 {len(keys)} 把，现场再派生 {need} 把…")
                await fast._fresh_senders(client, dc_id, conns, store, set(), 1)
                keys = store.keys(dc_id, 1)
            picked = keys[:conns]
            print(f"用 {len(picked)} 把不同的 key 开 {conns} 条连接："
                  + ", ".join(k[:8] for k in picked))

        pairs = []
        for hexed in picked:
            sender = await fast._open_with_key(client, dc_id, bytes.fromhex(hexed))
            pairs.append((client, sender))

        t0 = time.monotonic()
        try:
            await fast.download_location(
                client, location, size, DEST, dc_id=dc_id,
                connections=inflight, chunk=chunk, adaptive=False,
                senders=pairs,
            )
        finally:
            for _, sender in pairs:
                await fast._safe_disconnect(sender)
        elapsed = time.monotonic() - t0
        return {"elapsed": elapsed, "size": size, "dc": dc_id}
    finally:
        await client.disconnect()
        DEST.unlink(missing_ok=True)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("target")
    ap.add_argument("--conns", type=int, default=4)
    ap.add_argument("--inflight", type=int, default=8)
    ap.add_argument("--chunk", default="1M")
    ap.add_argument("--mb", type=int, default=48)
    ap.add_argument("--same", action="store_true", help="N 条连接共用一把 key")
    ap.add_argument("--distinct", action="store_true", help="N 条连接用 N 把不同 key")
    args = ap.parse_args()
    if not (args.same or args.distinct):
        raise SystemExit("要么 --same，要么 --distinct")

    cfg = load_config()
    chat_id, message_id = parse_target(args.target)
    data = await run(
        cfg, chat_id, message_id, conns=args.conns, inflight=args.inflight,
        chunk=parse_size(args.chunk), limit=args.mb * 1024 * 1024, same=args.same,
    )
    speed = data["size"] / data["elapsed"] / 1048576
    tag = "同一把 key" if args.same else "N 把 key"
    print(f"\n{tag}：{data['size'] / 1048576:.0f} MB / {data['elapsed']:.2f}s "
          f"= {speed:.2f} MiB/s")


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
