"""对比：同一把 DC key 连「普通 IP」和「media_only IP」下载同一段数据。

用法：uv run python scripts/probe_media_ip.py chat_id:message_id [每段字节数]
"""
from __future__ import annotations

import asyncio
import copy
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from telethon import functions  # noqa: E402
from telethon.client.telegrambaseclient import LAYER  # noqa: E402
from telethon.crypto import AuthKey  # noqa: E402
from telethon.network import MTProtoSender  # noqa: E402
from telethon.tl.functions import InvokeWithLayerRequest  # noqa: E402

from tgdl import fast  # noqa: E402
from tgdl.config import load_config  # noqa: E402
from tgdl.util import parse_target, to_mtproto_message_id  # noqa: E402


async def open_on_ip(client, dc_id, ip, port, key):
    sender = MTProtoSender(AuthKey(key), loggers=client._log)
    await asyncio.wait_for(
        sender.connect(client._connection(ip, port, dc_id, loggers=client._log)), 20
    )
    init = copy.copy(client._init_request)
    init.query = functions.help.GetConfigRequest()
    await asyncio.wait_for(sender.send(InvokeWithLayerRequest(LAYER, init)), 20)
    return sender


async def bench(client, dc_id, ip, port, keys, location, *, conns, per_conn, chunk):
    """conns 条连接（每把 key 一条）各取 per_conn 字节，返回聚合 MiB/s。"""
    senders = [await open_on_ip(client, dc_id, ip, port, k) for k in keys[:conns]]

    async def worker(sender, base):
        off = base
        while off < base + per_conn:
            await fast._get_file(client, sender, location, off, chunk, cdn=False)
            off += chunk

    t0 = time.perf_counter()
    try:
        await asyncio.gather(*[
            worker(s, i * per_conn) for i, s in enumerate(senders)
        ])
    finally:
        for s in senders:
            try:
                await s.disconnect()
            except Exception:  # noqa: BLE001
                pass
    dt = time.perf_counter() - t0
    return conns * per_conn / dt / 1048576, dt


async def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "8913476869:6799"
    chunk = int(sys.argv[2]) if len(sys.argv) > 2 else 512 * 1024

    cfg = load_config()
    client = fast.make_client(cfg, 1)
    await client.connect()
    try:
        _, _, _, types = fast._telethon()
        chat_id, msg_id = parse_target(target)
        peer = await fast._resolve_entity(client, chat_id)
        msg = await client.get_messages(peer, ids=to_mtproto_message_id(msg_id))
        location, size, name, dc_id = fast._location_from_message(msg, types)
        print(f"文件 {name}  {size/1048576:.1f} MiB  所在 DC{dc_id}")

        store = fast.AuthKeyStore(cfg.authkeys_file)
        keys = store.keys(dc_id)
        if not keys:
            print(f"authkeys.json 里没有 DC{dc_id} 的 key")
            return
        key = bytes.fromhex(keys[0])

        conf = await client(functions.help.GetConfigRequest())
        opts = [d for d in conf.dc_options if d.id == dc_id and not d.ipv6]
        print(f"DC{dc_id} 的 IPv4 入口：")
        for d in opts:
            print(f"  {d.ip_address:<18} {'media_only' if getattr(d, 'media_only', False) else 'normal'}")

        print(f"\n同一把 key 各取 {chunk/1024:.0f} KiB：")
        for d in opts:
            tag = "media" if getattr(d, "media_only", False) else "normal"
            try:
                sender = await open_on_ip(client, dc_id, d.ip_address, d.port, key)
            except Exception as e:  # noqa: BLE001
                print(f"  [{tag}] {d.ip_address} 连不上: {type(e).__name__}: {e}")
                continue
            try:
                t0 = time.perf_counter()
                data = await fast._get_file(client, sender, location, 0, chunk, cdn=False)
                dt = time.perf_counter() - t0
                print(f"  [{tag}] {d.ip_address:<18} {len(data)/1024:>7.0f} KiB  "
                      f"{dt*1000:>6.0f} ms  {len(data)/dt/1048576:>6.2f} MiB/s")
            except Exception as e:  # noqa: BLE001
                print(f"  [{tag}] {d.ip_address:<18} 取数据失败: {type(e).__name__}: {e}")
            finally:
                try:
                    await sender.disconnect()
                except Exception:  # noqa: BLE001
                    pass

        if "--bench" in sys.argv:
            conns, per_conn, chunk = 4, 2 * 1024 * 1024, 512 * 1024
            print(f"\n== 4 连接 × 2 MiB 聚合吞吐（每把 key 一条连接） ==")
            for d in opts:
                tag = "media" if getattr(d, "media_only", False) else "normal"
                try:
                    mibs, dt = await bench(client, dc_id, d.ip_address, d.port,
                                           [bytes.fromhex(k) for k in keys],
                                           location, conns=conns,
                                           per_conn=per_conn, chunk=chunk)
                    print(f"  [{tag}] {d.ip_address:<18} {conns*per_conn/1048576:>4.0f} MiB "
                          f"in {dt:>5.1f}s  -> {mibs:>5.2f} MiB/s")
                except Exception as e:  # noqa: BLE001
                    print(f"  [{tag}] {d.ip_address:<18} 失败: {type(e).__name__}: {e}")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
