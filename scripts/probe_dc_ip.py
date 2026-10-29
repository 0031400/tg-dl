"""探测：同一个 DC 的 auth key 能否连到该 DC 的其它 IP。

用法：uv run python scripts/probe_dc_ip.py [dc_id]
"""
from __future__ import annotations

import asyncio
import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from telethon import functions  # noqa: E402
from telethon.client.telegrambaseclient import LAYER  # noqa: E402
from telethon.crypto import AuthKey  # noqa: E402
from telethon.network import MTProtoSender  # noqa: E402
from telethon.tl.functions import InvokeWithLayerRequest  # noqa: E402

from tgdl import fast  # noqa: E402
from tgdl.config import load_config  # noqa: E402


async def try_ip(client, dc_id: int, ip: str, port: int, key: bytes, tag: str):
    sender = MTProtoSender(AuthKey(key), loggers=client._log)
    try:
        await asyncio.wait_for(
            sender.connect(client._connection(ip, port, dc_id, loggers=client._log)), 20
        )
        init = copy.copy(client._init_request)
        init.query = functions.help.GetConfigRequest()
        await asyncio.wait_for(sender.send(InvokeWithLayerRequest(LAYER, init)), 20)
        print(f"  [{tag}] {ip}:{port}  -> OK（key 被接受）")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"  [{tag}] {ip}:{port}  -> FAIL: {type(e).__name__}: {e}")
        return False
    finally:
        try:
            await sender.disconnect()
        except Exception:  # noqa: BLE001
            pass


async def main() -> None:
    cfg = load_config()
    client = fast.make_client(cfg, 1)
    await client.connect()
    try:
        conf = await client(functions.help.GetConfigRequest())
        print("== 服务端下发的 dc_options ==")
        by_dc: dict[int, list] = {}
        for dc in conf.dc_options:
            flags = []
            if dc.ipv6:
                flags.append("ipv6")
            if getattr(dc, "media_only", False):
                flags.append("media_only")
            if dc.cdn:
                flags.append("cdn")
            if getattr(dc, "static", False):
                flags.append("static")
            print(f"  DC{dc.id:<2} {dc.ip_address:<20} :{dc.port:<6} {' '.join(flags)}")
            by_dc.setdefault(dc.id, []).append(dc)

        home = client.session.dc_id
        store = fast.AuthKeyStore(cfg.authkeys_file)
        print(f"\n== 本会话 home DC = {home} ==")
        for i in (1, 2, 3):
            k = store.session_key(i)
            print(f"  sessions.{i}: {'有 key' if k else '无'}")
        for d, ks in store._keys.items():
            print(f"  keys.DC{d}: {len(ks)} 条")

        target = int(sys.argv[1]) if len(sys.argv) > 1 else home
        key_hex = store.session_key(1) if target == home else (store.keys(target) or [None])[0]
        if not key_hex:
            print(f"\n没有 DC{target} 的可用 key，跳过连通性测试")
            return
        key = bytes.fromhex(key_hex) if isinstance(key_hex, str) else key_hex

        opts = by_dc.get(target, [])
        print(f"\n== 用同一把 DC{target} 的 key 依次连该 DC 的所有 IP ==")
        ok_any = False
        for dc in opts:
            ok = await try_ip(client, target, dc.ip_address, dc.port, key,
                              "media" if getattr(dc, "media_only", False) else "normal")
            ok_any = ok_any or ok
        print("\n结论：" + ("至少一个 IP 接受了这把 key" if ok_any else "全部失败"))

        others = {}
        for d in conf.dc_options:
            if d.id != target and not d.ipv6 and (
                d.ip_address.startswith("149.154.167.")
                or d.ip_address.startswith("91.108.56.")
            ):
                others.setdefault(d.ip_address, d)
        if others:
            print(f"\n== 反向验证：拿 DC{target} 的 key 去连其它 DC ==")
            for d in others.values():
                await try_ip(client, d.id, d.ip_address, d.port, key, f"cross-DC{d.id}")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
