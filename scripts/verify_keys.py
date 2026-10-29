"""体检 auth key 池：找出「不属于这个账号」的 key 并清掉。

背景：早期版本把**所有会话**借来的 DC key 混在一个全局池子里（`keys.DCx`），
迁移到「按会话分组」时这些 key 全归到了会话 1 名下。问题是派生 key 是账号作用域的
——拿账号 A 的 key 去下账号 A 取的消息，服务端会回 FILE_REFERENCE_INVALID。

判据很简单：用这把 key 真去读 4 KiB 数据。能读通就是本账号的；回
FILE_REFERENCE_INVALID 就是别人的（或者真的作废了），删掉重派生即可。

    uv run python scripts/verify_keys.py --target -1004389561712:1253048320 --session 1
"""

from __future__ import annotations

import argparse
import asyncio

from tgdl import fast
from tgdl.config import load_config
from tgdl.util import parse_target


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True, help="chat_id:message_id")
    ap.add_argument("--session", type=int, default=1)
    ap.add_argument("--dry-run", action="store_true", help="只报告，不删除")
    args = ap.parse_args()

    cfg = load_config()
    chat_id, message_id = parse_target(args.target)
    store = fast.AuthKeyStore(cfg.authkeys_file)

    client = fast.make_client(cfg, args.session)
    await client.connect()
    try:
        _, errors, _, types = fast._telethon()
        message = await fast._fetch_message(client, chat_id, message_id)
        location, size, name, dc_id = fast._location_from_message(message, types)
        print(f"目标 {name}  {size / 1048576:.1f} MB  DC{dc_id}  会话 {args.session}")

        keys = store.keys(dc_id, args.session)
        print(f"池里有 {len(keys)} 条 DC{dc_id} key，逐条试读 4 KiB：")
        bad: list[str] = []
        for i, hexed in enumerate(keys, 1):
            try:
                sender = await fast._open_with_key(client, dc_id, bytes.fromhex(hexed))
            except Exception as exc:  # noqa: BLE001
                print(f"  #{i} {hexed[:8]}  连不上：{type(exc).__name__}")
                continue
            try:
                data = await fast._get_file(client, sender, location, 0, 4096, cdn=False)
                print(f"  #{i} {hexed[:8]}  OK（读到 {len(data)} 字节）")
            except errors.FileReferenceExpiredError:
                print(f"  #{i} {hexed[:8]}  ✗ FILE_REFERENCE_INVALID —— 不是这个账号的 key")
                bad.append(hexed)
            except Exception as exc:  # noqa: BLE001
                print(f"  #{i} {hexed[:8]}  读失败：{type(exc).__name__}: {str(exc)[:60]}")
            finally:
                await fast._safe_disconnect(sender)

        if not bad:
            print("没有发现别人的 key。")
            return
        if args.dry_run:
            print(f"发现 {len(bad)} 条脏 key（--dry-run 未删除）")
            return
        for hexed in bad:
            store.drop(dc_id, hexed, args.session)
        print(f"已从会话 {args.session} 的 DC{dc_id} 池里删掉 {len(bad)} 条")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
