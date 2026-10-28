"""CLI 小工具：进度条、体积格式化。"""

from __future__ import annotations

import time

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

console = Console()
err_console = Console(stderr=True)


def human_size(num: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(num) < 1024 or unit == "TiB":
            return f"{num:.2f} {unit}" if unit != "B" else f"{int(num)} B"
        num /= 1024
    return f"{num:.2f} TiB"


def parse_size(text: str) -> int:
    text = text.strip().upper().replace("IB", "B").replace("B", "")
    units = {"": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}
    for suffix, mult in sorted(units.items(), key=lambda kv: -len(kv[0])):
        if suffix and text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * mult)
    return int(float(text))


def parse_target(text: str) -> tuple[int, int]:
    """解析 ``chat_id:message_id``。

    chat_id 可以是 ``-1001234567890``（频道/超级群）、``-987654321``（群）
    或正数（用户/自己）。

    message_id 两种写法都接受：MTProto 的原始 id（如 ``6733``），
    或 TDLib 的 id（``6733 << 20``）。
    """
    raw = (text or "").strip()
    if ":" not in raw:
        raise ValueError(f"格式应为 chat_id:message_id，收到的是 {raw!r}")
    left, _, right = raw.partition(":")
    try:
        chat_id = int(left.strip())
        message_id = int(right.strip())
    except ValueError as exc:
        raise ValueError(f"chat_id 和 message_id 都必须是整数，收到的是 {raw!r}") from exc
    if message_id <= 0:
        raise ValueError(f"message_id 必须是正整数，收到 {message_id}")
    return chat_id, message_id


# TDLib 把服务端 message_id 存在高位：MessageId(server_id) == server_id << 20
# 见 td/telegram/MessageId.h:59 与 MessageId.h:26 SERVER_ID_SHIFT
TDLIB_MESSAGE_SHIFT = 20
_TDLIB_MESSAGE_MASK = (1 << TDLIB_MESSAGE_SHIFT) - 1


def is_tdlib_message_id(message_id: int) -> bool:
    """判断这个 id 是不是 TDLib 风格的（低位 20 bit 全 0）。"""
    return message_id >= (1 << TDLIB_MESSAGE_SHIFT) and (message_id & _TDLIB_MESSAGE_MASK) == 0


def to_tdlib_message_id(message_id: int) -> int:
    return message_id if is_tdlib_message_id(message_id) else message_id << TDLIB_MESSAGE_SHIFT


def to_mtproto_message_id(message_id: int) -> int:
    return message_id >> TDLIB_MESSAGE_SHIFT if is_tdlib_message_id(message_id) else message_id


class ProgressBar:
    """把 (done, total) 回调渲染成 rich 进度条。"""

    def __init__(self, label: str):
        self._progress = Progress(
            TextColumn("[bold]{task.description}"),
            BarColumn(bar_width=None),
            TextColumn("{task.fields[conns]}", style="cyan"),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        )
        self._task_id = None
        self._label = label
        self._started = time.monotonic()

    def __enter__(self) -> "ProgressBar":
        self._progress.start()
        self._task_id = self._progress.add_task(self._label, total=None, conns="")
        return self

    def __exit__(self, *exc) -> None:
        self._progress.stop()

    def update(self, done: int, total: int) -> None:
        if self._task_id is None:
            return
        self._progress.update(self._task_id, completed=done, total=total or None)

    def update_conns(self, speeds: list[float]) -> None:
        """在进度条同一行显示每条 TCP 连接的实时速度（MiB/s）。

        按终端宽度决定显示几条，避免挤到换行。
        """
        if self._task_id is None or not speeds:
            return
        budget = max(4, (console.width - 70) // 5)   # 每个数字约 5 列
        shown = speeds[:budget]
        text = " ".join(f"{s / 1048576:.1f}" for s in shown)
        if len(speeds) > len(shown):
            text += f" +{len(speeds) - len(shown)}"
        self._progress.update(self._task_id, conns=text)
