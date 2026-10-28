"""chat_id:message_id 解析测试。"""

from __future__ import annotations

import pytest

from tgdl.util import parse_target


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("8913476869:6733", (8913476869, 6733)),
        ("-1001234567890:42", (-1001234567890, 42)),
        ("-987654321:1", (-987654321, 1)),
        ("  123:456  ", (123, 456)),
    ],
)
def test_parse_ok(text, expected):
    assert parse_target(text) == expected


@pytest.mark.parametrize("text", ["8913476869", "abc:1", "1:abc", "1:0", "1:-5", ""])
def test_parse_bad(text):
    with pytest.raises(ValueError):
        parse_target(text)


def test_message_id_conversion():
    from tgdl.util import is_tdlib_message_id, to_mtproto_message_id, to_tdlib_message_id

    raw = 6733
    tdlib = raw << 20
    assert not is_tdlib_message_id(raw)
    assert is_tdlib_message_id(tdlib)
    assert to_tdlib_message_id(raw) == tdlib
    assert to_mtproto_message_id(tdlib) == raw
    # 幂等
    assert to_tdlib_message_id(tdlib) == tdlib
    assert to_mtproto_message_id(raw) == raw
