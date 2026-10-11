"""Offline workflow contracts for board constituent requests."""

from thspypc._transport import ConnectionManager, ConnectionRole
from thspypc.models import AccountKind, AccountProfile, Capability, Support
from thspypc.services.system_blocks import BoardService


class FakeSocket:
    def __init__(self):
        self.sent = []
        self.timeout = None
        self.closed = False

    def settimeout(self, value):
        self.timeout = value

    def sendall(self, data):
        self.sent.append(data)

    def close(self):
        self.closed = True


def _manager(kind, sock):
    capabilities = {Capability.BASIC_QUOTE: Support.YES}
    if kind is AccountKind.LEVEL2:
        capabilities[Capability.L2_MARKET_ACCESS] = Support.YES
    profile = AccountProfile(
        kind=kind,
        capabilities=capabilities,
    )
    sockets = {}

    def opener(spec):
        # Roles have independent sockets in production; retain one combined
        # send log so the assertions below can verify cross-role wire order.
        current = sock if not sockets else FakeSocket()
        current.sent = sock.sent
        sockets[spec.role] = current
        return current

    return ConnectionManager(profile, opener)


def test_normal_constituents_waits_for_sort_codes_before_quote(monkeypatch):
    sock = FakeSocket()
    responses = iter([b"sort-page", b"quote-page"])
    service = BoardService(
        _manager(AccountKind.STANDARD, sock),
        frame_reader=lambda _sock: next(responses),
        max_frames=1,
    )
    sorted_codes = [{"code": "688001"}, {"code": "600001"}]
    quotes = [{"code": "688001", "dt10": 1.0}, {"code": "600001", "dt10": 2.0}]
    monkeypatch.setattr(
        "thspypc.services.system_blocks.parse_board_constituents_selection_response",
        lambda body: sorted_codes if body == b"sort-page" else [],
    )
    monkeypatch.setattr(
        "thspypc.services.system_blocks.parse_board_constituents_response",
        lambda body: quotes if body == b"quote-page" else [],
    )

    result = service.board_constituents(
        ["600001", "688001"],
        stock_markets={"600001": 17, "688001": 17},
    )

    assert result == quotes
    assert len(sock.sent) == 4
    assert b"pageid=392" in sock.sent[0]
    assert b"SortType=Sort" in sock.sent[1]
    assert b"DataType=527527," in sock.sent[2]
    assert b"CodeList=17(688001,600001,);" in sock.sent[2]
    assert b"DataType=7,13,48,19" in sock.sent[3]
    assert b"\x12\x00\x0f\x00\x44\x01" in sock.sent[1]
    assert service._connections.peek(
        ConnectionRole.BOARD_CONSTITUENT_SH
    ) is not None


def test_level2_constituents_uses_complete_exchange_batches(monkeypatch):
    sock = FakeSocket()
    responses = iter([b"sh-quotes", b"sz-quotes"])
    service = BoardService(
        _manager(AccountKind.LEVEL2, sock),
        frame_reader=lambda _sock: next(responses),
        max_frames=1,
    )
    by_response = {
        b"sh-quotes": [
            {"code": "600001"},
            {"code": "600002"},
            {"code": "688001"},
        ],
        b"sz-quotes": [
            {"code": "000001"},
            {"code": "300001"},
            {"code": "300002"},
        ],
    }
    monkeypatch.setattr(
        "thspypc.services.system_blocks.parse_board_constituents_response",
        lambda body: by_response.get(body, []),
    )

    result = service.board_constituents(
        ["600001", "000001", "688001", "300001", "600002", "300002"],
        stock_markets={
            "600001": 17,
            "600002": 17,
            "000001": 33,
            "688001": 22,
            "300001": 33,
            "300002": 33,
        },
    )

    assert [record["code"] for record in result] == [
        "600001", "600002", "688001", "000001", "300001", "300002",
    ]
    assert len(sock.sent) == 3
    assert b"pageid=5716" in sock.sent[0]
    assert b"CodeList=17(600001,600002,);22(688001,);" in sock.sent[1]
    assert b"CodeList=33(000001,300001,300002,);" in sock.sent[2]
    assert b"CodeList=16(1A0002,);" in sock.sent[1]
    assert b"CodeList=32(399002,);" in sock.sent[2]
    assert b"\x12\x00\x09\x00\x5c\x01" in sock.sent[1]
    assert b"CodeList=33(000001" not in sock.sent[1]
    assert b"CodeList=17(600001" not in sock.sent[2]
    assert service._connections.peek(
        ConnectionRole.BOARD_CONSTITUENT_SH
    ) is not None
    assert service._connections.peek(
        ConnectionRole.BOARD_CONSTITUENT_SZ
    ) is not None


def test_level2_constituents_skips_tiny_exchange_group(monkeypatch):
    # 2026-09-16 实测：shlv2 网关对 <3 只的市场组不回 0x64 表，等满超时
    # 也等不到（血氧仪 886028 的 2 只沪市成分曾因此烧光全部预算）。
    # 小组必须被跳过、不发查询，由门面走单股行情补齐。
    sock = FakeSocket()
    responses = iter([b"sz-quotes"])
    service = BoardService(
        _manager(AccountKind.LEVEL2, sock),
        frame_reader=lambda _sock: next(responses),
        max_frames=1,
    )
    monkeypatch.setattr(
        "thspypc.services.system_blocks.parse_board_constituents_response",
        lambda body: (
            [{"code": "000001"}, {"code": "300001"}, {"code": "300002"}]
            if body == b"sz-quotes"
            else []
        ),
    )

    result = service.board_constituents(
        ["600001", "000001", "688001", "300001", "300002"],
        stock_markets={
            "600001": 17,
            "000001": 33,
            "688001": 22,
            "300001": 33,
            "300002": 33,
        },
    )

    # 沪组（2 只）未发任何查询；深组正常返回
    assert [record["code"] for record in result] == [
        "000001", "300001", "300002",
    ]
    assert len(sock.sent) == 1
    assert b"CodeList=33(000001,300001,300002,);" in sock.sent[0]
    assert service._connections.peek(
        ConnectionRole.BOARD_CONSTITUENT_SH
    ) is None
    assert service._connections.peek(
        ConnectionRole.BOARD_CONSTITUENT_SZ
    ) is not None


def test_level2_constituents_failed_group_does_not_starve_next(monkeypatch):
    # 每组独立预算：沪组不应答（解析永远为空）只消耗自己的份额，
    # 深组仍要执行并返回，不再被 remaining<=0 直接 break 掉。
    import time as _time

    sock = FakeSocket()
    responses = iter([b"sh-silence", b"sz-quotes"])
    service = BoardService(
        _manager(AccountKind.LEVEL2, sock),
        frame_reader=lambda _sock: next(responses),
        max_frames=1,
    )
    monkeypatch.setattr(
        "thspypc.services.system_blocks.parse_board_constituents_response",
        lambda body: (
            [{"code": "000001"}, {"code": "300001"}, {"code": "300002"}]
            if body == b"sz-quotes"
            else []
        ),
    )

    started = _time.monotonic()
    result = service.board_constituents(
        ["600001", "000001", "688001", "600002", "300001", "300002"],
        stock_markets={
            "600001": 17,
            "600002": 17,
            "000001": 33,
            "688001": 22,
            "300001": 33,
            "300002": 33,
        },
        timeout=0.4,
    )

    assert [record["code"] for record in result] == [
        "000001", "300001", "300002",
    ]
    # 沪组烧掉自己的预算份额（timeout/2）后深组照常执行
    assert _time.monotonic() - started < 0.4
    assert b"CodeList=33(000001,300001,300002,);" in sock.sent[-1]
