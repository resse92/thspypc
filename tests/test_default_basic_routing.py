"""Default L1 routing must not depend on account kind or concurrent L2 leases."""
import pytest

from thspypc.client import THSClient
from thspypc._transport import ConnectionRole
from thspypc.models import AccountKind, Capability, Support


class FakeSocket:
    def settimeout(self, _value):
        pass

    def close(self):
        pass


def basic_client(kind):
    client = THSClient("offline-user", "offline-password")
    client._sock = FakeSocket()
    client._account_evidence.record_main_ready()
    if kind is AccountKind.LEVEL2:
        client._account_evidence.record_l2_entitlement(Support.YES)
    elif kind is AccountKind.STANDARD:
        client._account_evidence.record_l2_entitlement(Support.NO)
    client._ensure_default_service_context()
    return client


@pytest.mark.parametrize("kind", list(AccountKind))
@pytest.mark.parametrize(
    "public_method,service_name,service_method,args,kwargs",
    [
        ("timeline", "_timeline_service", "timeline", ("000001",), {}),
        ("history_timeline", "_timeline_service", "history_timeline", ("000001", "2026-10-09"), {}),
        ("auction", "_auction_service", "auction", ("000001",), {}),
        ("closing_auction", "_auction_service", "closing_auction", ("000001",), {}),
        ("kline", "_kline_service", "kline", ("000001",), {"retries": 0}),
        ("stock_list_hot", "_stock_list_service", "ranked", (), {}),
        ("dde_rank", "_stock_list_service", "dde_ranked", (), {"with_names": False}),
        ("stock_list", "_stock_list_service", "full_list", (), {}),
    ],
)
def test_basic_public_methods_do_not_lease_l2(
    monkeypatch, kind, public_method, service_name, service_method, args, kwargs,
):
    client = basic_client(kind)
    monkeypatch.setattr(THSClient, "is_connected", property(lambda _self: True))
    service = getattr(client, service_name)
    calls = []
    expected = [{"code": "000001", "market": 33, "dt10": 12.34}]

    def operation(*_args, **_kwargs):
        calls.append(service._connections.profile.kind)
        assert all(
            not capability.name.startswith("L2_")
            for capability in client._service_capability_leases
        )
        return expected

    monkeypatch.setattr(service, service_method, operation)
    assert getattr(client, public_method)(*args, **kwargs) == expected
    assert calls == [AccountKind.STANDARD]
    assert client.observed_account_profile.kind is kind


def test_basic_views_remain_l1_during_an_l2_call():
    client = basic_client(AccountKind.UNKNOWN)
    manager = client._service_connections

    def l2_operation():
        assert manager.profile.kind is AccountKind.LEVEL2
        assert client._superorder_service._connections is manager
        for name in (
            "_quote_service", "_kline_service", "_timeline_service",
            "_auction_service", "_stock_list_service", "_stock_name_service",
            "_board_service",
        ):
            assert getattr(client, name)._connections.profile.kind is AccountKind.STANDARD

    client._run_default_service((Capability.L2_TIMELINE,), l2_operation)
    assert client.observed_account_profile.kind is AccountKind.UNKNOWN


@pytest.mark.parametrize("kind", list(AccountKind))
def test_stock_ready_does_not_register_l2_by_default(monkeypatch, kind):
    client = basic_client(kind)

    def reject_l2(*_args, **_kwargs):
        pytest.fail("basic stock-ready must not register L2")

    monkeypatch.setattr(client, "_register_l2_snapshot_code", reject_l2)
    assert client.market_events_prepare("000001") == 33


def test_explicit_kline_level2_still_leases_l2(monkeypatch):
    client = basic_client(AccountKind.LEVEL2)
    monkeypatch.setattr(THSClient, "is_connected", property(lambda _self: True))

    def operation(*_args, **kwargs):
        assert kwargs["channel"] == "level2"
        assert Capability.L2_TIMELINE in client._service_capability_leases
        assert client._kline_service._connections.source_profile.kind is AccountKind.LEVEL2
        return []

    monkeypatch.setattr(client._kline_service, "kline", operation)
    assert client.kline("000001", channel="level2", retries=0) == []


@pytest.mark.parametrize("method", ["kline", "depth_quote"])
def test_explicit_l2_requests_bypass_basic_routing_view(monkeypatch, method):
    client = basic_client(AccountKind.LEVEL2)
    monkeypatch.setattr(THSClient, "is_connected", property(lambda _self: True))
    requested = []

    def acquire(role, **kwargs):
        requested.append((role, kwargs["capability"]))
        raise RuntimeError("explicit L2 requested")

    monkeypatch.setattr(client._service_connections, "acquire", acquire)
    kwargs = {"channel": "level2"} if method == "kline" else {"ten_levels": True}
    with pytest.raises(RuntimeError, match="explicit L2 requested"):
        getattr(client, method)("000001", retries=0, **kwargs)
    assert requested[0][0] is ConnectionRole.SZ_L2
    assert requested[0][1].name.startswith("L2_")
