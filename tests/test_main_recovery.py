"""Offline regression tests for broken MAIN sockets and heartbeat recovery."""
import concurrent.futures
import threading

import pytest

from thspypc.client import LoginResult, THSClient
from thspypc._transport import ConnectionManager, ConnectionRole
from thspypc.models import AccountKind, AccountProfile, Capability, Support


class FakeSocket:
    def __init__(self, broken=False):
        self.broken = broken
        self.closed = False
        self.sent = []

    def settimeout(self, _value):
        pass

    def sendall(self, data):
        self.sent.append(data)
        if self.broken or self.closed:
            raise BrokenPipeError(32, "Broken pipe")

    def close(self):
        self.closed = True


def ready_client(monkeypatch):
    client = THSClient("offline-user", "offline-password", enable_heartbeat=False)
    old = FakeSocket(broken=True)
    client._sock = old
    client._account_evidence.record_main_ready()
    manager = client._ensure_default_service_context()
    monkeypatch.setattr(
        "thspypc.client.probe_socket_alive", lambda sock: not sock.closed,
    )
    return client, old, manager


def mock_reconnect(monkeypatch, client, new):
    calls = []

    def connect_main(*, refresh_auth=False):
        calls.append(refresh_auth)
        client._sock = new
        return LoginResult(success=True, server="fake:8901")

    monkeypatch.setattr(client._connection_factory, "connect_main", connect_main)
    return calls


def test_dde_broken_pipe_rebinds_main_before_one_retry(monkeypatch):
    client, old, manager = ready_client(monkeypatch)
    new = FakeSocket()
    calls = mock_reconnect(monkeypatch, client, new)
    expected = [{"code": "000001", "value": 12.34}]
    sockets = []

    def dde_ranked(**_kwargs):
        connection = manager.acquire(ConnectionRole.MAIN)
        sockets.append(connection.socket)
        with connection.request(b"DDE", timeout=1):
            return expected

    monkeypatch.setattr(client._stock_list_service, "dde_ranked", dde_ranked)
    assert client.dde_rank(with_names=False) == expected
    assert sockets == [old, new]
    assert old.closed and not new.closed
    assert calls == [True]  # fresh HTTP auth, never reuse a consumed passport
    assert manager.peek(ConnectionRole.MAIN).socket is new


def test_dde_stops_after_the_second_broken_pipe(monkeypatch):
    client, old, manager = ready_client(monkeypatch)
    new = FakeSocket(broken=True)
    calls = mock_reconnect(monkeypatch, client, new)

    def dde_ranked(**_kwargs):
        with manager.acquire(ConnectionRole.MAIN).request(b"DDE", timeout=1):
            return []

    monkeypatch.setattr(client._stock_list_service, "dde_ranked", dde_ranked)
    with pytest.raises(BrokenPipeError):
        client.dde_rank(with_names=False)
    assert len(old.sent) == len(new.sent) == 1
    assert calls == [True]


def test_dde_http_request_recovers_broken_pipe_instead_of_502(monkeypatch, tmp_path):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from thspypc.server.app import create_app
    from thspypc.server.runtime import ThsRuntime

    client, _old, manager = ready_client(monkeypatch)
    calls = mock_reconnect(monkeypatch, client, FakeSocket())
    expected = [{"code": "000001", "value": 12.34}]

    def dde_ranked(**_kwargs):
        with manager.acquire(ConnectionRole.MAIN).request(b"DDE", timeout=1):
            return expected

    monkeypatch.setattr(client._stock_list_service, "dde_ranked", dde_ranked)
    runtime = ThsRuntime(env_path=tmp_path / "missing.env")
    runtime._client = client
    runtime._connected_once = True
    response = TestClient(create_app(runtime)).get(
        "/api/dde_rank", params={"sort_by": 592888, "count": 58, "with_names": "false"},
    )
    assert response.status_code == 200
    assert response.json() == expected
    assert calls == [True]


def test_main_recovery_does_not_close_other_lanes(monkeypatch):
    client, old, manager = ready_client(monkeypatch)
    fast = FakeSocket()
    manager.adopt(ConnectionRole.KLINE_FAST, fast, owns_socket=True)
    new = FakeSocket()
    calls = mock_reconnect(monkeypatch, client, new)

    client._recover_main_connection(old)
    assert calls == [True]
    assert manager.peek(ConnectionRole.KLINE_FAST).socket is fast
    assert not fast.closed


def test_concurrent_main_recovery_logs_in_only_once(monkeypatch):
    client, old, manager = ready_client(monkeypatch)
    entered = threading.Event()
    release = threading.Event()
    new = FakeSocket()
    calls = []

    def connect_main(*, refresh_auth=False):
        calls.append(refresh_auth)
        entered.set()
        assert release.wait(2)
        client._sock = new
        return LoginResult(success=True)

    monkeypatch.setattr(client._connection_factory, "connect_main", connect_main)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(client._recover_main_connection, old) for _ in range(8)]
        assert entered.wait(2)
        release.set()
        assert all(future.result(timeout=2).success for future in futures)
    assert calls == [True]
    assert manager.peek(ConnectionRole.MAIN).socket is new


def test_dead_main_is_reconnected_before_query(monkeypatch):
    client, old, _manager = ready_client(monkeypatch)
    old.close()
    calls = mock_reconnect(monkeypatch, client, FakeSocket())
    client._ensure_main_connection()
    assert calls == [True]


def test_failed_reconnect_does_not_resend_on_old_socket(monkeypatch):
    client, old, _manager = ready_client(monkeypatch)
    monkeypatch.setattr(
        client._connection_factory, "connect_main",
        lambda **_kwargs: LoginResult(success=False, error="http_auth_failed"),
    )
    with pytest.raises(ConnectionError, match="MAIN 重连失败"):
        client._recover_main_connection(old)
    assert old.closed and client._sock is None


def test_shutdown_does_not_allow_background_reconnection(monkeypatch):
    client, old, _manager = ready_client(monkeypatch)
    calls = mock_reconnect(monkeypatch, client, FakeSocket())
    client.disconnect()
    with pytest.raises(ConnectionError, match="客户端已关闭"):
        client._recover_main_connection(old)
    assert calls == []


def test_transport_failure_schedules_single_background_recovery(monkeypatch):
    client, sock, _manager = ready_client(monkeypatch)
    runtime = client._connection_runtime
    runtime.enable_heartbeat = True
    entered = threading.Event()
    release = threading.Event()
    done = threading.Event()
    calls = []

    def recover(lane, failed):
        calls.append((lane, failed))
        entered.set()
        assert release.wait(2)
        done.set()

    runtime._recover_connection = recover
    runtime._bind_heartbeat_lane("main", sock)
    probe_id = runtime._heartbeat_monitor.begin_probe("main", sock)
    future = concurrent.futures.Future()
    future.set_exception(BrokenPipeError(32, "Broken pipe"))
    runtime._finish_probe_future("main", sock, probe_id, future)
    assert entered.wait(2)
    assert not runtime._schedule_connection_recovery("main", sock)
    release.set()
    assert done.wait(2)
    assert calls == [("main", sock)]


def test_probe_timeout_alone_does_not_schedule_recovery(monkeypatch):
    client, sock, _manager = ready_client(monkeypatch)
    runtime = client._connection_runtime
    runtime.enable_heartbeat = True
    runtime._recover_connection = lambda *_args: pytest.fail("ACK silence is not a dead socket")
    runtime._bind_heartbeat_lane("main", sock)
    for _ in range(2):
        probe_id = runtime._heartbeat_monitor.begin_probe("main", sock)
        future = concurrent.futures.Future()
        future.set_exception(TimeoutError("probe deadline"))
        runtime._finish_probe_future("main", sock, probe_id, future)
    assert runtime.heartbeat_status()["recovery"]["attempts"] == 0


def test_registry_can_be_accessed_while_a_role_is_opening():
    profile = AccountProfile(
        kind=AccountKind.STANDARD,
        capabilities={Capability.BASIC_QUOTE: Support.YES},
    )
    entered = threading.Event()
    release = threading.Event()

    def opener(_spec):
        entered.set()
        assert release.wait(2)
        return FakeSocket()

    manager = ConnectionManager(profile, opener)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        opened = pool.submit(manager.acquire, ConnectionRole.MAIN)
        assert entered.wait(2)
        lookup = pool.submit(manager.peek, ConnectionRole.MAIN)
        try:
            assert lookup.result(timeout=1) is None
        finally:
            release.set()
        assert opened.result(timeout=2).active


def test_late_failed_generation_does_not_close_replacement(monkeypatch):
    client, old, manager = ready_client(monkeypatch)
    new = FakeSocket()
    calls = mock_reconnect(monkeypatch, client, new)
    client._recover_main_connection(old)
    assert not manager.close_if_socket(ConnectionRole.MAIN, old)
    assert client._recover_main_connection(old).success
    assert calls == [True]
    assert not new.closed


def test_heartbeat_recovery_failures_are_rate_limited_and_retried(monkeypatch):
    client, old, _manager = ready_client(monkeypatch)
    runtime = client._connection_runtime
    runtime.enable_heartbeat = True
    calls = []

    class InlineThread:
        def __init__(self, *, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    def fail(lane, failed):
        calls.append((lane, failed))
        raise ConnectionError("network unavailable")

    monkeypatch.setattr("thspypc._client.connection_runtime.threading.Thread", InlineThread)
    runtime._recover_connection = fail
    assert runtime._schedule_connection_recovery("main", old)
    assert not runtime._schedule_connection_recovery("main", old)
    assert len(calls) == 1
    runtime._recovery_last_attempt["main"] -= runtime.RECOVERY_RETRY_INTERVAL + 1
    runtime._retry_connection_recoveries()
    assert len(calls) == 2
    recovery = runtime.heartbeat_status()["recovery"]
    assert recovery["failures"] == 2
    assert recovery["pending_lanes"] == ["main"]


def test_kline_heartbeat_recovery_does_not_drop_main(monkeypatch):
    client, main, manager = ready_client(monkeypatch)
    old_fast = FakeSocket(broken=True)
    new_fast = FakeSocket()
    manager.adopt(ConnectionRole.KLINE_FAST, old_fast, owns_socket=True)
    calls = []

    def open_fast():
        calls.append(True)
        assert old_fast.closed
        return new_fast

    monkeypatch.setattr(client, "_open_independent_main_connection", open_fast)
    client._recover_heartbeat_connection("kline_fast", old_fast)
    assert calls == [True]
    assert client._sock is main and not main.closed
    assert manager.peek(ConnectionRole.KLINE_FAST).socket is new_fast


def test_main_factory_does_not_re_adopt_a_dead_socket(monkeypatch):
    client, old, manager = ready_client(monkeypatch)
    old.close()
    new = FakeSocket()
    calls = mock_reconnect(monkeypatch, client, new)
    manager.close(ConnectionRole.MAIN)
    connection = manager.acquire(ConnectionRole.MAIN)
    assert connection.socket is new
    assert calls == [True]


def test_failed_init_restarts_beats_to_retry_recovery(monkeypatch):
    client, old, _manager = ready_client(monkeypatch)
    runtime = client._connection_runtime
    runtime.enable_heartbeat = True
    restarted = []

    class InlineThread:
        def __init__(self, *, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    def fail_init(*_args):
        runtime.heartbeat_stop.set()
        raise ConnectionError("MAIN init failed")

    def restart():
        restarted.append(True)
        runtime.heartbeat_stop.clear()

    monkeypatch.setattr("thspypc._client.connection_runtime.threading.Thread", InlineThread)
    monkeypatch.setattr(runtime, "start_heartbeat", restart)
    runtime._recover_connection = fail_init
    assert runtime._schedule_connection_recovery("main", old)
    assert restarted == [True]
    assert runtime.heartbeat_status()["recovery"]["pending_lanes"] == ["main"]
