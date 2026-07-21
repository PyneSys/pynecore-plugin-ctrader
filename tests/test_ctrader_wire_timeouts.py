"""
@pyne

Regression tests for the bounded, interruptible connect / teardown waits on the
cTrader wire.

The startup symbol-info fetch runs the one-shot bridge (``_authed_session`` under
``_run``): a plain ``asyncio.run`` on the CLI main thread. Before the fix,
``WireClient.connect`` awaited ``asyncio.open_connection`` with no bound and
``disconnect`` awaited ``StreamWriter.wait_closed`` with no bound. A broker edge
that accepts TCP but never finishes TLS, or a half-open socket whose close-notify
never returns, wedged that path forever — the exact stall behind
"Fetching symbol info..." hanging. Worse, ``wait_closed`` runs in the
cancellation ``finally`` of the bridge, so an unbounded drain there swallows a
Ctrl-C-driven cancel until an external SIGKILL.

Both waits are now bounded via ``asyncio.wait_for`` (event-driven, no polling),
so a dead peer surfaces a retryable error / a prompt teardown instead of a hang.
"""
import asyncio
import socket
import threading
import time

import pytest

from pynecore.core.plugin import is_retryable_provider_error

from pynecore_ctrader import helpers
from pynecore_ctrader.wire import CTraderConnectionError, WireClient


class _StallingTcpServer:
    """A loopback TCP server that accepts connections and then goes silent.

    A background thread accepts every incoming connection and holds it open
    without ever sending a byte, so a TLS client stalls in its handshake —
    reproducing a broker edge that is reachable at the socket level but
    unresponsive above it. Runs entirely on ``127.0.0.1`` (no venue traffic).
    """

    def __init__(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self._accepted: list[socket.socket] = []
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    @property
    def port(self) -> int:
        return self._sock.getsockname()[1]

    def _accept_loop(self) -> None:
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            self._accepted.append(conn)

    def close(self) -> None:
        self._sock.close()
        for conn in self._accepted:
            conn.close()


class _StallWriter:
    """A ``StreamWriter`` stand-in whose ``wait_closed`` never completes.

    Models a half-open TLS socket: ``close()`` is issued but the close-notify
    exchange with the dead peer never finishes, so ``wait_closed`` awaits
    forever. ``disconnect`` must abandon that drain on its bound rather than
    block on it.
    """

    def __init__(self) -> None:
        self._closing = False
        self._never = asyncio.Event()

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        self._closing = True

    async def wait_closed(self) -> None:
        await self._never.wait()  # never set -> hangs without a bound


def __test_connect_bounds_the_tls_handshake_and_is_retryable__(monkeypatch):
    # A reachable-but-silent socket must not wedge the connect forever: the
    # bounded connect surfaces a retryable CTraderConnectionError so the
    # --broker/--live startup rides it out via its backoff-retry loop.
    monkeypatch.setattr(helpers, "CONNECT_TIMEOUT", 0.5)
    server = _StallingTcpServer()

    async def scenario() -> float:
        wire = WireClient("127.0.0.1", server.port)
        start = time.monotonic()
        with pytest.raises(CTraderConnectionError) as excinfo:
            await wire.connect()
        assert is_retryable_provider_error(excinfo.value)
        return time.monotonic() - start

    try:
        # The outer bound turns a regression (unbounded connect) into a clean
        # test failure instead of a hung suite.
        elapsed = asyncio.run(asyncio.wait_for(scenario(), timeout=5.0))
    finally:
        server.close()

    assert elapsed < 3.0


def __test_connect_wraps_connection_reset_as_retryable__(monkeypatch):
    # A raw ConnectionResetError ([Errno 54]) out of asyncio.open_connection
    # during the TLS handshake must not propagate untranslated: it is wrapped as
    # a retryable CTraderConnectionError so the --broker/--live startup rides it
    # out on its backoff-retry loop instead of dying with a raw traceback.
    async def _boom(*_args, **_kwargs):
        raise ConnectionResetError(54, "Connection reset by peer")

    monkeypatch.setattr(asyncio, "open_connection", _boom)

    async def scenario() -> None:
        wire = WireClient("127.0.0.1", 1)
        with pytest.raises(CTraderConnectionError) as excinfo:
            await wire.connect()
        assert is_retryable_provider_error(excinfo.value)
        assert isinstance(excinfo.value.__cause__, ConnectionResetError)

    asyncio.run(asyncio.wait_for(scenario(), timeout=5.0))


def __test_disconnect_drain_is_bounded_on_a_dead_peer__(monkeypatch):
    # wait_closed cannot complete against a half-open peer; disconnect must
    # abandon the drain on its bound so teardown (which runs in the one-shot
    # bridge's cancellation finally) never wedges and swallows a Ctrl-C.
    monkeypatch.setattr(helpers, "CLOSE_TIMEOUT", 0.5)

    async def scenario() -> float:
        wire = WireClient("127.0.0.1", 1)
        wire._writer = _StallWriter()  # type: ignore[assignment]
        wire._reader = object()  # type: ignore[assignment]
        start = time.monotonic()
        await wire.disconnect()
        return time.monotonic() - start

    # A regression (unbounded wait_closed) makes this raise TimeoutError rather
    # than hang the suite; the fix returns promptly within the drain bound.
    elapsed = asyncio.run(asyncio.wait_for(scenario(), timeout=5.0))
    assert elapsed < 3.0
