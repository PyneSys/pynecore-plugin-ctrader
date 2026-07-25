"""
@pyne

Regression coverage for cTrader live OHLCV reconnect backfill.
"""
import asyncio
from datetime import datetime, timezone

from pynecore.types.ohlcv import OHLCV

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model


def _trendbar(timestamp: int, price: int) -> _model.ProtoOATrendbar:
    return _model.ProtoOATrendbar(
        utcTimestampInMinutes=timestamp // 60,
        low=price,
        deltaOpen=1,
        deltaHigh=3,
        deltaClose=2,
        volume=7,
    )


class _HistoryWire:
    """Lowest-seam wire fake recording subscription and history requests."""

    def __init__(
        self,
        history: list[_model.ProtoOATrendbar],
        *,
        history_responses: list[list[_model.ProtoOATrendbar]] | None = None,
    ) -> None:
        self.history = history
        self.history_responses = list(history_responses or [])
        self.requests: list = []

    async def send_request(self, request):
        self.requests.append(request)
        if isinstance(request, _oa.ProtoOAGetTrendbarsReq):
            history = (
                self.history_responses.pop(0)
                if self.history_responses
                else self.history
            )
            return _oa.ProtoOAGetTrendbarsRes(trendbar=history)
        if isinstance(request, _oa.ProtoOASubscribeSpotsReq):
            return _oa.ProtoOASubscribeSpotsRes()
        if isinstance(request, _oa.ProtoOASubscribeLiveTrendbarReq):
            return _oa.ProtoOASubscribeLiveTrendbarRes()
        raise AssertionError(f"unexpected request: {type(request).__name__}")


def _provider(wire: _HistoryWire) -> CTrader:
    config = CTraderConfig(
        demo=True,
        client_id="client",
        client_secret="secret",
        account_id="999",
    )
    provider = CTrader(symbol="broker:EURUSD", timeframe="1", config=config)
    provider._wire = wire  # type: ignore[assignment]
    provider._live_account_id = 999
    provider._symbols_by_name = {"EURUSD": 1}
    provider._live_subscription = ("EURUSD", "1")
    return provider


def __test_reconnect_backfills_only_fully_closed_missing_bars__(monkeypatch):
    """A missed closed slot comes from venue history before queued live data."""
    last_ts = 1_800_000_000
    missed_ts = last_ts + 60
    current_ts = last_ts + 120
    wire = _HistoryWire([
        _trendbar(current_ts, 114_020),
        _trendbar(last_ts, 114_000),
        _trendbar(missed_ts, 114_010),
        _trendbar(missed_ts, 114_011),
    ])
    provider = _provider(wire)
    provider._last_live_closed_bar = OHLCV(
        timestamp=last_ts,
        open=1.14,
        high=1.14,
        low=1.14,
        close=1.14,
        volume=1.0,
        is_closed=True,
    )

    class _Now(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(current_ts + 15, tz=timezone.utc)

    monkeypatch.setattr("pynecore_ctrader.provider.datetime", _Now)
    asyncio.run(provider.on_reconnect())

    assert [bar.timestamp for bar in provider._pending_bars] == [missed_ts]
    history_request = next(
        request for request in wire.requests if isinstance(request, _oa.ProtoOAGetTrendbarsReq)
    )
    assert history_request.fromTimestamp == missed_ts * 1000
    assert history_request.toTimestamp == current_ts * 1000
    assert [type(request) for request in wire.requests[:2]] == [
        _oa.ProtoOASubscribeSpotsReq,
        _oa.ProtoOASubscribeLiveTrendbarReq,
    ]


def __test_reconnect_without_a_closed_anchor_only_restores_subscription__():
    """Initial pre-close reconnect has no safe historical lower bound."""
    wire = _HistoryWire([])
    provider = _provider(wire)

    asyncio.run(provider.on_reconnect())

    assert not any(isinstance(request, _oa.ProtoOAGetTrendbarsReq) for request in wire.requests)
    assert not provider._pending_bars


def __test_reconnect_retries_temporarily_empty_recent_history__(monkeypatch):
    """A just-closed trendbar may settle after the first history response."""
    last_ts = 1_800_000_000
    missed_ts = last_ts + 60
    current_ts = last_ts + 120
    wire = _HistoryWire(
        [_trendbar(missed_ts, 114_010)],
        history_responses=[[]],
    )
    provider = _provider(wire)
    provider._live_history_settle_delay_seconds = 0
    provider._last_live_closed_bar = OHLCV(
        timestamp=last_ts,
        open=1.14,
        high=1.14,
        low=1.14,
        close=1.14,
        volume=1.0,
        is_closed=True,
    )

    class _Now(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(current_ts + 2, tz=timezone.utc)

    monkeypatch.setattr("pynecore_ctrader.provider.datetime", _Now)
    asyncio.run(provider.on_reconnect())

    history_requests = [
        request
        for request in wire.requests
        if isinstance(request, _oa.ProtoOAGetTrendbarsReq)
    ]
    assert len(history_requests) == 2
    assert [bar.timestamp for bar in provider._pending_bars] == [missed_ts]


def __test_closed_live_delivery_advances_the_reconnect_anchor__():
    """Only a closed bar actually handed to PyneCore becomes the cursor."""
    wire = _HistoryWire([])
    provider = _provider(wire)
    closed = OHLCV(
        timestamp=1_800_000_000,
        open=1.14,
        high=1.15,
        low=1.13,
        close=1.145,
        volume=7.0,
        is_closed=True,
    )
    provider._pending_bars.append(closed)

    async def scenario():
        provider._spot_events = asyncio.Queue()
        provider._subscribed_symbols.add("EURUSD")
        return await provider.watch_ohlcv("EURUSD", "1")

    delivered = asyncio.run(scenario())

    assert delivered is closed
    assert provider._last_live_closed_bar is closed
