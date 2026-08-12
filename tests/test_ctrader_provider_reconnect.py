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
from pynecore_ctrader.wire import (
    CTraderConnectionError,
    CTraderProtocolError,
    CTraderTimeoutError,
)


def _trendbar(
    timestamp: int,
    price: int,
    period: str = "M1",
) -> _model.ProtoOATrendbar:
    return _model.ProtoOATrendbar(
        utcTimestampInMinutes=timestamp // 60,
        period=period,
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
        history_faults: list[Exception] | None = None,
        history_has_more: list[bool] | None = None,
    ) -> None:
        self.history = history
        self.history_responses = list(history_responses or [])
        self.history_faults = list(history_faults or [])
        self.history_has_more = list(history_has_more or [])
        self.requests: list = []

    async def send_request(self, request):
        self.requests.append(request)
        if isinstance(request, _oa.ProtoOAGetTrendbarsReq):
            if self.history_faults:
                raise self.history_faults.pop(0)
            history = (
                self.history_responses.pop(0)
                if self.history_responses
                else self.history
            )
            has_more = self.history_has_more.pop(0) if self.history_has_more else False
            return _oa.ProtoOAGetTrendbarsRes(
                trendbar=history,
                hasMore=has_more,
            )
        if isinstance(request, _oa.ProtoOASubscribeSpotsReq):
            return _oa.ProtoOASubscribeSpotsRes()
        if isinstance(request, _oa.ProtoOASubscribeLiveTrendbarReq):
            return _oa.ProtoOASubscribeLiveTrendbarRes()
        raise AssertionError(f"unexpected request: {type(request).__name__}")


class _ObservedCTrader(CTrader):
    """Provider test seam collecting connected-gap observation hook calls."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.connected_gap_events: list[tuple[str, dict[str, object]]] = []

    def _observe_connected_gap_repair(
        self,
        event: str,
        payload: dict[str, object],
    ) -> None:
        self.connected_gap_events.append((event, dict(payload)))


def _provider(wire: _HistoryWire, timeframe: str = "1") -> _ObservedCTrader:
    config = CTraderConfig(
        demo=True,
        client_id="client",
        client_secret="secret",
        account_id="999",
    )
    provider = _ObservedCTrader(
        symbol="broker:EURUSD",
        timeframe=timeframe,
        config=config,
    )
    provider._wire = wire  # type: ignore[assignment]
    provider._live_account_id = 999
    provider._symbols_by_name = {"EURUSD": 1}
    provider._live_subscription = ("EURUSD", timeframe)
    return provider


def _closed(timestamp: int, price: float = 1.14) -> OHLCV:
    return OHLCV(
        timestamp=timestamp * 1000,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=1.0,
        is_closed=True,
    )


async def _watch(provider: _ObservedCTrader) -> OHLCV:
    provider._spot_events = asyncio.Queue()
    provider._subscribed_symbols.add("EURUSD")
    return await provider.watch_ohlcv("EURUSD", "1")


def _anchor(moment: datetime) -> OHLCV:
    return OHLCV(
        timestamp=int(moment.timestamp()) * 1000,
        open=1.14,
        high=1.14,
        low=1.14,
        close=1.14,
        volume=1.0,
        is_closed=True,
    )


def _freeze(monkeypatch, moment: datetime) -> None:
    class _Now(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment

    monkeypatch.setattr("pynecore_ctrader.provider.datetime", _Now)


class _BlockingHistoryWire(_HistoryWire):
    """History wire whose response is released by the test interleaving."""

    def __init__(
        self,
        history: list[_model.ProtoOATrendbar],
        started: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        super().__init__(history)
        self.started = started
        self.release = release

    async def send_request(self, request):
        if isinstance(request, _oa.ProtoOAGetTrendbarsReq):
            self.requests.append(request)
            self.started.set()
            await self.release.wait()
            return _oa.ProtoOAGetTrendbarsRes(trendbar=self.history)
        return await super().send_request(request)


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
        timestamp=last_ts * 1000,
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

    assert [bar.timestamp for bar in provider._pending_bars] == [missed_ts * 1000]
    history_request = next(
        request for request in wire.requests if isinstance(request, _oa.ProtoOAGetTrendbarsReq)
    )
    assert history_request.fromTimestamp == missed_ts * 1000
    assert history_request.toTimestamp == current_ts * 1000
    assert [type(request) for request in wire.requests[:2]] == [
        _oa.ProtoOASubscribeSpotsReq,
        _oa.ProtoOASubscribeLiveTrendbarReq,
    ]


def __test_reconnect_history_deduplicates_overlapping_live_snapshot_and_quotes__():
    """A backfilled bar is not re-emitted or allowed to leak stale quotes."""
    history_ts = 1_800_000_060
    next_ts = history_ts + 60
    following_ts = next_ts + 60
    provider = _provider(_HistoryWire([]))
    previous = _closed(history_ts - 60)
    history = _closed(history_ts, 1.141)
    provider._last_live_closed_bar = previous
    provider._pending_bars.append(history)
    provider._live_history_bar_ids = {id(history)}

    async def scenario() -> tuple[OHLCV, OHLCV, OHLCV]:
        spot_events = asyncio.Queue()
        provider._spot_events = spot_events
        provider._subscribed_symbols.add("EURUSD")
        provider._watch_symbol_id = 1
        await spot_events.put(
            _oa.ProtoOASpotEvent(
                symbolId=1,
                bid=900_000,
                ask=910_000,
                trendbar=[_trendbar(history_ts, 114_010)],
            )
        )
        await spot_events.put(
            _oa.ProtoOASpotEvent(
                symbolId=1,
                bid=120_000,
                ask=121_000,
                trendbar=[_trendbar(next_ts, 114_020)],
            )
        )
        await spot_events.put(
            _oa.ProtoOASpotEvent(
                symbolId=1,
                bid=130_000,
                ask=131_000,
                trendbar=[_trendbar(following_ts, 114_030)],
            )
        )
        first_result = await provider.watch_ohlcv("EURUSD", "1")
        second_result = await provider.watch_ohlcv("EURUSD", "1")
        third_result = await provider.watch_ohlcv("EURUSD", "1")
        return first_result, second_result, third_result

    history_bar, forming_bar, closed_bar = asyncio.run(scenario())

    assert history_bar is history
    assert forming_bar.timestamp == next_ts * 1000
    assert forming_bar.is_closed is False
    assert closed_bar.timestamp == next_ts * 1000
    assert closed_bar.is_closed is True
    assert closed_bar.close == 1.2
    assert closed_bar.extra_fields is not None
    assert closed_bar.extra_fields["ask_close"] == 1.21
    assert abs(closed_bar.extra_fields["spread"] - 0.01) < 1e-12
    assert provider._last_live_closed_bar is closed_bar
    assert all(bar.timestamp != history_ts * 1000 for bar in provider._pending_bars)


def __test_pending_stream_duplicate_behind_closed_cursor_is_discarded__():
    """The delivery boundary drops a stale closed stream candidate."""
    anchor_ts = 1_800_000_000
    provider = _provider(_HistoryWire([]))
    anchor = _closed(anchor_ts)
    stale = _closed(anchor_ts, 9.0)
    candidate = _closed(anchor_ts + 60, 1.141)
    provider._last_live_closed_bar = anchor
    provider._pending_bars.extend((stale, candidate))

    delivered = asyncio.run(_watch(provider))

    assert delivered is candidate
    assert provider._last_live_closed_bar is candidate
    assert not provider._pending_bars


def __test_live_event_normalizes_period_order_and_current_quotes__():
    """Reversed mixed-period snapshots keep a forward cursor and current quotes."""
    anchor_ts = 1_800_000_000
    first_ts = anchor_ts + 60
    current_ts = anchor_ts + 120
    provider = _provider(_HistoryWire([]))
    provider._last_live_closed_bar = _closed(anchor_ts)

    async def scenario() -> tuple[OHLCV, OHLCV]:
        spot_events = asyncio.Queue()
        provider._spot_events = spot_events
        provider._subscribed_symbols.add("EURUSD")
        provider._watch_symbol_id = 1
        await spot_events.put(
            _oa.ProtoOASpotEvent(
                symbolId=1,
                bid=120_000,
                ask=121_000,
                trendbar=[
                    _trendbar(current_ts, 114_030),
                    _trendbar(first_ts, 900_000, period="M5"),
                    _trendbar(first_ts, 114_020),
                    _trendbar(anchor_ts, 900_000),
                ],
            )
        )
        first_result = await provider.watch_ohlcv("EURUSD", "1")
        second_result = await provider.watch_ohlcv("EURUSD", "1")
        return first_result, second_result

    first_bar, forming_bar = asyncio.run(scenario())

    assert first_bar.timestamp == first_ts * 1000
    assert first_bar.is_closed is True
    assert forming_bar.timestamp == current_ts * 1000
    assert forming_bar.is_closed is False
    assert forming_bar.close == 1.2
    assert forming_bar.extra_fields is not None
    assert forming_bar.extra_fields["ask_close"] == 1.21
    assert abs(forming_bar.extra_fields["spread"] - 0.01) < 1e-12
    assert provider._current_bar_ts == current_ts * 1000


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
        timestamp=last_ts * 1000,
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
    assert [bar.timestamp for bar in provider._pending_bars] == [missed_ts * 1000]


def __test_reconnect_waits_for_the_newest_missing_bar__(monkeypatch):
    """An older slot settling first must not end the recovery early."""
    last_ts = 1_800_000_000
    first_missed = last_ts + 60
    second_missed = last_ts + 120
    current_ts = last_ts + 180
    wire = _HistoryWire(
        [_trendbar(first_missed, 114_010), _trendbar(second_missed, 114_020)],
        history_responses=[[_trendbar(first_missed, 114_010)]],
    )
    provider = _provider(wire)
    provider._live_history_settle_delay_seconds = 0
    provider._last_live_closed_bar = OHLCV(
        timestamp=last_ts * 1000,
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

    assert [bar.timestamp for bar in provider._pending_bars] == [
        first_missed * 1000, second_missed * 1000
    ]


def __test_reconnect_drains_capped_history_pages__(monkeypatch):
    """Every hasMore page is collected before reconnect history is complete."""
    last_ts = 1_800_000_000
    first_missed = last_ts + 60
    second_missed = last_ts + 120
    current_ts = last_ts + 180
    wire = _HistoryWire(
        [],
        history_responses=[
            [_trendbar(second_missed, 114_020)],
            [_trendbar(first_missed, 114_010)],
        ],
        history_has_more=[True, False],
    )
    provider = _provider(wire)
    provider._last_live_closed_bar = _closed(last_ts)

    _freeze(monkeypatch, datetime.fromtimestamp(current_ts + 2, timezone.utc))
    asyncio.run(provider.on_reconnect())

    assert [bar.timestamp for bar in provider._pending_bars] == [
        first_missed * 1000,
        second_missed * 1000,
    ]
    history_requests = [
        request
        for request in wire.requests
        if isinstance(request, _oa.ProtoOAGetTrendbarsReq)
    ]
    assert len(history_requests) == 2
    assert history_requests[0].fromTimestamp == first_missed * 1000
    assert history_requests[0].toTimestamp == current_ts * 1000
    assert history_requests[1].fromTimestamp == first_missed * 1000
    assert history_requests[1].toTimestamp == second_missed * 1000


def __test_reconnect_retries_a_request_scoped_history_failure__(monkeypatch):
    """A timed-out history page is retried instead of ending the reconnect."""
    last_ts = 1_800_000_000
    missed_ts = last_ts + 60
    current_ts = last_ts + 120
    wire = _HistoryWire(
        [_trendbar(missed_ts, 114_010)],
        history_faults=[CTraderTimeoutError("history timed out")],
    )
    provider = _provider(wire)
    provider._live_history_settle_delay_seconds = 0
    provider._last_live_closed_bar = OHLCV(
        timestamp=last_ts * 1000,
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

    assert [bar.timestamp for bar in provider._pending_bars] == [missed_ts * 1000]


def __test_reconnect_retries_after_a_permanently_failing_history_endpoint__(monkeypatch):
    """Unproven history coverage forces a fresh reconnect without queue commit."""
    last_ts = 1_800_000_000
    current_ts = last_ts + 120
    wire = _HistoryWire(
        [],
        history_faults=[CTraderProtocolError("REQUEST_FREQUENCY_EXCEEDED", "slow down")] * 8,
    )
    provider = _provider(wire)
    provider._live_history_settle_delay_seconds = 0
    provider._last_live_closed_bar = OHLCV(
        timestamp=last_ts * 1000,
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
    try:
        asyncio.run(provider.on_reconnect())
    except CTraderConnectionError:
        pass
    else:
        raise AssertionError("incomplete reconnect history was committed")

    assert not provider._pending_bars
    assert "EURUSD" in provider._subscribed_symbols


def __test_reconnect_propagates_a_dead_wire_during_backfill__(monkeypatch):
    """A socket-level failure must reach the runner so it reconnects again."""
    last_ts = 1_800_000_000
    current_ts = last_ts + 120
    wire = _HistoryWire(
        [],
        history_faults=[CTraderConnectionError("not connected")],
    )
    provider = _provider(wire)
    provider._live_history_settle_delay_seconds = 0
    anchor = OHLCV(
        timestamp=last_ts * 1000,
        open=1.14,
        high=1.14,
        low=1.14,
        close=1.14,
        volume=1.0,
        is_closed=True,
    )
    provider._last_live_closed_bar = anchor

    class _Now(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(current_ts + 2, tz=timezone.utc)

    monkeypatch.setattr("pynecore_ctrader.provider.datetime", _Now)
    try:
        asyncio.run(provider.on_reconnect())
    except CTraderConnectionError:
        pass
    else:
        raise AssertionError("dead wire did not surface to the runner")

    assert provider._last_live_closed_bar is anchor


def __test_daily_backfill_follows_the_venue_grid_phase__(monkeypatch):
    """Venue days open at 21:00 UTC, not on the epoch day boundary."""
    opens = [datetime(2027, 6, 30, 21, tzinfo=timezone.utc),
             datetime(2027, 7, 1, 21, tzinfo=timezone.utc),
             datetime(2027, 7, 2, 21, tzinfo=timezone.utc)]
    wire = _HistoryWire([_trendbar(int(moment.timestamp()), 114_000) for moment in opens[1:]])
    provider = _provider(wire, "1D")
    provider._live_history_settle_delay_seconds = 0
    provider._last_live_closed_bar = _anchor(opens[0])

    # Past the epoch midnight of 3 July but inside the day that opened at
    # 2 July 21:00 — the epoch grid would call that forming bar closed.
    _freeze(monkeypatch, datetime(2027, 7, 3, 10, tzinfo=timezone.utc))
    asyncio.run(provider.on_reconnect())

    assert [bar.timestamp for bar in provider._pending_bars] == [
        int(opens[1].timestamp()) * 1000
    ]


def __test_weekly_backfill_follows_the_venue_grid_phase__(monkeypatch):
    """Venue weeks are anchored to a venue opening, not to epoch Thursdays."""
    opens = [datetime(2027, 6, 20, 21, tzinfo=timezone.utc),
             datetime(2027, 6, 27, 21, tzinfo=timezone.utc),
             datetime(2027, 7, 4, 21, tzinfo=timezone.utc)]
    wire = _HistoryWire([_trendbar(int(moment.timestamp()), 114_000) for moment in opens[1:]])
    provider = _provider(wire, "1W")
    provider._live_history_settle_delay_seconds = 0
    provider._last_live_closed_bar = _anchor(opens[0])

    # Past the epoch week boundary (Thursday 8 July) but inside the week that
    # opened on 4 July, which is therefore still forming.
    _freeze(monkeypatch, datetime(2027, 7, 9, 10, tzinfo=timezone.utc))
    asyncio.run(provider.on_reconnect())

    assert [bar.timestamp for bar in provider._pending_bars] == [
        int(opens[1].timestamp()) * 1000
    ]


def __test_monthly_backfill_keeps_the_forming_month_out__(monkeypatch):
    """No fixed length locates a month; the newest opening is still running."""
    opens = [datetime(2027, 5, 1, tzinfo=timezone.utc),
             datetime(2027, 6, 1, tzinfo=timezone.utc),
             datetime(2027, 7, 1, tzinfo=timezone.utc)]
    wire = _HistoryWire([_trendbar(int(moment.timestamp()), 114_000) for moment in opens[1:]])
    provider = _provider(wire, "1M")
    provider._live_history_settle_delay_seconds = 0
    provider._last_live_closed_bar = _anchor(opens[0])

    # The average-month grid puts its boundary at 2027-07-18 22:34:33, so from
    # here on the still-running July bar looks closed to fixed-length maths.
    _freeze(monkeypatch, datetime(2027, 7, 25, 12, tzinfo=timezone.utc))
    asyncio.run(provider.on_reconnect())

    assert [bar.timestamp for bar in provider._pending_bars] == [
        int(opens[1].timestamp()) * 1000
    ]


def __test_monthly_backfill_waits_out_a_short_month__(monkeypatch):
    """A 28-day-old opening may already be closed, so it proves nothing settled."""
    opens = [datetime(2027, 1, 1, tzinfo=timezone.utc),
             datetime(2027, 2, 1, tzinfo=timezone.utc),
             datetime(2027, 3, 1, tzinfo=timezone.utc)]
    bars = [_trendbar(int(moment.timestamp()), 114_000) for moment in opens[1:]]
    # The venue's history read model has not published March yet on the first
    # request; February is 28 days old, i.e. exactly as old as the shortest
    # possible month, so its age cannot tell a forming bar from a closed one.
    wire = _HistoryWire(bars, history_responses=[bars[:1]])
    provider = _provider(wire, "1M")
    provider._live_history_settle_delay_seconds = 0
    provider._last_live_closed_bar = _anchor(opens[0])

    _freeze(monkeypatch, datetime(2027, 3, 1, 0, 1, tzinfo=timezone.utc))
    asyncio.run(provider.on_reconnect())

    assert [bar.timestamp for bar in provider._pending_bars] == [
        int(opens[1].timestamp()) * 1000
    ]


def __test_permanent_history_rejection_is_not_retried__(monkeypatch):
    """A rejection that cannot settle by waiting must not burn the budget."""
    last_ts = 1_800_000_000
    wire = _HistoryWire(
        [],
        history_faults=[CTraderProtocolError("SYMBOL_NOT_FOUND", "no such symbol")] * 8,
    )
    provider = _provider(wire)
    provider._live_history_settle_delay_seconds = 0
    provider._last_live_closed_bar = _anchor(
        datetime.fromtimestamp(last_ts, timezone.utc)
    )

    _freeze(monkeypatch, datetime.fromtimestamp(last_ts + 122, timezone.utc))
    try:
        asyncio.run(provider.on_reconnect())
    except CTraderConnectionError:
        pass
    else:
        raise AssertionError("permanent history rejection was treated as complete")

    history_requests = [
        request
        for request in wire.requests
        if isinstance(request, _oa.ProtoOAGetTrendbarsReq)
    ]
    assert len(history_requests) == 1
    assert "EURUSD" in provider._subscribed_symbols


def __test_closed_live_delivery_advances_the_reconnect_anchor__():
    """Only a closed bar actually handed to PyneCore becomes the cursor."""
    wire = _HistoryWire([])
    provider = _provider(wire)
    closed = OHLCV(
        timestamp=1_800_000_000_000,
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


def __test_connected_gap_returns_deduplicated_history_before_stream_candidate__():
    """A 21:06 history bar precedes the frozen 21:07 stream candidate exactly."""
    anchor_ts = 1_800_000_000
    missing_ts = anchor_ts + 60
    candidate_ts = anchor_ts + 120
    wire = _HistoryWire(
        [
            _trendbar(anchor_ts, 114_000),
            _trendbar(missing_ts, 114_010),
            _trendbar(missing_ts, 114_011),
            _trendbar(candidate_ts, 114_020),
        ]
    )
    provider = _provider(wire)
    anchor = _closed(anchor_ts)
    candidate = _closed(candidate_ts, 1.142)
    later_forming = OHLCV(
        timestamp=(candidate_ts + 60) * 1000,
        open=1.143,
        high=1.143,
        low=1.143,
        close=1.143,
        volume=1.0,
        is_closed=False,
    )
    provider._last_live_closed_bar = anchor
    provider._pending_bars.extend((candidate, later_forming))

    first = asyncio.run(_watch(provider))
    second = asyncio.run(_watch(provider))

    assert first.timestamp == missing_ts * 1000
    assert second is candidate
    assert list(provider._pending_bars) == [later_forming]
    assert provider._last_live_closed_bar is candidate
    history_requests = [
        request
        for request in wire.requests
        if isinstance(request, _oa.ProtoOAGetTrendbarsReq)
    ]
    assert len(history_requests) == 1
    assert history_requests[0].fromTimestamp == missing_ts * 1000
    assert history_requests[0].toTimestamp == candidate_ts * 1000
    assert [event for event, _payload in provider.connected_gap_events] == [
        "started",
        "completed",
    ]
    completed = provider.connected_gap_events[-1][1]
    assert completed["recovered_timestamps"] == [missing_ts * 1000]
    assert completed["candidate_released"] is True


def __test_connected_gap_retry_uses_an_event_loop_timer__(monkeypatch):
    """A temporarily empty history read settles through the bounded loop timer."""
    anchor_ts = 1_800_000_000
    missing_ts = anchor_ts + 60
    candidate_ts = anchor_ts + 120
    missing = _trendbar(missing_ts, 114_010)
    wire = _HistoryWire(
        [missing],
        history_responses=[[], [missing]],
    )
    provider = _provider(wire)
    provider._live_history_settle_delay_seconds = 0.001
    provider._last_live_closed_bar = _closed(anchor_ts)
    provider._pending_bars.append(_closed(candidate_ts))

    async def forbidden_sleep(_seconds: float) -> None:
        raise AssertionError("history retry used asyncio.sleep")

    monkeypatch.setattr("pynecore_ctrader.provider.asyncio.sleep", forbidden_sleep)
    delivered = asyncio.run(_watch(provider))

    assert delivered.timestamp == missing_ts * 1000
    assert len(
        [
            request
            for request in wire.requests
            if isinstance(request, _oa.ProtoOAGetTrendbarsReq)
        ]
    ) == 2


def __test_connected_gap_partial_history_preserves_original_candidate_order__():
    """A partial response is emitted exactly without inventing absent slots."""
    anchor_ts = 1_800_000_000
    returned_ts = anchor_ts + 120
    candidate_ts = anchor_ts + 180
    wire = _HistoryWire([_trendbar(returned_ts, 114_020)])
    provider = _provider(wire)
    provider._live_history_settle_attempts = 1
    anchor = _closed(anchor_ts)
    candidate = _closed(candidate_ts)
    provider._last_live_closed_bar = anchor
    provider._pending_bars.append(candidate)

    first = asyncio.run(_watch(provider))

    assert first.timestamp == returned_ts * 1000
    assert list(provider._pending_bars) == [candidate]
    assert provider._last_live_closed_bar is first
    assert provider.connected_gap_events[-1][1]["recovered_timestamps"] == [
        returned_ts * 1000
    ]


def __test_connected_gap_marks_undrainable_capped_history_incomplete__():
    """A capped page that cannot advance remains explicit failed evidence."""
    anchor_ts = 1_800_000_000
    returned_ts = anchor_ts + 120
    candidate_ts = anchor_ts + 180
    repeated = _trendbar(returned_ts, 114_020)
    wire = _HistoryWire(
        [],
        history_responses=[[repeated], [repeated]],
        history_has_more=[True, False],
    )
    provider = _provider(wire)
    provider._live_history_settle_attempts = 1
    anchor = _closed(anchor_ts)
    provider._last_live_closed_bar = anchor
    candidate = _closed(candidate_ts)
    provider._pending_bars.append(candidate)

    try:
        asyncio.run(_watch(provider))
    except CTraderConnectionError:
        pass
    else:
        raise AssertionError("incomplete connected-gap history was committed")

    assert list(provider._pending_bars) == [candidate]
    assert provider._last_live_closed_bar is anchor
    assert provider.connected_gap_events[-1][0] == "failed"
    assert provider.connected_gap_events[-1][1]["candidate_released"] is False
    assert provider.connected_gap_events[-1][1]["failure_type"] == "IncompleteHistory"
    assert provider.connected_gap_events[-1][1]["recovered_timestamps"] == []


def __test_connected_gap_permanent_rejection_preserves_candidate_and_anchor__():
    """A permanent rejection forces reconnect without cursor or queue commit."""
    anchor_ts = 1_800_000_000
    candidate_ts = anchor_ts + 120
    wire = _HistoryWire(
        [],
        history_faults=[CTraderProtocolError("SYMBOL_NOT_FOUND", "no such symbol")],
    )
    provider = _provider(wire)
    anchor = _closed(anchor_ts)
    provider._last_live_closed_bar = anchor
    candidate = _closed(candidate_ts)
    provider._pending_bars.append(candidate)

    try:
        asyncio.run(_watch(provider))
    except CTraderConnectionError:
        pass
    else:
        raise AssertionError("permanently rejected history released its candidate")

    assert list(provider._pending_bars) == [candidate]
    assert provider._last_live_closed_bar is anchor
    assert [event for event, _payload in provider.connected_gap_events] == [
        "started",
        "failed",
    ]
    failed = provider.connected_gap_events[-1][1]
    assert failed["candidate_released"] is False
    assert failed["recovered_timestamps"] == []


def __test_connected_gap_dead_wire_preserves_candidate_and_anchor__():
    """A socket failure leaves the accepted cursor and queue head untouched."""
    anchor_ts = 1_800_000_000
    candidate_ts = anchor_ts + 120
    wire = _HistoryWire(
        [],
        history_faults=[CTraderConnectionError("not connected")],
    )
    provider = _provider(wire)
    anchor = _closed(anchor_ts)
    candidate = _closed(candidate_ts)
    provider._last_live_closed_bar = anchor
    provider._pending_bars.append(candidate)

    async def scenario() -> None:
        try:
            await _watch(provider)
        except CTraderConnectionError:
            return
        raise AssertionError("dead wire did not propagate")

    asyncio.run(scenario())

    assert list(provider._pending_bars) == [candidate]
    assert provider._last_live_closed_bar is anchor
    assert provider.connected_gap_events[-1][1]["candidate_released"] is False


def __test_connected_gap_rejects_stale_history_after_wire_replacement__():
    """A new wire and queued update invalidate the blocked history result."""
    anchor_ts = 1_800_000_000
    missing_ts = anchor_ts + 60
    candidate_ts = anchor_ts + 120

    async def scenario() -> tuple[_ObservedCTrader, OHLCV, OHLCV, OHLCV]:
        started = asyncio.Event()
        release = asyncio.Event()
        wire = _BlockingHistoryWire(
            [_trendbar(missing_ts, 114_010)],
            started,
            release,
        )
        provider = _provider(wire)
        anchor = _closed(anchor_ts)
        candidate = _closed(candidate_ts)
        next_update = _closed(candidate_ts + 60)
        provider._last_live_closed_bar = anchor
        provider._pending_bars.append(candidate)
        provider._spot_events = asyncio.Queue()
        provider._subscribed_symbols.add("EURUSD")
        task = asyncio.create_task(provider.watch_ohlcv("EURUSD", "1"))
        await started.wait()
        provider._wire = _HistoryWire([])  # type: ignore[assignment]
        provider._pending_bars.append(next_update)
        release.set()
        try:
            await task
        except CTraderConnectionError:
            pass
        else:
            raise AssertionError("stale history result was accepted")
        return provider, anchor, candidate, next_update

    provider, anchor, candidate, next_update = asyncio.run(scenario())

    assert list(provider._pending_bars) == [candidate, next_update]
    assert provider._last_live_closed_bar is anchor
    assert provider.connected_gap_events[-1][1]["failure_type"] == (
        "ConnectedGapStateChanged"
    )


def __test_connected_gap_cancellation_leaves_no_queue_or_cursor_commit__():
    """Cancellation during collection preserves state and leaves no child task."""
    anchor_ts = 1_800_000_000
    candidate_ts = anchor_ts + 120

    async def scenario() -> tuple[_ObservedCTrader, OHLCV, OHLCV]:
        started = asyncio.Event()
        release = asyncio.Event()
        wire = _BlockingHistoryWire([], started, release)
        provider = _provider(wire)
        anchor = _closed(anchor_ts)
        candidate = _closed(candidate_ts)
        provider._last_live_closed_bar = anchor
        provider._pending_bars.append(candidate)
        provider._spot_events = asyncio.Queue()
        provider._subscribed_symbols.add("EURUSD")
        task = asyncio.create_task(provider.watch_ohlcv("EURUSD", "1"))
        await started.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("connected gap collection ignored cancellation")
        remaining = [
            pending
            for pending in asyncio.all_tasks()
            if pending is not asyncio.current_task() and not pending.done()
        ]
        assert not remaining
        return provider, anchor, candidate

    provider, anchor, candidate = asyncio.run(scenario())

    assert list(provider._pending_bars) == [candidate]
    assert provider._last_live_closed_bar is anchor
    assert provider.connected_gap_events[-1][1]["failure_type"] == "CancelledError"


def __test_connected_gap_empty_history_does_not_create_closed_session_bars__():
    """An empty venue response releases the jump without synthetic history."""
    anchor_ts = 1_800_000_000
    candidate_ts = anchor_ts + 3 * 60
    wire = _HistoryWire([])
    provider = _provider(wire)
    provider._live_history_settle_attempts = 1
    provider._last_live_closed_bar = _closed(anchor_ts)
    candidate = _closed(candidate_ts)
    provider._pending_bars.append(candidate)

    delivered = asyncio.run(_watch(provider))

    assert delivered is candidate
    assert not provider._pending_bars
    assert provider.connected_gap_events[-1][1]["recovered_timestamps"] == []


def __test_connected_gap_wire_identity_is_provider_owned_and_monotonic__():
    """Durable wire evidence never depends on a reusable Python object address."""
    first_wire = _HistoryWire([])
    provider = _provider(first_wire)

    assert provider._connection_generation_for_wire(first_wire) == 1  # type: ignore[arg-type]
    first_identity = provider._live_wire_identity
    assert provider._connection_generation_for_wire(first_wire) == 1  # type: ignore[arg-type]
    assert provider._live_wire_identity == first_identity

    second_wire = _HistoryWire([])
    assert provider._connection_generation_for_wire(second_wire) == 2  # type: ignore[arg-type]
    assert provider._live_wire_identity == first_identity + 1
