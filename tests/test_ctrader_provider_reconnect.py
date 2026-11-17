"""
@pyne

Regression coverage for the cTrader live stream and its closed-bar history query.
"""
import asyncio
from datetime import date, datetime, time, timezone

from pynecore.core.syminfo import SymInfo, SymInfoInterval
from pynecore.types.ohlcv import OHLCV

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader import provider as _provider_module
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model
from pynecore_ctrader.wire import (
    CTraderConnectionError,
    CTraderProtocolError,
    CTraderTimeoutError,
    CTraderWireError,
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


class _SymbolInfoWire:
    """Symbol metadata wire with configurable initial rate limits."""

    def __init__(
            self,
            blocked_requests: int = 1,
            holidays: list[_model.ProtoOAHoliday] | None = None,
    ) -> None:
        self.blocked_requests = blocked_requests
        self.symbol_list_requests = 0
        self.holidays = holidays or []

    async def send_request(self, request):
        if isinstance(request, _oa.ProtoOASymbolsListReq):
            self.symbol_list_requests += 1
            if self.symbol_list_requests <= self.blocked_requests:
                raise CTraderProtocolError(
                    "BLOCKED_PAYLOAD_TYPE", "You are being rate limited"
                )
            return _oa.ProtoOASymbolsListRes(
                symbol=[
                    _model.ProtoOALightSymbol(
                        symbolId=1,
                        symbolName="EURUSD",
                        enabled=True,
                        baseAssetId=1,
                        quoteAssetId=2,
                    )
                ]
            )
        if isinstance(request, _oa.ProtoOAAssetListReq):
            return _oa.ProtoOAAssetListRes(
                asset=[
                    _model.ProtoOAAsset(assetId=1, name="EUR"),
                    _model.ProtoOAAsset(assetId=2, name="USD"),
                ]
            )
        if isinstance(request, _oa.ProtoOASymbolByIdReq):
            return _oa.ProtoOASymbolByIdRes(
                symbol=[
                    _model.ProtoOASymbol(
                        symbolId=1,
                        digits=5,
                        pipPosition=4,
                        stepVolume=1000,
                        minVolume=1000,
                        maxVolume=10_000_000,
                        scheduleTimeZone="UTC",
                        measurementUnits="EUR",
                        schedule=[
                            _model.ProtoOAInterval(startSecond=0, endSecond=604799),
                        ],
                        holiday=self.holidays,
                    )
                ]
            )
        raise AssertionError(f"unexpected request: {type(request).__name__}")


class _SymbolInfoProvider(CTrader):
    """Run symbol metadata work on one supplied authenticated wire."""

    def __init__(self, wire: _SymbolInfoWire) -> None:
        super().__init__(
            symbol="pepperstoneuk:EURUSD",
            timeframe="1",
            config=CTraderConfig(
                demo=True,
                client_id="client",
                client_secret="secret",
                account_id="999",
            ),
        )
        self.symbol_info_wire = wire

    async def _authed_session(self, work):
        return await work(self.symbol_info_wire, 999)

    @staticmethod
    async def _wait_rate_limit_retry(seconds: float) -> None:
        return None


class _TestCTrader(CTrader):
    """Provider test seam with the rate-limit wait made instant."""

    @staticmethod
    async def _wait_rate_limit_retry(seconds: float) -> None:
        return None


def _calendar(*, open_all_week: bool) -> SymInfo:
    opening_hours = [
        SymInfoInterval(day=day, start=time(0, 0), end=time(23, 59, 59))
        for day in (range(7) if open_all_week else range(4))
    ]
    return SymInfo(
        prefix="CTRADER",
        description="EURUSD",
        ticker="EURUSD",
        currency="USD",
        basecurrency="EUR",
        period="1",
        type="forex",
        volumetype="tick",
        mintick=0.00001,
        pricescale=100000,
        pointvalue=1.0,
        mincontract=1000.0,
        opening_hours=opening_hours,
        session_starts=[],
        session_ends=[],
        timezone="UTC",
    )


def _provider(
        wire: _HistoryWire,
        timeframe: str = "1",
        *,
        open_all_week: bool = True,
) -> _TestCTrader:
    config = CTraderConfig(
        demo=True,
        client_id="client",
        client_secret="secret",
        account_id="999",
    )
    provider = _TestCTrader(
        symbol="broker:EURUSD",
        timeframe=timeframe,
        config=config,
    )
    provider._wire = wire  # type: ignore[assignment]
    provider._live_account_id = 999
    provider._symbols_by_name = {"EURUSD": 1}
    provider._live_subscription = ("EURUSD", timeframe)
    provider.syminfo = _calendar(open_all_week=open_all_week)
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


async def _watch(
    provider: _TestCTrader,
    timeframe: str = "1",
) -> OHLCV:
    provider._spot_events = asyncio.Queue()
    provider._subscribed_symbols.add("EURUSD")
    return await provider.watch_ohlcv("EURUSD", timeframe)


def _freeze(monkeypatch, moment: datetime) -> None:
    class _Now(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment

    monkeypatch.setattr("pynecore_ctrader.provider.datetime", _Now)


def __test_symbol_info_retries_blocked_payload_on_same_session__():
    """Startup metadata survives one recoverable payload throttle."""
    wire = _SymbolInfoWire()
    provider = _SymbolInfoProvider(wire)

    syminfo = provider.update_symbol_info()

    assert wire.symbol_list_requests == 2
    assert syminfo.ticker == "EURUSD"
    assert syminfo.currency == "USD"
    assert syminfo.basecurrency == "EUR"


def __test_symbol_info_persistent_payload_throttle_remains_bounded__():
    """A persistent throttle exhausts the bounded startup retry budget.

    Startup metadata reads run on the generous history profile (12 attempts)
    so a drained rolling quota is sat out instead of killing the run — but the
    bound must still exist: a venue that never unblocks propagates the error.
    """
    wire = _SymbolInfoWire(blocked_requests=20)
    provider = _SymbolInfoProvider(wire)

    try:
        provider.update_symbol_info()
    except CTraderProtocolError as exc:
        assert exc.error_code == "BLOCKED_PAYLOAD_TYPE"
    else:
        raise AssertionError("persistent rate limit did not propagate")

    assert wire.symbol_list_requests == 12


def __test_trendbar_history_retries_blocked_payload_on_same_session__():
    """Historical warmup retries a definitive payload throttle in place."""
    timestamp = 1_800_000_060
    wire = _HistoryWire(
        [_trendbar(timestamp, 114_000)],
        history_faults=[
            CTraderProtocolError(
                "BLOCKED_PAYLOAD_TYPE",
                "injected trendbar throttle",
                retry_after=5.0,
            )
        ],
    )
    provider = _provider(wire)

    bars, covered = asyncio.run(
        provider._fetch_trendbar_window(
            wire,
            999,
            1,
            "M1",
            timestamp * 1000,
            (timestamp + 60) * 1000,
        )
    )

    assert covered is True
    assert list(bars) == [timestamp * 1000]
    assert len(wire.requests) == 2


def __test_invalid_nonempty_schedule_timezone_fails_closed__():
    """Malformed venue timezone cannot become trusted UTC calendar evidence."""
    provider = _provider(_HistoryWire([]))
    schedule = [
        _model.ProtoOAInterval(
            startSecond=24 * 60 * 60 + 9 * 60 * 60,
            endSecond=24 * 60 * 60 + 17 * 60 * 60,
        )
    ]

    anchor = _closed(1_800_000_000)
    candidate = _closed(1_800_000_120)
    provider._last_live_closed_bar = anchor
    provider._pending_bars.append(candidate)

    try:
        provider._schedule_to_sessions(schedule, "Invalid/Timezone")
    except ValueError as error:
        assert "Invalid cTrader schedule timezone" in str(error)
    else:
        raise AssertionError("invalid venue timezone was converted into UTC evidence")

    assert provider._last_live_closed_bar is anchor
    assert list(provider._pending_bars) == [candidate]


def __test_empty_schedule_timezone_uses_utc__():
    """A missing venue timezone retains the documented UTC default."""
    provider = _provider(_HistoryWire([]))
    schedule = [
        _model.ProtoOAInterval(
            startSecond=24 * 60 * 60 + 9 * 60 * 60,
            endSecond=24 * 60 * 60 + 17 * 60 * 60,
        )
    ]

    opening_hours, _starts, _ends, _history = provider._schedule_to_sessions(
        schedule,
        "",
    )

    assert any(
        interval.day == 0
        and interval.start == time(9, 0)
        and interval.end == time(17, 0)
        for interval in opening_hours
    )


def _history_requests(wire: _HistoryWire) -> list[_oa.ProtoOAGetTrendbarsReq]:
    return [
        request
        for request in wire.requests
        if isinstance(request, _oa.ProtoOAGetTrendbarsReq)
    ]


def _stream_state(provider: _TestCTrader) -> tuple:
    """Everything a history query must leave as it was."""
    return (
        sorted(vars(provider)),
        list(provider._pending_bars),
        provider._last_live_closed_bar,
        provider._current_bar,
        provider._current_bar_ts,
        set(provider._subscribed_symbols),
        provider._live_subscription,
    )


def _backfill(provider: _TestCTrader, since_ms: int, timeframe: str = "1") -> list[OHLCV]:
    return asyncio.run(provider.backfill_closed_bars("EURUSD", timeframe, since_ms))


def _expect_raise(provider: _TestCTrader, since_ms: int, error: type[Exception]) -> Exception:
    try:
        _backfill(provider, since_ms)
    except error as exc:
        return exc
    raise AssertionError(f"history query did not raise {error.__name__}")


# --- class-level history declarations -----------------------------------------

def __test_provider_declares_its_history__():
    """The framework owns the recovery, waiting out the measured publication lag."""
    assert CTrader.history_has_idle_bars is False
    assert CTrader.history_publish_lag == 85.0


# --- backfill_closed_bars: a pure history query ---------------------------------

def __test_backfill_returns_closed_bars_strictly_after_since_ascending__(monkeypatch):
    """Unordered, duplicated, inclusive-edge history comes back clean and ascending."""
    since_ts = 1_800_000_000
    first_ts = since_ts + 60
    second_ts = since_ts + 120
    forming_ts = since_ts + 180
    wire = _HistoryWire([
        _trendbar(forming_ts, 114_030),
        _trendbar(second_ts, 114_020),
        _trendbar(since_ts, 114_000),
        _trendbar(first_ts, 114_010),
        _trendbar(first_ts, 114_011),
    ])
    provider = _provider(wire)
    _freeze(monkeypatch, datetime.fromtimestamp(forming_ts + 15, timezone.utc))

    bars = _backfill(provider, since_ts * 1000)

    assert [bar.timestamp for bar in bars] == [first_ts * 1000, second_ts * 1000]
    assert all(bar.is_closed for bar in bars)
    assert bars[0].low == 1.1401
    requests = _history_requests(wire)
    assert len(requests) == 1
    assert requests[0].fromTimestamp == since_ts * 1000 + 1
    assert requests[0].toTimestamp == (forming_ts + 15) * 1000


def __test_backfill_is_a_pure_query__(monkeypatch):
    """Repeated calls give the same answer and leave every stream field as it was."""
    since_ts = 1_800_000_000
    missed_ts = since_ts + 60
    forming_ts = since_ts + 120
    wire = _HistoryWire([_trendbar(missed_ts, 114_010), _trendbar(forming_ts, 114_020)])
    provider = _provider(wire)
    provider._last_live_closed_bar = _closed(since_ts)
    provider._current_bar = _closed(forming_ts)._replace(is_closed=False)
    provider._current_bar_ts = forming_ts * 1000
    provider._pending_bars.append(provider._current_bar)
    _freeze(monkeypatch, datetime.fromtimestamp(forming_ts + 30, timezone.utc))
    before = _stream_state(provider)

    first = _backfill(provider, since_ts * 1000)
    second = _backfill(provider, since_ts * 1000)

    assert [bar.timestamp for bar in first] == [missed_ts * 1000]
    assert first == second
    assert _stream_state(provider) == before
    assert len(_history_requests(wire)) == 2


def __test_backfill_leaves_a_young_missing_slot_out_without_waiting__(monkeypatch):
    """A just-closed bar the venue has not published yet is simply absent.

    One request, no polling: waiting out the publication lag is the framework's
    job, and it asks again later.
    """
    since_ts = 1_800_000_000
    first_ts = since_ts + 60
    unpublished_ts = since_ts + 120
    now = datetime.fromtimestamp(since_ts + 182, timezone.utc)
    wire = _HistoryWire([_trendbar(first_ts, 114_010)])
    provider = _provider(wire)
    _freeze(monkeypatch, now)

    bars = _backfill(provider, since_ts * 1000)

    assert [bar.timestamp for bar in bars] == [first_ts * 1000]
    assert unpublished_ts * 1000 not in {bar.timestamp for bar in bars}
    assert len(_history_requests(wire)) == 1


def __test_backfill_returns_a_tickless_hole_as_it_is__(monkeypatch):
    """A slot without a trendbar between two served bars is not classified here."""
    since_ts = 1_800_000_000
    before_hole = since_ts + 60
    after_hole = since_ts + 180
    wire = _HistoryWire([_trendbar(before_hole, 114_010), _trendbar(after_hole, 114_030)])
    provider = _provider(wire)
    _freeze(monkeypatch, datetime.fromtimestamp(since_ts + 300, timezone.utc))

    bars = _backfill(provider, since_ts * 1000)

    assert [bar.timestamp for bar in bars] == [before_hole * 1000, after_hole * 1000]
    assert len(_history_requests(wire)) == 1


def __test_backfill_raises_on_a_wire_error__(monkeypatch):
    """A failed request is no answer: it raises at once and changes nothing."""
    since_ts = 1_800_000_000
    for fault in (
            CTraderTimeoutError("history timed out"),
            CTraderProtocolError("SYMBOL_NOT_FOUND", "no such symbol"),
            CTraderConnectionError("not connected"),
    ):
        wire = _HistoryWire([_trendbar(since_ts + 60, 114_010)], history_faults=[fault])
        provider = _provider(wire)
        provider._last_live_closed_bar = _closed(since_ts)
        _freeze(monkeypatch, datetime.fromtimestamp(since_ts + 122, timezone.utc))
        before = _stream_state(provider)

        raised = _expect_raise(provider, since_ts * 1000, type(fault))

        assert raised is fault
        assert len(_history_requests(wire)) == 1
        assert _stream_state(provider) == before


def __test_backfill_raises_on_an_incomplete_answer__(monkeypatch):
    """A capped page whose rest cannot be read raises instead of a partial list."""
    since_ts = 1_800_000_000
    second_ts = since_ts + 120
    wire = _HistoryWire(
        [_trendbar(second_ts, 114_020)],
        history_has_more=[True, True],
    )
    provider = _provider(wire)
    _freeze(monkeypatch, datetime.fromtimestamp(since_ts + 182, timezone.utc))
    before = _stream_state(provider)

    raised = _expect_raise(provider, since_ts * 1000, CTraderWireError)

    assert not isinstance(raised, CTraderConnectionError)
    assert "capped" in str(raised)
    assert _stream_state(provider) == before


def __test_backfill_drains_capped_history_pages__(monkeypatch):
    """Every hasMore page is read, backwards from the oldest opening received."""
    since_ts = 1_800_000_000
    first_ts = since_ts + 60
    second_ts = since_ts + 120
    now = datetime.fromtimestamp(since_ts + 182, timezone.utc)
    wire = _HistoryWire(
        [],
        history_responses=[
            [_trendbar(second_ts, 114_020)],
            [_trendbar(first_ts, 114_010)],
        ],
        history_has_more=[True, False],
    )
    provider = _provider(wire)
    _freeze(monkeypatch, now)

    bars = _backfill(provider, since_ts * 1000)

    assert [bar.timestamp for bar in bars] == [first_ts * 1000, second_ts * 1000]
    requests = _history_requests(wire)
    assert len(requests) == 2
    assert requests[0].toTimestamp == (since_ts + 182) * 1000
    assert requests[1].fromTimestamp == since_ts * 1000 + 1
    assert requests[1].toTimestamp == second_ts * 1000


def __test_backfill_pages_a_long_gap_in_windows__(monkeypatch):
    """A gap longer than one 2000-bar window is read window by window."""
    since_ts = 1_800_000_000
    window_s = 60 * 2000
    early_ts = since_ts + 60
    late_ts = since_ts + 2 * window_s + 60
    now_ts = since_ts + 2 * window_s + 125
    wire = _HistoryWire([_trendbar(early_ts, 114_010), _trendbar(late_ts, 114_020)])
    provider = _provider(wire)
    _freeze(monkeypatch, datetime.fromtimestamp(now_ts, timezone.utc))

    bars = _backfill(provider, since_ts * 1000)

    assert [bar.timestamp for bar in bars] == [early_ts * 1000, late_ts * 1000]
    windows = [(r.fromTimestamp, r.toTimestamp) for r in _history_requests(wire)]
    assert windows == [
        (since_ts * 1000 + 1, since_ts * 1000 + 1 + window_s * 1000),
        (since_ts * 1000 + 1 + window_s * 1000, since_ts * 1000 + 1 + 2 * window_s * 1000),
        (since_ts * 1000 + 1 + 2 * window_s * 1000, now_ts * 1000),
    ]


def __test_backfill_raises_when_not_connected__():
    """Without a live connection the query cannot be answered."""
    provider = _provider(_HistoryWire([]))
    provider._wire = None

    _expect_raise(provider, 1_800_000_000_000, CTraderConnectionError)


def __test_daily_backfill_follows_the_venue_grid_phase__(monkeypatch):
    """Venue days open at 21:00 UTC, not on the epoch day boundary."""
    opens = [datetime(2027, 6, 30, 21, tzinfo=timezone.utc),
             datetime(2027, 7, 1, 21, tzinfo=timezone.utc),
             datetime(2027, 7, 2, 21, tzinfo=timezone.utc)]
    wire = _HistoryWire([_trendbar(int(moment.timestamp()), 114_000) for moment in opens[1:]])
    provider = _provider(wire, "1D")

    # Past the epoch midnight of 3 July but inside the day that opened at
    # 2 July 21:00 — the epoch grid would call that forming bar closed.
    _freeze(monkeypatch, datetime(2027, 7, 3, 10, tzinfo=timezone.utc))
    bars = _backfill(provider, int(opens[0].timestamp()) * 1000, "1D")

    assert [bar.timestamp for bar in bars] == [int(opens[1].timestamp()) * 1000]


def __test_weekly_backfill_follows_the_venue_grid_phase__(monkeypatch):
    """Venue weeks are anchored to a venue opening, not to epoch Thursdays."""
    opens = [datetime(2027, 6, 20, 21, tzinfo=timezone.utc),
             datetime(2027, 6, 27, 21, tzinfo=timezone.utc),
             datetime(2027, 7, 4, 21, tzinfo=timezone.utc)]
    wire = _HistoryWire([_trendbar(int(moment.timestamp()), 114_000) for moment in opens[1:]])
    provider = _provider(wire, "1W")

    # Past the epoch week boundary (Thursday 8 July) but inside the week that
    # opened on 4 July, which is therefore still forming.
    _freeze(monkeypatch, datetime(2027, 7, 9, 10, tzinfo=timezone.utc))
    bars = _backfill(provider, int(opens[0].timestamp()) * 1000, "1W")

    assert [bar.timestamp for bar in bars] == [int(opens[1].timestamp()) * 1000]


def __test_monthly_backfill_keeps_the_forming_month_out__(monkeypatch):
    """No fixed length locates a month; the newest opening is still running.

    The average-month grid puts its boundary at 2027-07-18 22:34:33, so from
    then on the still-running July bar looks closed to fixed-length maths;
    days 29-31 still classify by the actual next calendar opening.
    """
    opens = [datetime(2027, 5, 1, tzinfo=timezone.utc),
             datetime(2027, 6, 1, tzinfo=timezone.utc),
             datetime(2027, 7, 1, tzinfo=timezone.utc)]
    for current in (
            datetime(2027, 7, 25, 12, tzinfo=timezone.utc),
            datetime(2027, 7, 29, 12, tzinfo=timezone.utc),
            datetime(2027, 7, 30, 12, tzinfo=timezone.utc),
            datetime(2027, 7, 31, 12, tzinfo=timezone.utc),
    ):
        wire = _HistoryWire(
            [_trendbar(int(moment.timestamp()), 114_000) for moment in opens[1:]]
        )
        provider = _provider(wire, "1M")
        _freeze(monkeypatch, current)

        bars = _backfill(provider, int(opens[0].timestamp()) * 1000, "1M")

        assert [bar.timestamp for bar in bars] == [int(opens[1].timestamp()) * 1000]


def __test_monthly_backfill_closes_a_short_month_at_the_next_opening__(monkeypatch):
    """February is closed the moment March opens, though only 28 days long."""
    opens = [datetime(2027, 1, 1, tzinfo=timezone.utc),
             datetime(2027, 2, 1, tzinfo=timezone.utc),
             datetime(2027, 3, 1, tzinfo=timezone.utc)]
    wire = _HistoryWire([_trendbar(int(moment.timestamp()), 114_000) for moment in opens[1:]])
    provider = _provider(wire, "1M")
    _freeze(monkeypatch, datetime(2027, 3, 1, 0, 1, tzinfo=timezone.utc))

    bars = _backfill(provider, int(opens[0].timestamp()) * 1000, "1M")

    assert [bar.timestamp for bar in bars] == [int(opens[1].timestamp()) * 1000]


def __test_monthly_backfill_returns_a_sparse_answer_as_it_is__(monkeypatch):
    """A month the venue did not serve is absent, not an error."""
    opens = [
        datetime(2027, 1, 1, tzinfo=timezone.utc),
        datetime(2027, 2, 1, tzinfo=timezone.utc),
        datetime(2027, 3, 1, tzinfo=timezone.utc),
        datetime(2027, 4, 1, tzinfo=timezone.utc),
    ]
    wire = _HistoryWire([
        _trendbar(int(opens[1].timestamp()), 114_000),
        _trendbar(int(opens[3].timestamp()), 114_000),
    ])
    provider = _provider(wire, "1M")
    _freeze(monkeypatch, datetime(2027, 4, 15, 12, tzinfo=timezone.utc))

    bars = _backfill(provider, int(opens[0].timestamp()) * 1000, "1M")

    assert [bar.timestamp for bar in bars] == [int(opens[1].timestamp()) * 1000]


# --- the raw live stream ----------------------------------------------------------

def __test_reconnect_only_restores_the_subscription__():
    """The reconnect hook replays the subscription and reads no history."""
    wire = _HistoryWire([])
    provider = _provider(wire)
    provider._last_live_closed_bar = _closed(1_800_000_000)

    asyncio.run(provider.on_reconnect())

    assert [type(request) for request in wire.requests] == [
        _oa.ProtoOASubscribeSpotsReq,
        _oa.ProtoOASubscribeLiveTrendbarReq,
    ]
    assert not provider._pending_bars


def __test_stream_hands_on_a_skipped_slot_without_repair__():
    """A close after a slot the socket skipped goes on as it is, no history read."""
    anchor_ts = 1_800_000_000
    wire = _HistoryWire([_trendbar(anchor_ts + 60, 114_010)])
    provider = _provider(wire)
    provider._last_live_closed_bar = _closed(anchor_ts)
    candidate = _closed(anchor_ts + 120, 1.142)
    provider._pending_bars.append(candidate)

    delivered = asyncio.run(_watch(provider))

    assert delivered is candidate
    assert provider._last_live_closed_bar is candidate
    assert not _history_requests(wire)


def __test_stale_live_snapshot_and_its_quotes_are_dropped__():
    """A replayed snapshot of a handed-on close is not re-emitted, nor its quotes used."""
    closed_ts = 1_800_000_060
    next_ts = closed_ts + 60
    following_ts = next_ts + 60
    provider = _provider(_HistoryWire([]))
    provider._last_live_closed_bar = _closed(closed_ts, 1.141)

    async def scenario() -> tuple[OHLCV, OHLCV]:
        spot_events = asyncio.Queue()
        provider._spot_events = spot_events
        provider._subscribed_symbols.add("EURUSD")
        provider._watch_symbol_id = 1
        await spot_events.put(
            _oa.ProtoOASpotEvent(
                symbolId=1,
                bid=900_000,
                ask=910_000,
                trendbar=[_trendbar(closed_ts, 114_010)],
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
        return first_result, second_result

    forming_bar, closed_bar = asyncio.run(scenario())

    assert forming_bar.timestamp == next_ts * 1000
    assert forming_bar.is_closed is False
    assert closed_bar.timestamp == next_ts * 1000
    assert closed_bar.is_closed is True
    assert closed_bar.close == 1.2
    assert closed_bar.ask is not None
    assert closed_bar.ask.close == 1.21
    assert abs(closed_bar.ask.close - closed_bar.close - 0.01) < 1e-12
    assert provider._last_live_closed_bar is closed_bar
    assert all(bar.timestamp != closed_ts * 1000 for bar in provider._pending_bars)


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
    assert forming_bar.ask is not None
    assert forming_bar.ask.close == 1.21
    assert abs(forming_bar.ask.close - forming_bar.close - 0.01) < 1e-12
    assert provider._current_bar_ts == current_ts * 1000


def __test_closed_live_delivery_advances_the_stream_cursor__():
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


def _all_week_hours() -> list[SymInfoInterval]:
    """The 24/7 schedule rendered the way ``_schedule_to_sessions`` splits it."""
    return [
        SymInfoInterval(day=day, start=time(0, 0), end=time(23, 59, 59))
        for day in range(7)
    ]


def __test_symbol_holiday_becomes_session_correction__():
    """A one-off venue holiday cuts its window out of the touched local dates.

    Pepperstone published ``Break 01:00 - 07:00`` (Europe/Moscow) for day
    20722 = 2026-09-25: 22:00 UTC that Friday to 04:00 UTC Saturday. Orders
    in that window are rejected with ``SYMBOL_HAS_HOLIDAY`` and no bars
    arrive, so the live calendar must show both dates as partially closed.
    """
    provider = _provider(_HistoryWire([]))
    holiday = _model.ProtoOAHoliday(
        holidayId=4682,
        name="Break 01:00 - 07:00",
        scheduleTimeZone="Europe/Moscow",
        holidayDate=20722,
        isRecurring=False,
        startSecond=3600,
        endSecond=25200,
    )

    corrections = provider._holidays_to_corrections(
        [holiday], "America/New_York", _all_week_hours())

    assert corrections == {
        date(2026, 9, 25): (SymInfoInterval(day=4, start=time(0, 0), end=time(22, 0)),),
        date(2026, 9, 26): (SymInfoInterval(day=5, start=time(4, 0), end=time(23, 59, 59)),),
    }


def __test_full_day_holiday_closes_the_date__():
    """A whole-day holiday leaves an empty correction: no trading at all."""
    provider = _provider(_HistoryWire([]))
    holiday = _model.ProtoOAHoliday(
        holidayId=1,
        name="Christmas",
        scheduleTimeZone="UTC",
        holidayDate=(date(2025, 12, 25) - date(1970, 1, 1)).days,
        isRecurring=False,
        startSecond=0,
        endSecond=86400,
    )

    corrections = provider._holidays_to_corrections([holiday], "UTC", _all_week_hours())

    assert corrections == {date(2025, 12, 25): ()}


def __test_recurring_holiday_is_projected_onto_every_year__():
    """A recurring holiday repeats on its month/day across the schedule history."""
    provider = _provider(_HistoryWire([]))
    holiday = _model.ProtoOAHoliday(
        holidayId=2,
        name="New Year",
        scheduleTimeZone="UTC",
        holidayDate=(date(2020, 1, 1) - date(1970, 1, 1)).days,
        isRecurring=True,
        startSecond=0,
        endSecond=86400,
    )

    corrections = provider._holidays_to_corrections([holiday], "UTC", _all_week_hours())

    this_year = datetime.now(timezone.utc).year
    assert date(this_year, 1, 1) in corrections
    assert date(this_year + 1, 1, 1) in corrections
    assert date(this_year - _provider_module._SCHEDULE_HISTORY_YEARS, 1, 1) in corrections
    assert all(hours == () for hours in corrections.values())
    assert len(corrections) == _provider_module._SCHEDULE_HISTORY_YEARS + 2


def __test_holiday_without_timezone_uses_schedule_timezone__():
    """A holiday that names no zone is read in the symbol schedule zone."""
    provider = _provider(_HistoryWire([]))
    holiday = _model.ProtoOAHoliday(
        holidayId=3,
        name="Break",
        scheduleTimeZone="",
        holidayDate=(date(2026, 9, 25) - date(1970, 1, 1)).days,
        isRecurring=False,
        startSecond=3600,
        endSecond=25200,
    )

    corrections = provider._holidays_to_corrections(
        [holiday], "Europe/Moscow", _all_week_hours())

    # 01:00-07:00 Moscow on 2026-09-25 is 22:00 UTC the day before to 04:00 UTC.
    assert set(corrections) == {date(2026, 9, 24), date(2026, 9, 25)}

    try:
        provider._holidays_to_corrections([holiday], "Invalid/Timezone", _all_week_hours())
    except ValueError as error:
        assert "Invalid cTrader holiday timezone" in str(error)
    else:
        raise AssertionError("invalid holiday timezone was accepted")


def __test_symbol_info_carries_holiday_corrections__():
    """``update_symbol_info`` publishes the venue holidays on the SymInfo."""
    wire = _SymbolInfoWire(holidays=[
        _model.ProtoOAHoliday(
            holidayId=4,
            name="Break",
            scheduleTimeZone="UTC",
            holidayDate=(date(2026, 9, 25) - date(1970, 1, 1)).days,
            isRecurring=False,
            startSecond=0,
            endSecond=86400,
        )
    ])
    provider = _SymbolInfoProvider(wire)

    syminfo = provider.update_symbol_info()

    assert syminfo.session_corrections == {date(2026, 9, 25): ()}
