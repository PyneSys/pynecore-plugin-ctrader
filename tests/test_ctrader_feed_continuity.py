"""
@pyne

Deterministic fault injection for the cTrader live feed: every closed bar of
an outage window must reach the runner, in order and exactly once.
"""
import asyncio
from datetime import datetime, time, timezone

from pynecore.core.syminfo import SymInfo, SymInfoInterval
from pynecore.types.ohlcv import OHLCV

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model
from pynecore_ctrader.wire import CTraderConnectionError, CTraderTimeoutError


# === Fixed grid ============================================================

#: One minute in milliseconds — every scenario runs on the ``1`` timeframe.
MIN = 60_000
#: Minute-aligned epoch anchor; the clock is frozen, so no wall-clock reads.
BASE = 1_800_000_000_000 // MIN * MIN


def _bar(k: int) -> int:
    """Opening timestamp of the k-th minute bar of the scenario grid."""
    return BASE + k * MIN


def _trendbar(timestamp_ms: int, price: int = 114_000) -> _model.ProtoOATrendbar:
    """One venue trendbar for the bar opening at ``timestamp_ms``."""
    return _model.ProtoOATrendbar(
        utcTimestampInMinutes=timestamp_ms // MIN,
        period="M1",
        low=price,
        deltaOpen=1,
        deltaHigh=3,
        deltaClose=2,
        volume=7,
    )


def _calendar() -> SymInfo:
    """Always-open calendar so no missing slot can excuse itself as closed."""
    return SymInfo(
        prefix="CTRADER", description="EURUSD", ticker="EURUSD", currency="USD",
        basecurrency="EUR", period="1", type="forex", volumetype="tick",
        mintick=0.00001, pricescale=100000, pointvalue=1.0, mincontract=1000.0,
        opening_hours=[
            SymInfoInterval(day=day, start=time(0, 0), end=time(23, 59, 59))
            for day in range(7)
        ],
        session_starts=[], session_ends=[], timezone="UTC",
    )


# === Fake venue ============================================================

class _FeedWire:
    """Wire fake serving trendbar history from a book, with fault switches.

    ``history_faults`` are raised (and consumed) one per history request;
    ``missing`` openings are withheld from every response, which is how a
    venue that has not yet published a slot looks.
    """

    def __init__(self, venue: '_Venue'):
        self.venue = venue
        self.events: asyncio.Queue = asyncio.Queue()
        self.is_connected = True
        self.history_requests: list[tuple[int, int]] = []
        self.subscribe_requests: list[str] = []

    async def connect(self) -> None:
        self.is_connected = True

    async def disconnect(self) -> None:
        self.is_connected = False

    async def send_request(self, request, **_kwargs):
        if isinstance(request, _oa.ProtoOAGetTrendbarsReq):
            self.history_requests.append(
                (request.fromTimestamp, request.toTimestamp)
            )
            if self.venue.history_faults:
                raise self.venue.history_faults.pop(0)
            bars = [
                _trendbar(ts) for ts in sorted(self.venue.book)
                if request.fromTimestamp <= ts <= request.toTimestamp
                and ts not in self.venue.missing
            ]
            return _oa.ProtoOAGetTrendbarsRes(trendbar=bars, hasMore=False)
        if isinstance(request, _oa.ProtoOASubscribeSpotsReq):
            self.subscribe_requests.append('spots')
            return _oa.ProtoOASubscribeSpotsRes()
        if isinstance(request, _oa.ProtoOASubscribeLiveTrendbarReq):
            self.subscribe_requests.append('trendbar')
            return _oa.ProtoOASubscribeLiveTrendbarRes()
        raise AssertionError(f"unexpected request: {type(request).__name__}")


class _Venue(CTrader):
    """cTrader provider with the transport, handshake and clock faked out.

    ``book`` is every opening the venue can serve from history; ``missing``
    withholds some of them; ``history_faults`` and ``connect_faults`` are
    consumed one per attempt.
    """

    def __init__(self):
        super().__init__(
            symbol="broker:EURUSD", timeframe="1",
            config=CTraderConfig(demo=True, client_id="c", client_secret="s",
                                 account_id="999"),
        )
        self.syminfo = _calendar()
        self._symbols_by_name = {"EURUSD": 1}
        self._live_subscription = ("EURUSD", "1")
        self._watch_symbol_id = 1
        self._live_history_settle_delay_seconds = 0
        #: Openings the venue holds in its trendbar history.
        self.book: set[int] = set()
        self.missing: set[int] = set()
        self.history_faults: list[Exception] = []
        self.connect_faults: list[Exception] = []
        self.wires: list[_FeedWire] = []
        #: Frozen wall clock in milliseconds, advanced explicitly.
        self.now_ms = _bar(0)

    # --- fake transport / handshake ---------------------------------------

    @property
    def wire(self) -> _FeedWire:
        """The wire the provider is currently talking to."""
        return self.wires[-1]

    def _make_wire(self):
        wire = _FeedWire(self)
        if self.connect_faults:
            fault = self.connect_faults.pop(0)

            async def _fail() -> None:
                raise fault

            wire.connect = _fail  # type: ignore[method-assign]
        self.wires.append(wire)
        return wire  # type: ignore[return-value]

    async def _full_handshake(self, wire) -> int:
        return 999

    async def _probe_account(self, account_id: int) -> None:
        return None

    async def _recover_in_flight_submissions(self) -> None:
        return None

    @staticmethod
    async def _wait_rate_limit_retry(seconds: float) -> None:
        return None

    def advance_to(self, k: int, *, offset_ms: int = 30_000) -> None:
        """Move the frozen clock into the k-th bar, ``offset_ms`` past open."""
        self.now_ms = _bar(k) + offset_ms

    def fill_book(self, upto: int) -> None:
        """Let the venue hold every scenario bar up to (excluding) ``upto``."""
        self.book = {_bar(k) for k in range(upto)}


# === Runner-side simulation =================================================

class _Runner:
    """Mimics the core live loop's consumption of the provider stream.

    ``dropped`` collects the closed bars the core monotonicity guard had to
    throw away — a non-empty list means the PLUGIN re-served settled history.
    """

    def __init__(self):
        self.closed: list[int] = []
        self.dropped: list[int] = []
        self.last_confirmed: int | None = None

    def feed(self, bar: OHLCV) -> None:
        if not bar.is_closed:
            return
        if self.last_confirmed is not None and bar.timestamp <= self.last_confirmed:
            self.dropped.append(bar.timestamp)
            return
        self.last_confirmed = bar.timestamp
        self.closed.append(bar.timestamp)


def _spot(*openings: int) -> _oa.ProtoOASpotEvent:
    """One spot event carrying the trendbars for ``openings``."""
    return _oa.ProtoOASpotEvent(
        ctidTraderAccountId=999, symbolId=1, bid=114_002, ask=114_004,
        trendbar=[_trendbar(ts) for ts in openings],
    )


async def _take(provider: _Venue, runner: _Runner, count: int) -> None:
    """Consume exactly ``count`` updates already queued on the provider."""
    for _ in range(count):
        runner.feed(await provider.watch_ohlcv("EURUSD", "1"))


async def _push(provider: _Venue, runner: _Runner, opening: int,
                *, expect: int) -> None:
    """Deliver one spot event and consume the ``expect`` updates it yields.

    A rollover yields two: the close of the previous bar and the opening
    snapshot of the new one. The first bar of a connection yields only the
    snapshot.
    """
    assert provider._spot_events is not None
    provider._spot_events.put_nowait(_spot(opening))
    await _take(provider, runner, expect)


async def _reconnect(provider: _Venue, runner: _Runner,
                     *, attempts: int = 8) -> int:
    """Replay the live runner's reconnect loop verbatim.

    ``disconnect`` -> ``connect`` -> ``on_reconnect``, retried on any
    exception; the runner never reads the update stream between a failed
    attempt and the next ``disconnect()``.

    :return: The number of attempts the reconnect took.
    """
    for attempt in range(1, attempts + 1):
        await provider.on_disconnect()
        try:
            await provider.disconnect()
        except Exception:  # noqa: BLE001 - the runner logs and continues too
            pass
        try:
            await provider.connect()
            await provider.on_reconnect()
        except Exception:  # noqa: BLE001 - "Reconnect failed", next attempt
            continue
        await _take(provider, runner, len(provider._pending_bars))
        return attempt
    raise AssertionError("reconnect never succeeded")


def _run(scenario, monkeypatch) -> None:
    """Run ``scenario(provider, runner)`` against the faked venue."""
    provider = _Venue()
    runner = _Runner()

    class _Now(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(provider.now_ms / 1000.0, tz=timezone.utc)

    monkeypatch.setattr("pynecore_ctrader.provider.datetime", _Now)

    async def _main():
        await scenario(provider, runner)
        await provider.disconnect()

    asyncio.run(_main())


async def _start_live(provider: _Venue, runner: _Runner) -> None:
    """Bring the feed up and let it close one bar, as a real session does."""
    provider.advance_to(1)
    await provider.connect()
    await _push(provider, runner, _bar(0), expect=1)
    provider.advance_to(2)
    await _push(provider, runner, _bar(1), expect=2)
    assert runner.closed == [_bar(0)]


# === (a) Outage bars arrive via backfill, seam stays clean ==================

def __test_ctrader_outage_bars_backfilled_in_order__(monkeypatch):
    """Bars that close during an outage are delivered once, in order."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)

        # The link dies inside bar 1; bars 1..3 close while offline.
        provider.advance_to(4)
        provider.fill_book(5)
        assert await _reconnect(provider, runner) == 1
        assert runner.closed == [_bar(k) for k in range(4)]

        # Live streaming resumes with no seam gap and no repeat.
        await _push(provider, runner, _bar(4), expect=1)
        provider.advance_to(5)
        await _push(provider, runner, _bar(5), expect=2)

        assert runner.closed == [_bar(k) for k in range(5)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


# === (b) Reconnect backfill re-serving pre-outage bars ======================

def __test_ctrader_backfill_overlap_not_re_emitted__(monkeypatch):
    """Bars the venue re-serves from before the outage never leave the plugin.

    The history window is filtered at the accepted cursor inside the plugin,
    so the core monotonicity guard never has to fire.
    """

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(4)
        # The book holds the whole session, including the bars already
        # delivered — the venue's inclusive edges re-serve them.
        provider.fill_book(5)
        await _reconnect(provider, runner)

        assert runner.closed == [_bar(k) for k in range(4)]
        assert runner.dropped == []
        # Nothing at or before the accepted anchor was even requested.
        assert all(start > _bar(0) for start, _end in provider.wire.history_requests)

    _run(scenario, monkeypatch)


def __test_ctrader_seam_bar_emitted_exactly_once__(monkeypatch):
    """The bar spanning the reconnect boundary is neither lost nor doubled.

    The restored subscription replays the same opening the backfill just
    recovered; the live path drops it against the accepted cursor.
    """

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(3)
        provider.fill_book(4)
        await _reconnect(provider, runner)
        assert runner.closed == [_bar(k) for k in range(3)]

        # Fresh subscription replays bar 2 (already backfilled) and then the
        # forming bar 3: only the latter may produce anything.
        await _push(provider, runner, _bar(2), expect=0)
        await _push(provider, runner, _bar(3), expect=1)

        assert runner.closed == [_bar(k) for k in range(3)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


# === (c) Repeated failed reconnect attempts =================================

def __test_ctrader_no_bar_lost_across_failed_reconnects__(monkeypatch):
    """Two dead handshakes and a dead history read later, the gap is intact."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(5)
        provider.fill_book(6)
        provider.connect_faults = [
            CTraderConnectionError("connection refused"),
            CTraderConnectionError("connection refused"),
        ]
        # Enough faults to exhaust the in-collection settle retries, so the
        # third reconnect attempt fails on the backfill itself.
        provider.history_faults = [
            CTraderTimeoutError("history read timed out")
            for _ in range(provider._live_history_settle_attempts)
        ]

        assert await _reconnect(provider, runner) == 4
        assert runner.closed == [_bar(k) for k in range(5)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


def __test_ctrader_partial_history_coverage_retries_whole_gap__(monkeypatch):
    """A venue that has not published every gap slot yet loses nothing.

    Committing the pages that did arrive would step the cursor over the
    still-missing slot, so the incomplete collection is discarded whole and
    the retried backfill re-reads the entire window.
    """

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(5)
        provider.fill_book(6)
        provider.missing = {_bar(3)}

        async def _publish_after_first_attempt() -> None:
            provider.missing = set()

        # First attempt sees a hole, the second sees the published bar.
        original = provider._collect_live_gap_history
        attempts: list[int] = []

        async def _counting(*args, **kwargs):
            attempts.append(1)
            result = await original(*args, **kwargs)
            if len(attempts) == 1:
                await _publish_after_first_attempt()
            return result

        monkeypatch.setattr(provider, '_collect_live_gap_history', _counting)

        assert await _reconnect(provider, runner) == 2
        assert runner.closed == [_bar(k) for k in range(5)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


# === (d) Drop exactly on a bar boundary =====================================

def __test_ctrader_drop_at_bar_boundary_delivers_once__(monkeypatch):
    """A bar closing at the very moment of the drop arrives exactly once.

    Two variants of the same instant: the rollover trendbar arrives just
    before the link dies (so the bar is delivered live and the backfill must
    not repeat it), and it does not (so the backfill must supply it).
    """

    async def rollover_arrived(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(2, offset_ms=0)
        # Bar 1 closes because bar 2's opening lands, then the link dies.
        await _push(provider, runner, _bar(2), expect=2)
        assert runner.closed == [_bar(0), _bar(1)]
        provider.advance_to(3)
        provider.fill_book(4)
        await _reconnect(provider, runner)

        assert runner.closed == [_bar(0), _bar(1), _bar(2)]
        assert runner.dropped == []

    async def rollover_lost(provider, runner):
        await _start_live(provider, runner)
        provider.advance_to(2, offset_ms=0)
        # Same instant, but the rollover trendbar died with the link.
        provider.advance_to(3)
        provider.fill_book(4)
        await _reconnect(provider, runner)

        assert runner.closed == [_bar(0), _bar(1), _bar(2)]
        assert runner.dropped == []

    _run(rollover_arrived, monkeypatch)
    _run(rollover_lost, monkeypatch)


# === Drop before the stream's first close ===================================

def __test_ctrader_gap_after_startup_backfill_only__(monkeypatch):
    """A drop before the first live close still backfills the gap.

    The startup-gap query hands the runner everything up to its newest bar;
    that — not "nothing" — is where the reconnect backfill resumes from.
    """

    async def scenario(provider, runner):
        provider.advance_to(2)
        await provider.connect()
        provider.fill_book(2)
        # Framework startup gap: the warmup history ended at bar 0.
        recovered = await provider.backfill_closed_bars("EURUSD", "1", _bar(0))
        assert [bar.timestamp for bar in recovered] == [_bar(1)]
        runner.feed(OHLCV(timestamp=_bar(0), open=1.14, high=1.14, low=1.14,
                          close=1.14, volume=1.0, is_closed=True))
        for bar in recovered:
            runner.feed(bar)

        # The link dies without the stream ever delivering a bar.
        provider.advance_to(5)
        provider.fill_book(6)
        await _reconnect(provider, runner)

        assert runner.closed == [_bar(k) for k in range(5)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


def __test_ctrader_gap_with_only_a_forming_bar_seen__(monkeypatch):
    """A drop after only a forming bar still backfills the gap.

    Nothing has closed on the stream, so there is no accepted anchor; the
    bar seen forming is the first one that can close while offline, and
    everything before it came from the warmup history.
    """

    async def scenario(provider, runner):
        provider.advance_to(1)
        await provider.connect()
        runner.feed(OHLCV(timestamp=_bar(0), open=1.14, high=1.14, low=1.14,
                          close=1.14, volume=1.0, is_closed=True))
        await _push(provider, runner, _bar(1), expect=1)
        assert runner.closed == [_bar(0)]

        provider.advance_to(4)
        provider.fill_book(5)
        await _reconnect(provider, runner)

        assert runner.closed == [_bar(k) for k in range(4)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


# === Sanity: no reconnect, no history reads =================================

def __test_ctrader_clean_stream_never_backfills__(monkeypatch):
    """Without a drop the subscription serves everything — no history reads."""

    async def scenario(provider, runner):
        await _start_live(provider, runner)
        for k in (2, 3, 4):
            provider.advance_to(k + 1)
            await _push(provider, runner, _bar(k), expect=2)

        assert runner.closed == [_bar(k) for k in range(4)]
        assert runner.dropped == []
        assert provider.wire.history_requests == []

    _run(scenario, monkeypatch)
