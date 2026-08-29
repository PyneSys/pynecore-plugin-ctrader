"""Data-provider mix-in for the cTrader Open API plugin.

Implements the :class:`~pynecore.core.plugin.ProviderPlugin` /
:class:`~pynecore.core.plugin.LiveProviderPlugin` data surface on top of
:class:`~pynecore_ctrader._base._CTraderBase`:

- timeframe conversion between TradingView strings and ``ProtoOATrendbarPeriod``,
- broker and symbol listing (``--list-brokers`` / ``--list-symbols``),
- symbol metadata (:meth:`update_symbol_info`) from ``ProtoOASymbol`` plus the
  asset list, with the weekly trading schedule mapped to PyneCore sessions,
- historical OHLCV via paged ``ProtoOAGetTrendbarsReq`` (bid) with the ask side
  reconstructed from paged ``ProtoOAGetTickDataReq`` (``ASK``), and
- live OHLCV from ``ProtoOASpotEvent`` trendbars.

All cTrader trendbar prices are integers in units of 1/100000; the low carries
the absolute price and open/high/close are non-negative deltas above it.
"""
import asyncio
import logging
import time as monotonic_time
from abc import ABC
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from typing import Callable, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pynecore.core.plugin import override, Broker
from pynecore.core.syminfo import (
    SymInfo, SymInfoInterval, SymInfoScheduleVariant, SymInfoSession,
)
from pynecore.lib.timeframe import in_seconds
from pynecore.types.ohlcv import OHLCV

from . import auth
from ._base import (
    _CTraderBase,
    _RATE_LIMIT_HISTORY_ATTEMPTS,
    _RATE_LIMIT_HISTORY_BUDGET_SECONDS,
)
from .config import CTraderConfig
from .exceptions import is_rate_limited
from .helpers import VOLUME_SCALE
from .messages.OpenApiMessages_pb2 import (
    ProtoOAAssetListReq,
    ProtoOAAssetListRes,
    ProtoOAGetTickDataReq,
    ProtoOAGetTickDataRes,
    ProtoOAGetTrendbarsReq,
    ProtoOAGetTrendbarsRes,
    ProtoOASpotEvent,
    ProtoOASubscribeLiveTrendbarReq,
    ProtoOASubscribeSpotsReq,
    ProtoOASymbolByIdReq,
    ProtoOASymbolByIdRes,
    ProtoOASymbolsListReq,
    ProtoOASymbolsListRes,
)
from .messages.OpenApiModelMessages_pb2 import (
    ProtoOAInterval,
    ProtoOALightSymbol,
    ProtoOAQuoteType,
    ProtoOASymbol,
    ProtoOATrendbar,
    ProtoOATrendbarPeriod,
)
from .wire import (
    CTraderConnectionError,
    CTraderProtocolError,
    CTraderWireError,
    WireClient,
)

logger = logging.getLogger(__name__)

#: cTrader prices are integers in units of 1/100000 of the quote currency.
_PRICE_SCALE = 100000.0

#: How far back the weekly schedule is re-rendered when building the
#: effective-dated session history (DST-correct backtest sessions).
_SCHEDULE_HISTORY_YEARS = 5

#: A just-closed cTrader trendbar can briefly be absent from history immediately
#: after reconnect. Keep the recovery bounded while allowing that read model to
#: settle before handing control back to the live stream.
_LIVE_HISTORY_SETTLE_ATTEMPTS = 5
_LIVE_HISTORY_SETTLE_DELAY_SECONDS = 1.0

#: How long a session-open slot must stay missing — across fully-served
#: collection passes — before it is accepted as venue-empty (tickless).
#: Far beyond any observed trendbar publication lag, yet short enough that a
#: reconnect over a tick-sparse stretch cannot starve the live feed for long.
_LIVE_HISTORY_HOLE_EVIDENCE_SECONDS = 120.0

#: TradingView timeframe -> ``ProtoOATrendbarPeriod`` enum name.
_TV_TO_PERIOD = {
    '1': 'M1', '2': 'M2', '3': 'M3', '4': 'M4', '5': 'M5', '10': 'M10',
    '15': 'M15', '30': 'M30', '60': 'H1', '240': 'H4', '720': 'H12',
    '1D': 'D1', '1W': 'W1', '1M': 'MN1',
}
_PERIOD_TO_TV = {period: tv for tv, period in _TV_TO_PERIOD.items()}


class SubscribeStatus(str, Enum):
    """Server outcome of one cTrader subscription request."""

    SUCCESS = "success"
    ALREADY_SUBSCRIBED = "already_subscribed"


@dataclass(frozen=True, slots=True)
class SubscribeOutcome:
    """Immutable outcome of the spot and live-trendbar subscription pair."""

    spots: SubscribeStatus
    trendbars: SubscribeStatus


@dataclass(frozen=True, slots=True)
class _LiveHistoryCollection:
    """Side-effect-free result of one bounded live-history collection."""

    bars: tuple[OHLCV, ...]
    failure: CTraderWireError | None
    response_received_monotonic_ns: int
    complete: bool


class _ProviderMixin(_CTraderBase, ABC):
    """Provider mix-in: timeframe maps, listings, symbol info and OHLCV."""

    _live_history_settle_attempts = _LIVE_HISTORY_SETTLE_ATTEMPTS
    _live_history_settle_delay_seconds = _LIVE_HISTORY_SETTLE_DELAY_SECONDS
    _live_history_hole_evidence_seconds = _LIVE_HISTORY_HOLE_EVIDENCE_SECONDS
    _live_history_bar_ids: set[int]
    _history_hole_first_missing_ns: dict[int, int]
    _live_generation_wire: WireClient
    _live_connection_generation: int
    _live_wire_identity: int
    _connected_gap_repair_occurrence: int

    # --- timeframe helpers --------------------------------------------------

    @classmethod
    @override
    def to_tradingview_timeframe(cls, timeframe: str) -> str:
        """Convert a ``ProtoOATrendbarPeriod`` name to TradingView format."""
        try:
            return _PERIOD_TO_TV[timeframe.upper()]
        except KeyError:
            raise ValueError(f"Invalid cTrader timeframe: {timeframe}")

    @classmethod
    @override
    def to_exchange_timeframe(cls, timeframe: str) -> str:
        """Convert a TradingView timeframe to a ``ProtoOATrendbarPeriod`` name."""
        try:
            return _TV_TO_PERIOD[timeframe]
        except KeyError:
            raise ValueError(
                f"Unsupported timeframe for cTrader: {timeframe}. "
                f"Supported: {', '.join(_TV_TO_PERIOD)}"
            )

    def _period_name(self) -> str:
        """Return the ``ProtoOATrendbarPeriod`` name for the current timeframe."""
        assert self.xchg_timeframe is not None
        return self.xchg_timeframe

    # --- broker listing -----------------------------------------------------

    @classmethod
    @override
    def get_list_of_brokers(cls) -> list[Broker]:
        """List the brokers the configured token grants accounts with.

        Unlike a static exchange list, cTrader's brokers come from the user's
        own account list, so this opens a short-lived authenticated socket. The
        config is loaded from the standard plugin path; this is the only
        provider method that reaches the CLI app state, and only on the
        ``--list-brokers`` path (never inside a security subprocess).

        :return: The distinct brokers (short ``brokerName`` slug as id, readable
            ``brokerTitleShort`` as name), sorted by id.
        """
        # Local import: keep the plugin import graph free of the CLI app module;
        # this classmethod only ever runs from the ``pyne data`` command.
        from pynecore.cli.app import app_state
        from pynecore.core.config import ensure_config

        config = ensure_config(
            CTraderConfig, app_state.config_dir / 'plugins' / 'ctrader.toml'
        )
        return cls(symbol=None, config=cast(CTraderConfig, config))._list_brokers()

    def _list_brokers(self) -> list[Broker]:
        """Enumerate the distinct brokers for the configured host kind.

        The id (``ProtoOATrader.brokerName`` slug, e.g. ``pepperstoneuk``) lives
        on the per-account trader record, so each account is authorized to read
        it; the readable name is the account list's ``brokerTitleShort``.
        """

        async def work(wire) -> list[Broker]:
            accounts = await self._get_accounts(wire)
            want_live = not self._demo
            titles: dict[str, str] = {}
            for a in accounts:
                if a.isLive != want_live:
                    continue
                await self._account_auth(wire, a.ctidTraderAccountId)
                slug = await self._broker_name(wire, a.ctidTraderAccountId)
                if slug:
                    titles.setdefault(slug, a.brokerTitleShort or "")
            return [Broker(id=slug, name=titles[slug]) for slug in sorted(titles)]

        return self._run(self._app_session(work))

    # --- symbol listing + resolution ----------------------------------------

    @override
    def get_list_of_symbols(self, *args, **kwargs) -> list[str]:
        """List the tradable symbol names of the selected broker's account.

        Only symbols flagged ``enabled`` are returned — the cTrader catalog is
        the whole broker universe, including symbols not tradable on this
        account (e.g. spread-bet ``_SB``/``_SBE`` variants on a CFD account, and
        broker test symbols), which the native platform also hides.
        """

        async def work(wire, account_id: int) -> list[str]:
            symbols = await self._fetch_light_symbols(wire, account_id)
            return sorted(s.symbolName for s in symbols if s.symbolName and s.enabled)

        return self._run(self._authed_session(work))

    async def _fetch_light_symbols(
            self, wire, account_id: int, *, recover: bool = False
    ) -> list[ProtoOALightSymbol]:
        """Fetch the account's light-symbol list and cache name -> id.

        :param recover: When ``True`` (the live order / state paths, where
            ``wire`` is the persistent connection) route the account-scoped
            request through :meth:`_account_request` so a mid-session de-auth is
            recovered instead of leaking a raw protocol error. The one-shot CLI
            paths leave it ``False`` (they run on a private, just-authed wire).
        """
        req = ProtoOASymbolsListReq(ctidTraderAccountId=account_id)
        response = await (
            self._account_request(req)
            if recover
            else self._retry_rate_limited(
                lambda: wire.send_request(req),
                context="light-symbol read",
                attempts=_RATE_LIMIT_HISTORY_ATTEMPTS,
                budget_seconds=_RATE_LIMIT_HISTORY_BUDGET_SECONDS,
            )
        )
        response = cast(ProtoOASymbolsListRes, response)
        symbols = list(response.symbol)
        self._symbols_by_name = {s.symbolName: s.symbolId for s in symbols}
        self._symbols_by_id = {s.symbolId: s.symbolName for s in symbols}
        return symbols

    # --- symbol info --------------------------------------------------------

    @override
    def get_symbol_info(self, force_update: bool = False) -> SymInfo:
        """Load or fetch symbol metadata and retain it for live gap decisions."""
        syminfo = super().get_symbol_info(force_update)
        self.syminfo = syminfo
        return syminfo

    @override
    def update_symbol_info(self) -> SymInfo:
        """Fetch full symbol metadata and map it to a :class:`SymInfo`."""
        assert self.symbol is not None

        async def work(wire, account_id: int) -> SymInfo:
            light = await self._fetch_light_symbols(wire, account_id)
            match = next(
                (s for s in light if s.symbolName == self.symbol), None
            )
            if match is None:
                raise auth.CTraderAuthError(
                    "SYMBOL_NOT_FOUND",
                    f"symbol '{self.symbol}' not on this account",
                )

            assets_request = ProtoOAAssetListReq(
                ctidTraderAccountId=account_id,
            )
            assets_res = cast(
                ProtoOAAssetListRes,
                await self._retry_rate_limited(
                    lambda: wire.send_request(assets_request),
                    context="asset metadata read",
                    attempts=_RATE_LIMIT_HISTORY_ATTEMPTS,
                    budget_seconds=_RATE_LIMIT_HISTORY_BUDGET_SECONDS,
                ),
            )
            asset_names = {a.assetId: a.name for a in assets_res.asset}

            detail_request = ProtoOASymbolByIdReq(
                ctidTraderAccountId=account_id,
                symbolId=[match.symbolId],
            )
            detail_res = cast(
                ProtoOASymbolByIdRes,
                await self._retry_rate_limited(
                    lambda: wire.send_request(detail_request),
                    context="symbol detail read",
                    attempts=_RATE_LIMIT_HISTORY_ATTEMPTS,
                    budget_seconds=_RATE_LIMIT_HISTORY_BUDGET_SECONDS,
                ),
            )
            if not detail_res.symbol:
                raise auth.CTraderAuthError(
                    "SYMBOL_NOT_FOUND",
                    f"no detail for symbol '{self.symbol}'",
                )
            return self._build_sym_info(
                match, detail_res.symbol[0], asset_names
            )

        syminfo = self._run(self._authed_session(work))
        self.syminfo = syminfo
        return syminfo

    def _build_sym_info(
            self,
            light: ProtoOALightSymbol,
            detail: ProtoOASymbol,
            asset_names: dict[int, str],
    ) -> SymInfo:
        """Assemble a :class:`SymInfo` from the light + full symbol records."""
        assert self.symbol is not None

        digits = detail.digits
        mintick = 10 ** -digits
        pricescale = 10 ** digits

        opening_hours, session_starts, session_ends, session_schedules = \
            self._schedule_to_sessions(list(detail.schedule), detail.scheduleTimeZone)

        # ``basecurrency`` must be a genuine currency: PyneCore treats the
        # ``(basecurrency, currency)`` tuple strictly as an FX pair for
        # exchange-rate lookups, so a non-currency value poisons that machinery.
        # cTrader only uses a currency as the base asset for real FX / crypto-spot
        # pairs; for indices, equities and spread-bet symbols the base asset is a
        # synthetic instrument asset (``GBPJPY_SB`` -> base ``GBPJPY``, ``NAS100``
        # -> base ``USTEC``). ``measurementUnits`` names the unit a position is
        # settled in: it equals the base asset name exactly when the base is the
        # traded currency, so anything else leaves ``basecurrency`` unset. A blank
        # ``measurementUnits`` (brokers that don't populate it) falls back to the
        # base asset so normal FX pairs keep their rate source.
        basecurrency = asset_names.get(light.baseAssetId) or None
        measurement = detail.measurementUnits
        if basecurrency is not None and measurement and measurement != basecurrency:
            basecurrency = None

        # ``stepVolume`` / ``minVolume`` are centi-units (helpers.VOLUME_SCALE):
        # the step is the order quantity grid (TV's mincontract), the minimum
        # is the fallback for brokers that omit the step. 0.0 lets the provider
        # chain fall back to volume analysis / heuristics.
        raw_step = detail.stepVolume or detail.minVolume
        mincontract = raw_step / VOLUME_SCALE if raw_step else 0.0

        return SymInfo(
            prefix='CTRADER',
            description=light.description or light.symbolName,
            ticker=light.symbolName,
            currency=asset_names.get(light.quoteAssetId, ''),
            basecurrency=basecurrency,
            period=self.timeframe or "1D",
            type='other',
            mintick=mintick,
            pricescale=pricescale,
            minmove=1,
            # cTrader trades only forex/CFD/spot instruments, for which the TV
            # point value is 1.0. ``detail.lotSize`` is the lot volume in
            # centi-units (order sizing), not a Pine point value.
            pointvalue=1.0,
            mincontract=mincontract,
            timezone=self.timezone,
            opening_hours=opening_hours,
            session_starts=session_starts,
            session_ends=session_ends,
            session_schedules=session_schedules,
        )

    def _schedule_to_sessions(
            self, schedule: list[ProtoOAInterval], schedule_tz: str
    ) -> tuple[list[SymInfoInterval], list[SymInfoSession], list[SymInfoSession],
    list[SymInfoScheduleVariant]]:
        """Map cTrader's weekly schedule to PyneCore sessions.

        Each :class:`ProtoOAInterval` is given in seconds from Sunday 00:00 in
        ``schedule_tz`` (start inclusive, end exclusive). An interval is anchored
        on a week's Sunday, shifted into ``self.timezone`` and split at local
        midnight so a session that straddles midnight matches candles on both
        local weekdays — the same shape the session checker expects.

        The rendered wall-clock times depend on the UTC-offset relationship of
        the two zones at that instant, so weeks on opposite sides of a DST
        transition render an hour apart. Anchoring everything on the current
        week would bake the current offsets into all of history; instead every
        week of the past :data:`_SCHEDULE_HISTORY_YEARS` years is rendered on
        its own Sunday anchor and each change opens a new effective-dated
        :class:`SymInfoScheduleVariant`. The flat lists are the current week's
        rendering (== the newest variant); when every week renders identically
        (both zones keep the same offset relationship year-round) the history
        is empty. Future transitions are deliberately not emitted: the flat
        fields feed live-session checks and must mirror "now", and the info is
        re-fetched on every data update anyway.

        :param schedule: The weekly trading intervals.
        :param schedule_tz: The IANA zone the intervals are expressed in. An empty
            value uses UTC; an invalid nonempty value is rejected.
        :return: ``(opening_hours, session_starts, session_ends,
            session_schedules)``.
        :raises ValueError: If the venue supplies an invalid nonempty timezone.
        """
        try:
            src = ZoneInfo(schedule_tz) if schedule_tz else ZoneInfo('UTC')
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(
                f"Invalid cTrader schedule timezone: {schedule_tz!r}"
            ) from error
        dst = ZoneInfo(self.timezone)

        def render_week(sunday_date: date) -> tuple[
            list[SymInfoInterval], list[SymInfoSession], list[SymInfoSession]]:
            sunday = datetime.combine(sunday_date, time(0, 0), tzinfo=src)
            opening_hours: list[SymInfoInterval] = []
            session_starts: list[SymInfoSession] = []
            session_ends: list[SymInfoSession] = []
            for interval in schedule:
                start_dt = (sunday + timedelta(seconds=interval.startSecond)).astimezone(dst)
                end_dt = (sunday + timedelta(seconds=interval.endSecond)).astimezone(dst)
                if end_dt <= start_dt:
                    continue
                session_starts.append(
                    SymInfoSession(day=start_dt.weekday(), time=start_dt.time()))
                session_ends.append(SymInfoSession(day=end_dt.weekday(), time=end_dt.time()))
                cursor = start_dt
                while cursor < end_dt:
                    day_end = datetime.combine(cursor.date(), time(23, 59, 59), tzinfo=dst)
                    seg_end = min(end_dt, day_end)
                    opening_hours.append(SymInfoInterval(
                        day=cursor.weekday(), start=cursor.time(), end=seg_end.time(),
                    ))
                    cursor = datetime.combine(
                        (cursor + timedelta(days=1)).date(), time(0, 0), tzinfo=dst
                    )
            return opening_hours, session_starts, session_ends

        now_src = datetime.now(src)
        this_sunday = (now_src - timedelta(days=(now_src.weekday() + 1) % 7)).date()

        variants: list[SymInfoScheduleVariant] = []
        prev: tuple | None = None
        for weeks_back in range(_SCHEDULE_HISTORY_YEARS * 52, -1, -1):
            week_sunday = this_sunday - timedelta(days=7 * weeks_back)
            rendered = render_week(week_sunday)
            if rendered != prev:
                anchor = datetime.combine(week_sunday, time(0, 0), tzinfo=src)
                variants.append(SymInfoScheduleVariant(
                    effective_from=anchor.astimezone(dst).date(),
                    opening_hours=rendered[0],
                    session_starts=rendered[1],
                    session_ends=rendered[2],
                ))
                prev = rendered

        newest = variants[-1]
        history = variants if len(variants) > 1 else []
        return newest.opening_hours, newest.session_starts, newest.session_ends, history

    # --- historical OHLCV ---------------------------------------------------

    @override
    def download_ohlcv(self, time_from: datetime, time_to: datetime,
                       on_progress: Callable[[datetime], None] | None = None,
                       limit: int | None = None, with_extra: bool = False) -> None:
        """Download historical OHLCV via paged ``ProtoOAGetTrendbarsReq``.

        When ``with_extra`` is set, the ask side is additionally reconstructed
        from paged ``ProtoOAGetTickDataReq`` (``ASK``) and written to the
        ``.extra.csv`` sidecar; this roughly multiplies the request count, so it
        is opt-in.
        """
        assert self.symbol is not None
        period_seconds = max(1, int(in_seconds(self.timeframe))) if self.timeframe else 60
        chunk = limit or 2000
        window = period_seconds * chunk

        # ``time_from`` / ``time_to`` arrive as naive UTC (framework contract).
        # Anchor them to UTC before converting to epoch — a naive datetime's
        # ``.timestamp()`` would otherwise be interpreted in the local timezone.
        from_dt = time_from.replace(tzinfo=timezone.utc) if time_from.tzinfo is None \
            else time_from.astimezone(timezone.utc)
        to_dt = time_to.replace(tzinfo=timezone.utc) if time_to.tzinfo is None \
            else time_to.astimezone(timezone.utc)

        async def work(wire, account_id: int) -> None:
            symbol_id = await self._resolve_symbol_id(wire, account_id)
            period = self._period_name()
            from_ms = int(from_dt.timestamp() * 1000)
            to_ms = int(to_dt.timestamp() * 1000)
            cursor = from_ms
            last_saved: int | None = None
            while cursor < to_ms:
                end_ms = min(cursor + window * 1000, to_ms)
                bars_by_open, covered = await self._fetch_trendbar_window(
                    wire, account_id, symbol_id, period, cursor, end_ms
                )
                if not bars_by_open:
                    cursor = end_ms
                    continue
                # The writer demands strictly increasing timestamps, so the page
                # is ordered here rather than trusting the response order.
                bar_opens = sorted(bars_by_open)
                resume_from = bar_opens[-1] + 1
                ask_end = end_ms
                if not covered and len(bar_opens) > 1:
                    # The venue capped the window, so nothing bounds the newest
                    # returned bar: its successor opening — the only thing that
                    # tells the tick bucketing where this bar ends — is still
                    # unread. Hand the bar back to the next window rather than
                    # closing it against data that was never fetched.
                    resume_from = bar_opens[-1]
                    ask_end = bar_opens.pop()
                bars = [bars_by_open[opening] for opening in bar_opens]
                # The trendbars are bid-based; the ask side has no trendbars and
                # is reconstructed (only when requested) by bucketing ``ASK`` tick
                # history into the same bars (open/high/low/close per period).
                ask_bars = await self._fetch_ask_bars(
                    wire, account_id, symbol_id, cursor, ask_end, bar_opens
                ) if with_extra else {}
                for bar in bars:
                    candle = self._decode_trendbar(bar)
                    if from_ms <= candle.timestamp < to_ms \
                            and (last_saved is None or candle.timestamp > last_saved):
                        self.save_ohlcv_data(self._attach_ask(candle, ask_bars.get(candle.timestamp)))
                        last_saved = candle.timestamp
                if on_progress is not None:
                    progress_ms = bar_opens[-1] + period_seconds * 1000
                    on_progress(datetime.fromtimestamp(progress_ms / 1000, tz=timezone.utc)
                                .replace(tzinfo=None))
                # Resume just past the newest bar that was fully read. Stepping
                # by ``open + period_seconds`` instead would overshoot the next
                # opening whenever the period is a calendar unit (the average
                # month is not a month), silently skipping it.
                cursor = max(resume_from, cursor + 1)

        self._run(self._authed_session(work))

    async def _fetch_trendbar_window(
            self, wire, account_id: int, symbol_id: int, period: str,
            from_ms: int, to_ms: int,
    ) -> tuple[dict[int, ProtoOATrendbar], bool]:
        """Read the trendbars the venue holds in ``[from_ms, to_ms]``.

        ``ProtoOAGetTrendbarsRes.hasMore`` marks a response the backend capped to
        its chunk size, and ``ProtoOAGetTrendbarsReq.count`` is documented as
        limiting the bars *back from* ``toTimestamp`` — so a capped response
        carries the newest part of the window and paging forward from it would
        drop the older part for good. The window is therefore drained backwards
        from the oldest opening received so far. A backend that instead caps to
        the oldest part answers the narrowed request with nothing new, which ends
        the drain and reports the window as not fully covered.

        :param from_ms: Window start in epoch milliseconds.
        :param to_ms: Window end in epoch milliseconds.
        :return: The trendbars keyed by their opening (epoch milliseconds), and
            whether they are known to cover the whole window.
        """
        bars: dict[int, ProtoOATrendbar] = {}
        upper = to_ms
        narrowed = False
        while upper > from_ms:
            request = ProtoOAGetTrendbarsReq(
                ctidTraderAccountId=account_id,
                symbolId=symbol_id,
                period=period,
                fromTimestamp=from_ms,
                toTimestamp=upper,
            )
            response = cast(
                ProtoOAGetTrendbarsRes,
                await self._retry_rate_limited(
                    lambda: wire.send_request(request),
                    context="trendbar history read",
                    attempts=_RATE_LIMIT_HISTORY_ATTEMPTS,
                    budget_seconds=_RATE_LIMIT_HISTORY_BUDGET_SECONDS,
                ),
            )
            oldest = upper
            added = False
            for bar in response.trendbar:
                opening = bar.utcTimestampInMinutes * 60_000
                oldest = min(oldest, opening)
                if opening not in bars:
                    bars[opening] = bar
                    added = True
            if narrowed and not added:
                # The narrowed request brought nothing new, so the backend caps
                # to the oldest rows: the newest part of the window stays unread.
                # This has to outrank the response's own ``hasMore``, which is
                # naturally clear here — the narrowed request was not truncated,
                # it simply had nothing left to truncate.
                return bars, False
            if not response.hasMore:
                return bars, True
            if not added or oldest <= from_ms:
                return bars, False
            upper = oldest
            narrowed = True
        return bars, False

    async def _fetch_ask_bars(
            self, wire, account_id: int, symbol_id: int, from_ms: int, to_ms: int,
            bar_opens: list[int],
    ) -> dict[int, tuple[float, float, float, float]]:
        """Aggregate ``ASK`` tick history into per-bar open/high/low/close.

        cTrader has no ask trendbars, so the ask side is rebuilt from raw tick
        data. ``ProtoOAGetTickDataReq`` returns ticks newest-first and capped per
        response (``hasMore`` flags truncation); each response is its own delta
        chain (first tick absolute, the rest cumulative deltas of both timestamp
        and price). The window is paged backwards by re-requesting up to one
        millisecond before the oldest tick seen so far.

        :param from_ms: Window start in epoch milliseconds (inclusive).
        :param to_ms: Window end in epoch milliseconds (exclusive upper bound).
            Must not reach past the end of the last bar in ``bar_opens``, since
            every tick above the last opening is bucketed into that bar.
        :param bar_opens: Ascending bar openings (epoch milliseconds) of this
            page. Ticks are assigned to the venue's own bar boundaries rather
            than to a fixed-width grid, which is the only assignment that holds
            for calendar periods (``W1`` / ``MN1``) whose openings do not sit on
            a Unix-epoch multiple of their average length.
        :return: Mapping of bar timestamp (epoch milliseconds) to ``(open, high,
            low, close)`` ask prices; empty when no tick history covers the window.
        """
        if not bar_opens:
            return {}
        # ``[min_ts, open, max_ts, close, high, low]`` per bar, updated tick by
        # tick so ticks may arrive in any order across pages.
        buckets: dict[int, list[float]] = {}
        upper = to_ms
        while upper > from_ms:
            request = ProtoOAGetTickDataReq(
                ctidTraderAccountId=account_id,
                symbolId=symbol_id,
                type=ProtoOAQuoteType.ASK,
                fromTimestamp=from_ms,
                toTimestamp=upper,
            )
            response = cast(
                ProtoOAGetTickDataRes,
                await self._retry_rate_limited(
                    lambda: wire.send_request(request),
                    context="ask-tick history read",
                    attempts=_RATE_LIMIT_HISTORY_ATTEMPTS,
                    budget_seconds=_RATE_LIMIT_HISTORY_BUDGET_SECONDS,
                ),
            )
            ticks = self._decode_ticks(response.tickData)
            if not ticks:
                break
            for ts_ms, price in ticks:
                if ts_ms >= to_ms:
                    # Past the last bar this page bounds; the venue's inclusive
                    # upper bound can still hand it over.
                    continue
                index = bisect_right(bar_opens, ts_ms) - 1
                if index < 0:
                    # Older than the first bar of this page: it belongs to a bar
                    # outside the window and has no bucket here.
                    continue
                key = bar_opens[index]
                bucket = buckets.get(key)
                if bucket is None:
                    buckets[key] = [ts_ms, price, ts_ms, price, price, price]
                    continue
                if price > bucket[4]:
                    bucket[4] = price
                if price < bucket[5]:
                    bucket[5] = price
                if ts_ms < bucket[0]:
                    bucket[0], bucket[1] = ts_ms, price
                if ts_ms > bucket[2]:
                    bucket[2], bucket[3] = ts_ms, price
            if not response.hasMore:
                break
            upper = int(ticks[-1][0]) - 1
        return {key: (b[1], b[4], b[5], b[3]) for key, b in buckets.items()}

    @staticmethod
    def _decode_ticks(ticks) -> list[tuple[int, float]]:
        """Decode a ``ProtoOAGetTickDataRes`` tick array to ``(ts_ms, price)``.

        The first tick is absolute and the rest are cumulative deltas, so a
        running sum yields absolute values; the array is newest-first, so the
        result keeps that order (index 0 newest, last oldest).
        """
        out: list[tuple[int, float]] = []
        ts = 0
        raw = 0
        for tick in ticks:
            ts += tick.timestamp
            raw += tick.tick
            out.append((ts, raw / _PRICE_SCALE))
        return out

    @staticmethod
    def _attach_ask(
            candle: OHLCV, ask: tuple[float, float, float, float] | None
    ) -> OHLCV:
        """Attach ask O/H/L/C and ``spread`` to a bid candle, if ask is known.

        ``spread`` is ``ask_close - close`` (close is the bid close), matching the
        live path. When no ask ticks covered the bar the candle is returned
        unchanged (bid-only), so a download never stalls on sparse tick history.
        """
        if ask is None:
            return candle
        ask_open, ask_high, ask_low, ask_close = ask
        return candle._replace(extra_fields={
            'ask_open': ask_open,
            'ask_high': ask_high,
            'ask_low': ask_low,
            'ask_close': ask_close,
            'spread': ask_close - candle.close,
        })

    async def _resolve_symbol_id(self, wire, account_id: int) -> int:
        """Resolve ``self.symbol`` to its numeric ``symbolId`` (cached)."""
        assert self.symbol is not None
        if self.symbol not in self._symbols_by_name:
            await self._fetch_light_symbols(wire, account_id)
        try:
            return self._symbols_by_name[self.symbol]
        except KeyError:
            raise auth.CTraderAuthError(
                "SYMBOL_NOT_FOUND", f"symbol '{self.symbol}' not on this account"
            )

    @staticmethod
    def _decode_trendbar(bar: ProtoOATrendbar, *, is_closed: bool = True) -> OHLCV:
        """Decode a ``ProtoOATrendbar`` into an :class:`OHLCV`.

        The low carries the absolute price; open/high/close are deltas above it.
        Prices are integers in units of 1/100000.

        :param bar: The trendbar to decode.
        :param is_closed: Whether the bar is final.
        :return: The decoded OHLCV record.
        """
        low = bar.low
        return OHLCV(
            timestamp=bar.utcTimestampInMinutes * 60_000,
            open=(low + bar.deltaOpen) / _PRICE_SCALE,
            high=(low + bar.deltaHigh) / _PRICE_SCALE,
            low=low / _PRICE_SCALE,
            close=(low + bar.deltaClose) / _PRICE_SCALE,
            volume=float(bar.volume),
            is_closed=is_closed,
        )

    # --- live OHLCV ---------------------------------------------------------

    async def _subscribe_live(self, symbol: str, timeframe: str) -> SubscribeOutcome:
        """Subscribe spot quotes + live trendbars for the watched symbol.

        Tolerant of a half-completed earlier attempt: ``watch_ohlcv`` runs
        under the framework's ``asyncio.wait_for`` budget, so a subscribe
        request can reach the server while the coroutine is cancelled
        awaiting the response — the server-side subscription then exists
        with no local record, and the blind replay here would otherwise be
        rejected with ``ALREADY_SUBSCRIBED`` (cTrader subscribes are not
        idempotent). That state is exactly what we want, so the error is
        treated as success (mirroring ``ALREADY_LOGGED_IN`` in
        ``_send_account_auth``). The returned immutable outcome preserves the
        distinction for diagnostics while existing callers may ignore it. On
        success the ``(symbol, timeframe)`` pair is recorded in
        ``_live_subscription`` so :meth:`on_reconnect` can replay it on a fresh
        connection.

        :param symbol: The cTrader symbol name.
        :param timeframe: Timeframe in TradingView format.
        :return: The distinct server outcome of both subscription requests.
        :raises CTraderConnectionError: If the live connection is not open.
        """
        wire = self._wire
        if wire is None or self._live_account_id is None:
            raise CTraderConnectionError("live connection not established")
        self._connection_generation_for_wire(wire)
        symbol_id = await self._resolve_symbol_id(wire, self._live_account_id)
        period = self.to_exchange_timeframe(timeframe)
        send_request = wire.send_request
        statuses: list[SubscribeStatus] = []
        for request in (
                ProtoOASubscribeSpotsReq(
                    ctidTraderAccountId=self._live_account_id, symbolId=[symbol_id],
                ),
                ProtoOASubscribeLiveTrendbarReq(
                    ctidTraderAccountId=self._live_account_id, period=period,
                    symbolId=symbol_id,
                ),
        ):
            try:
                await self._retry_rate_limited(
                    lambda: send_request(request),
                    context=type(request).__name__,
                )
            except CTraderProtocolError as exc:
                if exc.error_code != 'ALREADY_SUBSCRIBED':
                    raise
                statuses.append(SubscribeStatus.ALREADY_SUBSCRIBED)
            else:
                statuses.append(SubscribeStatus.SUCCESS)
        self._subscribed_symbols.add(symbol)
        self._watch_symbol_id = symbol_id
        self._live_subscription = (symbol, timeframe)
        return SubscribeOutcome(spots=statuses[0], trendbars=statuses[1])

    @override
    async def on_reconnect(self) -> None:
        """Replay the live subscription and backfill closed outage bars.

        Subscriptions are connection-scoped server state, and the lazy
        subscribe in :meth:`watch_ohlcv` runs under the framework's
        ``asyncio.wait_for`` budget — which pins at its 50 ms floor during
        the post-outage synth catch-up, far too short for the subscribe
        round-trips. The framework awaits this hook OUTSIDE any timeout
        right after a successful ``connect()``, so the replay gets a full
        request budget here and the first ``watch_ohlcv`` call finds
        ``_subscribed_symbols`` already populated. The subscription is restored
        before querying history so current pushes accumulate in the router queue
        while the historical request fills any fully closed slots missed during
        the outage. Historical bars are bid-only, matching the provider's normal
        historical download unless ask reconstruction is explicitly requested.
        """
        if self._live_subscription is not None:
            self._live_history_bar_ids = set()
            await self._subscribe_live(*self._live_subscription)
            await self._backfill_live_gap(*self._live_subscription)

    def _connection_generation_for_wire(self, wire: WireClient) -> int:
        """Return the stable streaming generation assigned to ``wire``."""
        if getattr(self, '_live_generation_wire', None) is not wire:
            self._live_generation_wire = wire
            self._live_connection_generation = (
                    getattr(self, '_live_connection_generation', 0) + 1
            )
            self._live_wire_identity = getattr(self, '_live_wire_identity', 0) + 1
        return self._live_connection_generation

    def _observe_connected_gap_repair(
            self,
            event: str,
            payload: dict[str, object],
    ) -> None:
        """Observe a connected-stream history repair without changing behavior.

        The production provider intentionally does nothing. Read-only laboratory
        subclasses may override this hook to persist credential-free evidence.

        :param event: ``started``, ``completed`` or ``failed``.
        :param payload: Primitive repair identity, timing and timestamp evidence.
        """

    @staticmethod
    async def _wait_provider_retry(seconds: float) -> None:
        """Wait for one bounded provider retry timer without polling or sleep."""
        loop = asyncio.get_running_loop()
        elapsed = loop.create_future()

        def complete(_unused: object) -> None:
            if not elapsed.done():
                elapsed.set_result(None)

        handle = loop.call_later(max(0.0, seconds), complete, None)
        try:
            await elapsed
        finally:
            handle.cancel()

    @staticmethod
    def _history_opening_hours_for_date(
            syminfo: SymInfo,
            local_date: date,
    ) -> tuple[list[SymInfoInterval], bool]:
        """Resolve one date plus the prior date's possible overnight sessions."""
        previous_date = local_date - timedelta(days=1)
        corrections = syminfo.session_corrections

        def resolve(target: date) -> tuple[list[SymInfoInterval], bool]:
            if target in corrections:
                return list(corrections[target]), True
            opening_hours, _starts, _ends = syminfo.schedule_for(target)
            return opening_hours, bool(opening_hours) or bool(syminfo.session_schedules)

        today_hours, today_known = resolve(local_date)
        previous_hours, previous_known = resolve(previous_date)
        weekday = local_date.weekday()
        previous_weekday = previous_date.weekday()
        relevant = [interval for interval in today_hours if interval.day == weekday]
        relevant.extend(
            interval
            for interval in previous_hours
            if interval.day == previous_weekday and interval.end < interval.start
        )
        return relevant, today_known and previous_known

    @staticmethod
    def _local_boundary_timestamps(
            local_date: date,
            local_time: time,
            zone: ZoneInfo,
    ) -> tuple[int, ...]:
        """Return all valid absolute timestamps for one local wall boundary."""
        timestamps: set[int] = set()
        for fold in (0, 1):
            local = datetime.combine(local_date, local_time, tzinfo=zone).replace(
                fold=fold
            )
            timestamp_ms = int(local.timestamp() * 1000)
            round_trip = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=zone)
            if (
                    round_trip.date() == local_date
                    and round_trip.time().replace(tzinfo=None) == local_time
                    and round_trip.fold == fold
            ):
                timestamps.add(timestamp_ms)
        return tuple(sorted(timestamps))

    @classmethod
    def _history_segment_overlaps_sessions(
            cls,
            segment_start_ms: int,
            segment_end_ms: int,
            local_date: date,
            opening_hours: list[SymInfoInterval],
            zone: ZoneInfo,
    ) -> bool | None:
        """Intersect one absolute segment with all relevant local sessions."""
        weekday = local_date.weekday()
        previous_date = local_date - timedelta(days=1)
        previous_weekday = previous_date.weekday()
        boundaries_known = True
        for interval in opening_hours:
            if interval.day == weekday:
                start_date = local_date
                end_date = (
                    local_date + timedelta(days=1)
                    if interval.end < interval.start
                    else local_date
                )
            elif interval.day == previous_weekday and interval.end < interval.start:
                start_date = previous_date
                end_date = local_date
            else:
                continue
            starts = cls._local_boundary_timestamps(
                start_date,
                interval.start,
                zone,
            )
            ends = cls._local_boundary_timestamps(
                end_date,
                interval.end,
                zone,
            )
            if not starts or not ends:
                boundaries_known = False
                continue
            if any(
                    end_ms > start_ms
                    and segment_end_ms > start_ms
                    and segment_start_ms < end_ms
                    for start_ms in starts
                    for end_ms in ends
            ):
                return True
        return False if boundaries_known else None

    def _history_interval_is_open(
            self,
            timestamp_ms: int,
            next_timestamp_ms: int,
    ) -> bool | None:
        """Classify a complete history interval across every local date it spans."""
        syminfo = self.syminfo
        if syminfo is None:
            return None
        try:
            zone = ZoneInfo(syminfo.timezone)
        except (ZoneInfoNotFoundError, ValueError):
            return None
        interval_start = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=zone)
        interval_end = datetime.fromtimestamp(next_timestamp_ms / 1000.0, tz=zone)
        calendar_known = True
        day = interval_start.date()
        while day <= interval_end.date():
            day_start = datetime.combine(day, time(0, 0), tzinfo=zone)
            next_day = datetime.combine(
                day + timedelta(days=1),
                time(0, 0),
                tzinfo=zone,
            )
            day_start_ms = int(day_start.timestamp() * 1000)
            next_day_ms = int(next_day.timestamp() * 1000)
            segment_start_ms = max(timestamp_ms, day_start_ms)
            segment_end_ms = min(next_timestamp_ms, next_day_ms)
            if segment_start_ms < segment_end_ms:
                opening_hours, day_known = self._history_opening_hours_for_date(
                    syminfo,
                    day,
                )
                overlap = self._history_segment_overlaps_sessions(
                    segment_start_ms,
                    segment_end_ms,
                    day,
                    opening_hours,
                    zone,
                )
                if overlap:
                    return True
                calendar_known = calendar_known and day_known and overlap is False
            day += timedelta(days=1)
        return False if calendar_known else None

    def _history_slot_is_open(self, timestamp_ms: int, timeframe: str) -> bool | None:
        """Classify one complete history slot with the effective-dated calendar.

        ``None`` means symbol metadata is unavailable, so callers must fail closed.
        Each local date in the slot is resolved through explicit corrections first,
        then the effective weekly schedule, before applying slot-overlap semantics.

        :param timestamp_ms: Bar opening timestamp in Unix milliseconds.
        :param timeframe: Timeframe in TradingView format.
        :return: ``True`` for open-session, ``False`` for closed-session, otherwise
            ``None`` when no calendar is available.
        """
        period_ms = max(1, int(in_seconds(timeframe))) * 1000
        return self._history_interval_is_open(
            timestamp_ms,
            timestamp_ms + period_ms,
        )

    def _next_month_opening(self, timestamp_ms: int) -> int | None:
        """Return the next monthly opening on the provider's calendar grid."""
        try:
            zone = ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError):
            return None
        local = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=zone)
        year = local.year + 1 if local.month == 12 else local.year
        month = 1 if local.month == 12 else local.month + 1
        next_open = datetime(
            year,
            month,
            1,
            local.hour,
            local.minute,
            local.second,
            local.microsecond,
            tzinfo=zone,
        )
        return int(next_open.timestamp() * 1000)

    def _history_month_is_open(
            self,
            timestamp_ms: int,
            next_timestamp_ms: int,
    ) -> bool | None:
        """Classify whether any venue session overlaps one calendar-month slot."""
        return self._history_interval_is_open(timestamp_ms, next_timestamp_ms)

    @staticmethod
    def _history_failure_is_transient(exc: CTraderWireError) -> bool:
        """Whether a failed history request can plausibly succeed on a retry.

        Timeouts and dropped-link faults are transient by nature. A server error
        response is only transient when the wire layer classifies its code as a
        connectivity / maintenance fault, or when it is a rate-limit rejection —
        every other code is a permanent rejection of this exact request.

        :param exc: The wire error the history request failed with.
        :return: ``True`` when retrying the same request is worthwhile.
        """
        if isinstance(exc, CTraderProtocolError):
            return exc.retryable or is_rate_limited(exc.error_code)
        return True

    async def _collect_live_gap_history(
            self,
            wire: WireClient,
            account_id: int,
            timeframe: str,
            *,
            cursor: int,
            query_ceiling: int,
    ) -> _LiveHistoryCollection:
        """Collect sorted closed trendbars inside an explicit live-gap window.

        This method never mutates the pending queue or the accepted closed-bar
        cursor. Inclusive venue edges are filtered locally and duplicate
        timestamps collapse before the result is returned.

        :param wire: Frozen wire used for every request in this collection.
        :param account_id: Frozen live account identity.
        :param timeframe: Timeframe in TradingView format.
        :param cursor: Opening of the first bar the window must recover. Every
            caller knows it directly — the bar after its accepted anchor, or,
            when nothing has been accepted yet, the bar that was forming when
            the stream died.
        :param query_ceiling: Exclusive upper opening bound in milliseconds.
        :return: Sorted closed bars plus terminal request evidence.
        :raises CTraderConnectionError: If the frozen wire disconnects.
        """
        exchange_period = self.to_exchange_timeframe(timeframe)
        calendar_period = exchange_period == 'MN1'
        period_ms = max(1, int(in_seconds(timeframe))) * 1000
        response_received_ns = monotonic_time.monotonic_ns()
        if cursor >= query_ceiling:
            return _LiveHistoryCollection((), None, response_received_ns, True)

        try:
            symbol_id = await self._resolve_symbol_id(wire, account_id)
        except CTraderConnectionError:
            raise
        except CTraderWireError as exc:
            return _LiveHistoryCollection(
                (), exc, monotonic_time.monotonic_ns(), False
            )

        recovered_by_timestamp: dict[int, OHLCV] = {}
        window_ms = period_ms * 2000
        newest_expected = None if calendar_period else query_ceiling - period_ms
        failure: CTraderWireError | None = None
        coverage_complete = False
        # Tick-sparse markets have session-open minutes with zero ticks, and
        # the venue never materialises a trendbar for those slots (measured
        # live: Pepperstone BTCUSD, Saturday). Such holes are indistinguishable
        # from an incomplete page by the calendar alone, so they are classified
        # from venue evidence instead: a slot that stays missing across
        # fully-served collection passes for longer than any plausible trendbar
        # publication lag is proven tickless. The first-seen ledger lives on
        # the provider so evidence keeps accumulating across the reconnect
        # attempts that each run one of these collections.
        venue_empty: set[int] = set()
        hole_first_missing_ns = getattr(
            self, '_history_hole_first_missing_ns', None)
        if hole_first_missing_ns is None:
            hole_first_missing_ns = {}
            self._history_hole_first_missing_ns = hole_first_missing_ns
        hole_evidence_ns = int(self._live_history_hole_evidence_seconds * 1e9)

        def unrecovered_slots() -> set[int]:
            """Expected fixed-grid slots that no pass has recovered yet."""
            return {
                slot_timestamp
                for slot_timestamp in range(cursor, query_ceiling, period_ms)
                if slot_timestamp not in recovered_by_timestamp
            }

        def settled() -> bool:
            """Whether every expected slot is recovered or calendar-closed."""
            if calendar_period:
                expected_timestamp = cursor
                while (
                        expected_timestamp is not None
                        and expected_timestamp < query_ceiling
                ):
                    following_timestamp = self._next_month_opening(
                        expected_timestamp
                    )
                    if following_timestamp is None:
                        return False
                    if expected_timestamp not in recovered_by_timestamp:
                        if query_ceiling < following_timestamp:
                            return False
                        if self._history_month_is_open(
                                expected_timestamp,
                                following_timestamp,
                        ) is not False:
                            return False
                    expected_timestamp = following_timestamp
                return expected_timestamp is not None
            assert newest_expected is not None
            for expected_timestamp in range(cursor, query_ceiling, period_ms):
                if expected_timestamp in recovered_by_timestamp:
                    continue
                if expected_timestamp in venue_empty:
                    continue
                if self._history_slot_is_open(expected_timestamp, timeframe) is not False:
                    return False
            return True

        for attempt in range(self._live_history_settle_attempts):
            query_cursor = cursor
            attempt_complete = True
            try:
                while query_cursor < query_ceiling:
                    window_end = min(query_cursor + window_ms, query_ceiling)
                    window_bars, window_complete = await self._fetch_trendbar_window(
                        wire,
                        account_id,
                        symbol_id,
                        exchange_period,
                        query_cursor,
                        window_end,
                    )
                    response_received_ns = monotonic_time.monotonic_ns()
                    attempt_complete = attempt_complete and window_complete
                    for timestamp, trendbar in window_bars.items():
                        if cursor <= timestamp < window_end:
                            recovered_by_timestamp[timestamp] = self._decode_trendbar(
                                trendbar
                            )
                    query_cursor = window_end
                failure = None
                coverage_complete = attempt_complete
            except CTraderConnectionError:
                raise
            except CTraderWireError as exc:
                response_received_ns = monotonic_time.monotonic_ns()
                failure = exc
                coverage_complete = False
                if not self._history_failure_is_transient(exc):
                    break
            if coverage_complete and not calendar_period:
                now_ns = monotonic_time.monotonic_ns()
                missing = unrecovered_slots()
                # A slot recovered by any pass (this collection or a later
                # window that moved the cursor past it) is no hole anymore.
                for slot_timestamp in list(hole_first_missing_ns):
                    if slot_timestamp not in missing:
                        del hole_first_missing_ns[slot_timestamp]
                for slot_timestamp in missing:
                    first_missing_ns = hole_first_missing_ns.setdefault(
                        slot_timestamp, now_ns)
                    if now_ns - first_missing_ns >= hole_evidence_ns:
                        venue_empty.add(slot_timestamp)
            if (
                    coverage_complete and settled()
            ) or attempt + 1 == self._live_history_settle_attempts:
                break
            await self._wait_provider_retry(
                self._live_history_settle_delay_seconds
            )

        complete = coverage_complete and settled()
        if calendar_period and recovered_by_timestamp:
            newest = max(recovered_by_timestamp)
            next_open = self._next_month_opening(newest)
            if next_open is None:
                complete = False
            elif query_ceiling < next_open:
                del recovered_by_timestamp[newest]

        return _LiveHistoryCollection(
            tuple(
                recovered_by_timestamp[timestamp]
                for timestamp in sorted(recovered_by_timestamp)
            ),
            failure if not complete else None,
            response_received_ns,
            complete,
        )

    async def _backfill_live_gap(self, symbol: str, timeframe: str) -> None:
        """Queue venue trendbars that closed since the last delivered live bar.

        The bar that is still forming is deliberately excluded because it will
        arrive through the restored live subscription. Responses are sorted and
        filtered at the local cursor, making inclusive venue bounds and replayed
        edge bars harmless.

        Bar openings are NOT Unix-epoch multiples of the period: cTrader
        aggregates on its own grid (measured on live data: daily bars open at
        21:00 UTC, i.e. three hours off the epoch day). The grid phase therefore
        comes from ``_last_live_closed_bar`` — an opening the venue itself
        produced — and every fixed-length period is a whole number of periods
        away from it. ``MN1`` has no fixed length at all, so for it closedness is
        read off the venue's own bar sequence instead: a returned month bar is
        closed once a newer opening exists (the same rule the live path uses in
        :meth:`_ingest_live_bar`), or once no month can still be forming.

        Before the stream has delivered its first closed bar there is no
        anchor at all, and returning here would silently drop the whole
        outage window: nothing else fetches those bars. ``_live_gap_cursor_ts``
        carries the opening the run is missing from in exactly that case —
        the bar that was forming when the link died, or the one after the
        startup-gap query's newest bar.

        :param symbol: The cTrader symbol name.
        :param timeframe: Timeframe in TradingView format.
        :raises CTraderConnectionError: If the live connection is not open.
        """
        anchor = self._last_live_closed_bar
        if anchor is None and self._live_gap_cursor_ts is None:
            return
        wire = self._wire
        account_id = self._live_account_id
        if wire is None or account_id is None:
            raise CTraderConnectionError('live connection not established')

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        exchange_period = self.to_exchange_timeframe(timeframe)
        calendar_period = exchange_period == 'MN1'
        period_ms = max(1, int(in_seconds(timeframe))) * 1000
        if anchor is None:
            cursor = self._live_gap_cursor_ts
        else:
            anchor_ts = int(anchor.timestamp)
            cursor = (
                self._next_month_opening(anchor_ts)
                if calendar_period
                else anchor_ts + period_ms
            )
        if cursor is None:
            return
        # The grid phase comes from the cursor itself — it is a venue-produced
        # opening, one whole period past the accepted anchor.
        if calendar_period:
            query_ceiling = now_ms
        else:
            query_ceiling = cursor + (now_ms - cursor) // period_ms * period_ms
        if cursor >= query_ceiling:
            return
        collection = await self._collect_live_gap_history(
            wire,
            account_id,
            timeframe,
            cursor=cursor,
            query_ceiling=query_ceiling,
        )
        if not collection.complete:
            # Preserve the accepted anchor and retry through a fresh connection;
            # committing any partial page would permanently skip unknown slots.
            logger.warning(
                'Reconnect backfill incomplete for %s %s (%d bar(s) recovered): %s',
                symbol,
                timeframe,
                len(collection.bars),
                collection.failure or 'history coverage incomplete',
            )
            raise CTraderConnectionError(
                'reconnect history coverage remained incomplete'
            )
        self._pending_bars.extend(collection.bars)
        history_ids = getattr(self, '_live_history_bar_ids', set())
        history_ids.update(id(bar) for bar in collection.bars)
        self._live_history_bar_ids = history_ids
        if collection.bars:
            logger.info(
                'Backfilled %d closed trendbar(s) after reconnect (%d..%d)',
                len(collection.bars),
                collection.bars[0].timestamp,
                collection.bars[-1].timestamp,
            )

    async def backfill_closed_bars(
            self, symbol: str, timeframe: str, since_ms: int,
    ) -> list[OHLCV]:
        """Fetch trendbars that closed between the warmup history and the stream.

        cTrader's handshake is the slowest of the supported venues (application
        auth, account auth, symbol resolution, subscription), so an M1 bar can
        easily close before the subscription is live. The window is collected
        with the same paged reader the reconnect path uses; a partial collection
        is discarded rather than committed, because committing one page would
        silently skip the slots the other pages would have carried.

        :param symbol: The cTrader symbol name (unused; the live subscription
            already fixes the instrument).
        :param timeframe: Timeframe in TradingView format.
        :param since_ms: Opening of the last bar the framework already holds.
        :return: Sorted closed bars strictly after ``since_ms``, possibly empty.
        """
        wire = self._wire
        account_id = self._live_account_id
        if wire is None or account_id is None:
            return []
        exchange_period = self.to_exchange_timeframe(timeframe)
        period_ms = max(1, int(in_seconds(timeframe))) * 1000
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if exchange_period == 'MN1':
            query_ceiling = now_ms
            cursor = self._next_month_opening(since_ms)
        else:
            query_ceiling = since_ms + (now_ms - since_ms) // period_ms * period_ms
            cursor = since_ms + period_ms
        if cursor is None:
            logger.warning('Startup gap collection skipped: invalid monthly timezone')
            return []
        collection = await self._collect_live_gap_history(
            wire,
            account_id,
            timeframe,
            cursor=cursor,
            query_ceiling=query_ceiling,
        )
        if not collection.complete:
            logger.warning(
                'Startup gap collection incomplete for %s (%d bar(s) seen): %s',
                timeframe,
                len(collection.bars),
                collection.failure or 'history coverage incomplete',
            )
            return []
        # The framework splices these bars in ahead of the first live one, so
        # after this call the run holds everything up to the newest of them.
        # A drop before the stream closes its own first bar has no accepted
        # anchor to resume from; this is what it falls back to.
        newest_held = collection.bars[-1].timestamp if collection.bars else since_ms
        self._live_gap_cursor_ts = (
            self._next_month_opening(newest_held)
            if exchange_period == 'MN1'
            else newest_held + period_ms
        )
        return list(collection.bars)

    async def _repair_connected_gap(
            self,
            symbol: str,
            timeframe: str,
            candidate: OHLCV,
    ) -> None:
        """Insert exact history bars before one frozen stream candidate."""
        anchor = self._last_live_closed_bar
        wire = self._wire
        account_id = self._live_account_id
        subscription = self._live_subscription
        if anchor is None or wire is None or account_id is None:
            return

        anchor_timestamp = int(anchor.timestamp)
        candidate_timestamp = int(candidate.timestamp)
        period_ms = max(1, int(in_seconds(timeframe))) * 1000
        if self.to_exchange_timeframe(timeframe) == 'MN1':
            requested_from_timestamp = self._next_month_opening(anchor_timestamp)
            if requested_from_timestamp is None:
                raise CTraderConnectionError(
                    'monthly connected gap timezone is invalid'
                )
        else:
            requested_from_timestamp = anchor_timestamp + period_ms
        occurrence = getattr(self, '_connected_gap_repair_occurrence', 0) + 1
        self._connected_gap_repair_occurrence = occurrence
        recovery_id = f'connected-gap-{occurrence}'
        generation = self._connection_generation_for_wire(wire)
        request_started_ns = monotonic_time.monotonic_ns()
        shared: dict[str, object] = {
            'recovery_id': recovery_id,
            'occurrence': occurrence,
            'wire_identity': self._live_wire_identity,
            'connection_generation': generation,
            'symbol': symbol,
            'timeframe': timeframe,
            'accepted_anchor_timestamp': anchor_timestamp,
            'candidate_timestamp': candidate_timestamp,
            'requested_from_timestamp': requested_from_timestamp,
            'requested_to_timestamp': candidate_timestamp,
            'query_ceiling_timestamp': candidate_timestamp,
            'request_started_monotonic_ns': request_started_ns,
        }
        self._observe_connected_gap_repair('started', shared)
        try:
            collection = await self._collect_live_gap_history(
                wire,
                account_id,
                timeframe,
                cursor=requested_from_timestamp,
                query_ceiling=candidate_timestamp,
            )
        except asyncio.CancelledError:
            self._observe_connected_gap_repair(
                'failed',
                {
                    **shared,
                    'response_received_monotonic_ns': monotonic_time.monotonic_ns(),
                    'recovered_timestamps': [],
                    'candidate_released': False,
                    'failure_type': 'CancelledError',
                },
            )
            raise
        except CTraderConnectionError:
            self._observe_connected_gap_repair(
                'failed',
                {
                    **shared,
                    'response_received_monotonic_ns': monotonic_time.monotonic_ns(),
                    'recovered_timestamps': [],
                    'candidate_released': False,
                    'failure_type': 'CTraderConnectionError',
                },
            )
            raise

        current_anchor = self._last_live_closed_bar
        current_anchor_timestamp = (
            int(current_anchor.timestamp) if current_anchor is not None else None
        )
        state_changed = (
                self._wire is not wire
                or self._live_subscription != subscription
                or current_anchor is not anchor
                or current_anchor_timestamp != anchor_timestamp
                or getattr(self, '_live_connection_generation', None) != generation
                or not self._pending_bars
                or self._pending_bars[0] is not candidate
        )
        if state_changed:
            self._observe_connected_gap_repair(
                'failed',
                {
                    **shared,
                    'response_received_monotonic_ns': (
                        collection.response_received_monotonic_ns
                    ),
                    'recovered_timestamps': [],
                    'candidate_released': False,
                    'failure_type': 'ConnectedGapStateChanged',
                },
            )
            raise CTraderConnectionError(
                'connected gap state changed during history repair'
            )

        recovered = tuple(
            bar
            for bar in collection.bars
            if anchor_timestamp < int(bar.timestamp) < candidate_timestamp
        )
        if not collection.complete:
            self._observe_connected_gap_repair(
                'failed',
                {
                    **shared,
                    'response_received_monotonic_ns': (
                        collection.response_received_monotonic_ns
                    ),
                    'recovered_timestamps': [],
                    'candidate_released': False,
                    'failure_type': (
                        type(collection.failure).__name__
                        if collection.failure is not None
                        else 'IncompleteHistory'
                    ),
                },
            )
            raise CTraderConnectionError(
                'connected gap history coverage remained incomplete'
            )
        self._observe_connected_gap_repair(
            'completed',
            {
                **shared,
                'response_received_monotonic_ns': (
                    collection.response_received_monotonic_ns
                ),
                'recovered_timestamps': [int(bar.timestamp) for bar in recovered],
                'candidate_released': True,
                'failure_type': None,
            },
        )
        for bar in reversed(recovered):
            self._pending_bars.appendleft(bar)
        history_ids = getattr(self, '_live_history_bar_ids', set())
        history_ids.update(id(bar) for bar in recovered)
        self._live_history_bar_ids = history_ids

    @override
    async def watch_ohlcv(self, symbol: str, timeframe: str) -> OHLCV:
        """Return the next live OHLCV update from the spot/trendbar feed.

        On first call for a symbol it subscribes to spot quotes and live
        trendbars for the requested period; subsequent calls drain the bars the
        background receive loop has queued. A bar arrives as ``is_closed=False``
        while it is still forming and is re-emitted with ``is_closed=True`` once a
        newer bar timestamp shows up.

        :param symbol: The cTrader symbol name.
        :param timeframe: Timeframe in TradingView format.
        :return: The next OHLCV update.
        :raises CTraderConnectionError: If the live connection is not open.
        """
        wire = self._wire
        if wire is None or self._live_account_id is None:
            raise CTraderConnectionError("live connection not established")

        if symbol not in self._subscribed_symbols:
            await self._subscribe_live(symbol, timeframe)

        spot_events = self._spot_events
        if spot_events is None:
            raise CTraderConnectionError("live event router not started")
        expected_period = ProtoOATrendbarPeriod.Value(
            self.to_exchange_timeframe(timeframe)
        )

        bar: OHLCV | None = None
        while bar is None:
            while not self._pending_bars:
                # The event router (see ``_CTraderBase._event_router_loop``) is the
                # sole consumer of ``wire.events`` and forwards spot events here, so
                # ``watch_ohlcv`` and ``watch_orders`` can stream concurrently
                # without racing on the shared queue.
                message = await spot_events.get()
                if not isinstance(message, ProtoOASpotEvent):
                    continue
                if message.symbolId != self._watch_symbol_id:
                    continue
                # Normalize the repeated trendbar field before mutating live state:
                # only the subscribed period participates, duplicate openings use
                # the last payload, and openings are processed chronologically.
                trendbars_by_timestamp: dict[int, ProtoOATrendbar] = {}
                for trendbar in message.trendbar:
                    if trendbar.period != expected_period:
                        continue
                    trendbars_by_timestamp[
                        trendbar.utcTimestampInMinutes * 60_000
                        ] = trendbar
                accepted_trendbar = False
                for timestamp in sorted(trendbars_by_timestamp):
                    accepted_trendbar = (
                            self._ingest_live_bar(trendbars_by_timestamp[timestamp])
                            or accepted_trendbar
                    )
                if len(message.trendbar) > 0 and not accepted_trendbar:
                    # Quotes carried by an all-stale or wrong-period snapshot do
                    # not belong to the current subscribed bar.
                    continue
                if message.bid:
                    self._last_bid = message.bid / _PRICE_SCALE
                if message.ask:
                    self._track_ask(message.ask / _PRICE_SCALE)
                if accepted_trendbar:
                    current_bar = self._current_bar
                    if current_bar is None:
                        raise CTraderConnectionError(
                            'accepted live trendbar did not establish current state'
                        )
                    self._pending_bars.append(
                        self._finalize_bar(current_bar, is_closed=False)
                    )

            candidate = self._pending_bars[0]
            history_ids = getattr(self, '_live_history_bar_ids', set())
            self._live_history_bar_ids = history_ids
            if (
                    candidate.is_closed
                    and id(candidate) not in history_ids
                    and self._last_live_closed_bar is not None
                    and int(candidate.timestamp) <= int(self._last_live_closed_bar.timestamp)
            ):
                stale = self._pending_bars.popleft()
                history_ids.discard(id(stale))
                continue
            if (
                    candidate.is_closed
                    and id(candidate) not in history_ids
                    and self._last_live_closed_bar is not None
            ):
                anchor_timestamp = int(self._last_live_closed_bar.timestamp)
                if self.to_exchange_timeframe(timeframe) == 'MN1':
                    expected_timestamp = self._next_month_opening(anchor_timestamp)
                    if expected_timestamp is None:
                        raise CTraderConnectionError(
                            'monthly live cursor timezone is invalid'
                        )
                else:
                    period_ms = max(1, int(in_seconds(timeframe))) * 1000
                    expected_timestamp = anchor_timestamp + period_ms
                if int(candidate.timestamp) > expected_timestamp:
                    await self._repair_connected_gap(symbol, timeframe, candidate)
                    history_ids = self._live_history_bar_ids

            delivered = self._pending_bars.popleft()
            history_ids.discard(id(delivered))
            self._live_history_bar_ids = history_ids
            if delivered.is_closed:
                self._last_live_closed_bar = delivered
            bar = delivered
        return bar

    def _ingest_live_bar(self, bar: ProtoOATrendbar) -> bool:
        """Fold a live trendbar into current state and the closed-bar buffer.

        When the bar's timestamp advances past the bar being tracked, the prior
        bar has closed: it is finalized (spot bid close, ask O/H/L/C) and queued,
        then the quote accumulators are reset for the new bar.

        :return: ``True`` when the trendbar advanced live state, otherwise ``False``.
        """
        candle = self._decode_trendbar(bar, is_closed=False)
        if (
                self._last_live_closed_bar is not None
                and candle.timestamp <= self._last_live_closed_bar.timestamp
        ):
            return False
        if self._current_bar_ts is not None and candle.timestamp < self._current_bar_ts:
            return False
        if self._live_gap_cursor_ts is None:
            # First opening this run ever saw on the stream: the warmup
            # history (plus the startup-gap query) covers everything before
            # it, so it is where a reconnect backfill must resume from until
            # a closed bar is actually delivered. Only ever set once —
            # a later opening would step over bars that never reached the
            # runner, and the accepted anchor takes over from the first
            # delivery onwards anyway.
            self._live_gap_cursor_ts = candle.timestamp
        if self._current_bar_ts is not None and candle.timestamp > self._current_bar_ts:
            if self._current_bar is not None:
                self._pending_bars.append(
                    self._finalize_bar(self._current_bar, is_closed=True)
                )
            self._reset_quotes()
        self._current_bar_ts = candle.timestamp
        self._current_bar = candle
        return True

    def _track_ask(self, ask: float) -> None:
        """Fold a spot ``ask`` quote into the current bar's ask O/H/L/C."""
        if self._ask_bar is None:
            self._ask_bar = (ask, ask, ask, ask)
        else:
            o, high, low, _ = self._ask_bar
            self._ask_bar = (o, max(high, ask), min(low, ask), ask)

    def _reset_quotes(self) -> None:
        """Clear the per-bar bid/ask accumulators at a bar boundary."""
        self._last_bid = None
        self._ask_bar = None

    def _finalize_bar(self, candle: OHLCV, *, is_closed: bool) -> OHLCV:
        """Apply the spot bid close and ask/spread to a trendbar-derived candle.

        The live trendbar's close lags the spot stream, so the authoritative
        close is the last spot ``bid`` of the bar; high/low are widened to keep
        the close inside the range. Ask O/H/L/C and ``spread`` (= ask_close -
        close) are attached from the spot ``ask`` stream when available.
        """
        high, low, close = candle.high, candle.low, candle.close
        if self._last_bid is not None:
            close = self._last_bid
            high = max(high, close)
            low = min(low, close)
        candle = candle._replace(high=high, low=low, close=close, is_closed=is_closed)
        if self._ask_bar is None:
            return candle
        ask_open, ask_high, ask_low, ask_close = self._ask_bar
        return candle._replace(extra_fields={
            'ask_open': ask_open,
            'ask_high': ask_high,
            'ask_low': ask_low,
            'ask_close': ask_close,
            'spread': ask_close - close,
        })
