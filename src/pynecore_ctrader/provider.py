"""Data-provider mix-in for the cTrader Open API plugin.

Implements the :class:`~pynecore.core.plugin.ProviderPlugin` /
:class:`~pynecore.core.plugin.LiveProviderPlugin` data surface on top of
:class:`~pynecore_ctrader._base._CTraderBase`:

- timeframe conversion between TradingView strings and ``ProtoOATrendbarPeriod``,
- broker and symbol listing (``--list-brokers`` / ``--list-symbols``),
- symbol metadata (:meth:`update_symbol_info`) from ``ProtoOASymbol`` plus the
  asset list, with the weekly trading schedule mapped to PyneCore sessions and
  the symbol holidays to date-specific session corrections,
- historical OHLCV via paged ``ProtoOAGetTrendbarsReq`` (bid) with the ask side
  reconstructed from paged ``ProtoOAGetTickDataReq`` (``ASK``), and
- live OHLCV from ``ProtoOASpotEvent`` trendbars.

All cTrader trendbar prices are integers in units of 1/100000; the low carries
the absolute price and open/high/close are non-negative deltas above it.
"""
import logging
from abc import ABC
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from typing import Callable, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pynecore.core.plugin import override, Broker, ExtraData
from pynecore.core.syminfo import (
    SymInfo, SymInfoInterval, SymInfoScheduleVariant, SymInfoSession,
)
from pynecore.lib.timeframe import in_seconds
from pynecore.types.ohlcv import OHLCV, AskOHLC

from . import auth
from ._base import (
    _CTraderBase,
    _RATE_LIMIT_HISTORY_ATTEMPTS,
    _RATE_LIMIT_HISTORY_BUDGET_SECONDS,
)
from .config import CTraderConfig
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
    ProtoOAHoliday,
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
)

logger = logging.getLogger(__name__)

#: cTrader prices are integers in units of 1/100000 of the quote currency.
_PRICE_SCALE = 100000.0

#: How far back the weekly schedule is re-rendered when building the
#: effective-dated session history (DST-correct backtest sessions).
_SCHEDULE_HISTORY_YEARS = 5

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


class _ProviderMixin(_CTraderBase, ABC):
    """Provider mix-in: timeframe maps, listings, symbol info and OHLCV."""

    extra_data = ExtraData(
        label="Ask side (bid/ask)",
        help="Rebuilds the ask side of every bar from tick history. The venue has no "
             "ask bars, so this takes several extra requests per trading day (about "
             "seven for EURUSD): a long history downloads much slower than the bid "
             "bars alone.",
    )

    # Measured live: Pepperstone BTCUSD M1 served a just-closed trendbar in the
    # history 50-73 s after its close.
    history_publish_lag = 90.0
    # Measured live: a session-open minute without a tick (Pepperstone BTCUSD,
    # Saturday) never gets a trendbar in the history.
    history_has_idle_bars = False

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
        session_corrections = self._holidays_to_corrections(
            list(detail.holiday), detail.scheduleTimeZone, opening_hours)

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
            session_corrections=session_corrections,
        )

    def _holidays_to_corrections(
            self,
            holidays: list[ProtoOAHoliday],
            schedule_tz: str,
            opening_hours: list[SymInfoInterval],
    ) -> dict[date, tuple[SymInfoInterval, ...]]:
        """Map cTrader's symbol holidays to PyneCore session corrections.

        A :class:`ProtoOAHoliday` closes the symbol for ``[startSecond,
        endSecond)`` counted from midnight of ``holidayDate`` (days since the
        epoch) in the holiday's own ``scheduleTimeZone``; a recurring holiday
        repeats on the same month and day every year. The venue rejects orders
        inside that window (``SYMBOL_HAS_HOLIDAY``) and sends no bars, so the
        window must be part of the session calendar: each exchange-local date
        the window touches gets a correction holding that date's weekly
        intervals with the closed window cut out (an empty tuple when nothing
        of the day remains). Recurring holidays are projected onto the
        :data:`_SCHEDULE_HISTORY_YEARS` history plus the current and next
        year, matching the schedule history depth.

        :param holidays: The symbol's holiday list.
        :param schedule_tz: The symbol schedule timezone, the fallback for a
            holiday that carries no timezone of its own.
        :param opening_hours: The current week's rendered intervals in
            ``self.timezone`` (single-day segments, as :meth:`_schedule_to_sessions`
            produces them).
        :return: Corrections keyed by exchange-local date.
        :raises ValueError: If the venue supplies an invalid nonempty timezone.
        """
        dst = ZoneInfo(self.timezone)
        corrections: dict[date, tuple[SymInfoInterval, ...]] = {}
        if not holidays:
            return corrections
        current_year = datetime.now(dst).year
        years = range(current_year - _SCHEDULE_HISTORY_YEARS, current_year + 2)

        def cut(day: date, closed_from: datetime, closed_to: datetime) -> None:
            base = corrections.get(day)
            if base is None:
                base = tuple(iv for iv in opening_hours if iv.day == day.weekday())
            kept: list[SymInfoInterval] = []
            for iv in base:
                start = datetime.combine(day, iv.start, tzinfo=dst)
                end = datetime.combine(day, iv.end, tzinfo=dst)
                if end <= closed_from or start >= closed_to:
                    kept.append(iv)
                    continue
                if start < closed_from:
                    kept.append(SymInfoInterval(
                        day=iv.day, start=iv.start, end=closed_from.time()))
                if end > closed_to:
                    kept.append(SymInfoInterval(
                        day=iv.day, start=closed_to.time(), end=iv.end))
            corrections[day] = tuple(kept)

        for holiday in holidays:
            tz_name = holiday.scheduleTimeZone or schedule_tz
            try:
                src = ZoneInfo(tz_name) if tz_name else ZoneInfo('UTC')
            except (ZoneInfoNotFoundError, ValueError) as error:
                raise ValueError(
                    f"Invalid cTrader holiday timezone: {tz_name!r}"
                ) from error
            first = date(1970, 1, 1) + timedelta(days=holiday.holidayDate)
            dates = [first]
            if holiday.isRecurring:
                dates = []
                for year in years:
                    try:
                        dates.append(first.replace(year=year))
                    except ValueError:  # Feb 29 in a non-leap year
                        continue
            for holiday_date in dates:
                midnight = datetime.combine(holiday_date, time(0, 0), tzinfo=src)
                closed_from = (midnight + timedelta(seconds=holiday.startSecond)).astimezone(dst)
                closed_to = (midnight + timedelta(seconds=holiday.endSecond)).astimezone(dst)
                if closed_to <= closed_from:
                    continue
                day = closed_from.date()
                last = (closed_to - timedelta(microseconds=1)).date()
                while day <= last:
                    cut(day, closed_from, closed_to)
                    day += timedelta(days=1)
        return corrections

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
        from paged ``ProtoOAGetTickDataReq`` (``ASK``) and stored with each bar.
        It is opt-in because the venue has no ask trendbars: the cost follows the
        length of the history, about seven tick requests per trading day of
        EURUSD, against one trendbar request per two thousand bars.
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
                # The venue serves the bar that CONTAINS ``fromTimestamp``, so a
                # window resumed one millisecond past the newest bar read gets
                # that same bar back. While the market is open it comes with
                # newer bars; on a closed market (holiday, weekend) it is the
                # only bar in the answer, and taking it as progress would resume
                # one millisecond further and re-read it forever, one request
                # per iteration against the venue's request pacing. A bar older
                # than the cursor is therefore never progress.
                bars_by_open = {
                    opening: bar for opening, bar in bars_by_open.items()
                    if opening >= cursor
                }
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
        """Attach the ask O/H/L/C to a bid candle, if ask is known.

        When no ask ticks covered the bar the candle is returned unchanged
        (bid-only), so a download never stalls on sparse tick history.
        """
        if ask is None:
            return candle
        return candle._replace(ask=AskOHLC(*ask))

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
        """Replay the live subscription on the fresh connection.

        Subscriptions are connection-scoped server state, and the lazy
        subscribe in :meth:`watch_ohlcv` runs under the framework's
        ``asyncio.wait_for`` budget — which pins at its 50 ms floor during
        the post-outage synth catch-up, far too short for the subscribe
        round-trips. The framework awaits this hook OUTSIDE any timeout
        right after a successful ``connect()``, so the replay gets a full
        request budget here and the first ``watch_ohlcv`` call finds
        ``_subscribed_symbols`` already populated. The framework then fills the
        bars that closed during the outage through :meth:`backfill_closed_bars`
        while the pushes of the restored subscription queue up behind it.
        """
        if self._live_subscription is not None:
            await self._subscribe_live(*self._live_subscription)

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

    async def backfill_closed_bars(
            self, symbol: str, timeframe: str, since_ms: int,
    ) -> list[OHLCV]:
        """Return the closed trendbars the venue's history holds after ``since_ms``.

        A pure query on the live connection: one collection of the history from
        ``since_ms`` up to now, paged in windows of 2000 bars, with no state
        read or written beyond the symbol id cache. It neither waits for a bar
        the venue has not published yet nor decides whether a missing slot is a
        hole: what the venue returned is returned, and an absent slot is simply
        absent. The framework owns the publication lag
        (:attr:`~_ProviderMixin.history_publish_lag`) and the hole decision.

        The bar the venue serves for an inclusive lower bound, the still-forming
        bar and duplicate openings are left out. A fixed-length period is closed
        once its full length has passed since its opening, whatever grid phase
        the venue aggregates on (daily bars open at 21:00 UTC, for example).
        ``MN1`` has no fixed length, so a month bar is closed once the next
        calendar month on the provider's timezone has opened.

        :param symbol: The cTrader symbol name (unused; the live subscription
            already fixes the instrument).
        :param timeframe: Timeframe in TradingView format.
        :param since_ms: Opening of the last bar the framework already holds.
        :return: Closed bars strictly after ``since_ms``, ascending, possibly empty.
        :raises CTraderConnectionError: If the live connection is not open.
        :raises CTraderWireError: If a history request fails, or the venue capped
            a page and the rest of it could not be read.
        """
        wire = self._wire
        account_id = self._live_account_id
        if wire is None or account_id is None:
            raise CTraderConnectionError('live connection not established')
        exchange_period = self.to_exchange_timeframe(timeframe)
        period_ms = max(1, int(in_seconds(timeframe))) * 1000
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        symbol_id = await self._resolve_symbol_id(wire, account_id)

        window_ms = period_ms * 2000
        bars_by_open: dict[int, OHLCV] = {}
        window_start = since_ms + 1
        while window_start < now_ms:
            window_end = min(window_start + window_ms, now_ms)
            window_bars, covered = await self._fetch_trendbar_window(
                wire, account_id, symbol_id, exchange_period, window_start, window_end,
            )
            if not covered:
                raise CTraderWireError(
                    f'trendbar history for {timeframe} between {window_start} and '
                    f'{window_end} was capped and the rest could not be read'
                )
            for opening, trendbar in window_bars.items():
                if since_ms < opening < window_end and opening not in bars_by_open:
                    bars_by_open[opening] = self._decode_trendbar(trendbar)
            window_start = window_end

        if exchange_period == 'MN1':
            closed: list[OHLCV] = []
            for opening in sorted(bars_by_open):
                next_open = self._next_month_opening(opening)
                if next_open is None:
                    raise CTraderWireError(
                        f'monthly bar closes cannot be placed: invalid timezone '
                        f'{self.timezone!r}'
                    )
                if next_open <= now_ms:
                    closed.append(bars_by_open[opening])
            return closed
        return [
            bars_by_open[opening]
            for opening in sorted(bars_by_open)
            if opening + period_ms <= now_ms
        ]

    @override
    async def watch_ohlcv(self, symbol: str, timeframe: str) -> OHLCV:
        """Return the next live OHLCV update from the spot/trendbar feed.

        On first call for a symbol it subscribes to spot quotes and live
        trendbars for the requested period; subsequent calls drain the bars the
        background receive loop has queued. A bar arrives as ``is_closed=False``
        while it is still forming and is re-emitted with ``is_closed=True`` once a
        newer bar timestamp shows up.

        The stream hands on what the venue sends: a slot the socket skipped is
        not filled here (the framework recovers it through
        :meth:`backfill_closed_bars`). Its closes only move forward — a close at
        or behind the last one handed on is dropped — and a reconnect starts the
        bar in progress afresh (see :meth:`~_CTraderBase.connect`), so a partial
        bar from before the outage is never handed on as closed.

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

            delivered = self._pending_bars.popleft()
            if delivered.is_closed:
                last_closed = self._last_live_closed_bar
                if last_closed is not None and delivered.timestamp <= last_closed.timestamp:
                    # A close at or behind one already handed on is never
                    # handed on again: the stream's closes only move forward.
                    continue
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
        the close inside the range. The ask O/H/L/C is attached from the spot
        ``ask`` stream when available.
        """
        high, low, close = candle.high, candle.low, candle.close
        if self._last_bid is not None:
            close = self._last_bid
            high = max(high, close)
            low = min(low, close)
        candle = candle._replace(high=high, low=low, close=close, is_closed=is_closed)
        if self._ask_bar is None:
            return candle
        return candle._replace(ask=AskOHLC(*self._ask_bar))
