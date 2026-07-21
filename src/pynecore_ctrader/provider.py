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
import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Callable, cast
from zoneinfo import ZoneInfo

from pynecore.core.plugin import override, Broker
from pynecore.core.syminfo import (
    SymInfo, SymInfoInterval, SymInfoScheduleVariant, SymInfoSession,
)
from pynecore.lib.timeframe import in_seconds
from pynecore.types.ohlcv import OHLCV

from . import auth
from ._base import _CTraderBase
from .config import CTraderConfig
from .helpers import VOLUME_SCALE
from .messages import OpenApiMessages_pb2 as _oa
from .messages import OpenApiModelMessages_pb2 as _model
from .wire import CTraderConnectionError, CTraderProtocolError

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


class _ProviderMixin(_CTraderBase):
    """Provider mix-in: timeframe maps, listings, symbol info and OHLCV."""

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

    def _period_value(self) -> int:
        """Return the numeric ``ProtoOATrendbarPeriod`` for the current timeframe."""
        assert self.xchg_timeframe is not None
        return _model.ProtoOATrendbarPeriod.Value(self.xchg_timeframe)

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
        return cast("list[Broker]", self._run(self._app_session(work)))

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
        return cast(list[str], self._run(self._authed_session(work)))

    async def _fetch_light_symbols(
        self, wire, account_id: int, *, recover: bool = False
    ) -> list[_model.ProtoOALightSymbol]:
        """Fetch the account's light-symbol list and cache name -> id.

        :param recover: When ``True`` (the live order / state paths, where
            ``wire`` is the persistent connection) route the account-scoped
            request through :meth:`_account_request` so a mid-session de-auth is
            recovered instead of leaking a raw protocol error. The one-shot CLI
            paths leave it ``False`` (they run on a private, just-authed wire).
        """
        req = _oa.ProtoOASymbolsListReq(ctidTraderAccountId=account_id)
        response = await (self._account_request(req) if recover else wire.send_request(req))
        response = cast(_oa.ProtoOASymbolsListRes, response)
        symbols = list(response.symbol)
        self._symbols_by_name = {s.symbolName: s.symbolId for s in symbols}
        self._symbols_by_id = {s.symbolId: s.symbolName for s in symbols}
        return symbols

    # --- symbol info --------------------------------------------------------

    @override
    def update_symbol_info(self) -> SymInfo:
        """Fetch full symbol metadata and map it to a :class:`SymInfo`."""
        assert self.symbol is not None

        async def work(wire, account_id: int) -> SymInfo:
            light = await self._fetch_light_symbols(wire, account_id)
            match = next((s for s in light if s.symbolName == self.symbol), None)
            if match is None:
                raise auth.CTraderAuthError(
                    "SYMBOL_NOT_FOUND", f"symbol '{self.symbol}' not on this account"
                )

            assets_res = cast(_oa.ProtoOAAssetListRes, await wire.send_request(
                _oa.ProtoOAAssetListReq(ctidTraderAccountId=account_id)
            ))
            asset_names = {a.assetId: a.name for a in assets_res.asset}

            detail_res = cast(_oa.ProtoOASymbolByIdRes, await wire.send_request(
                _oa.ProtoOASymbolByIdReq(
                    ctidTraderAccountId=account_id, symbolId=[match.symbolId]
                )
            ))
            if not detail_res.symbol:
                raise auth.CTraderAuthError(
                    "SYMBOL_NOT_FOUND", f"no detail for symbol '{self.symbol}'"
                )
            detail = detail_res.symbol[0]
            return self._build_sym_info(match, detail, asset_names)

        return cast(SymInfo, self._run(self._authed_session(work)))

    def _build_sym_info(
        self,
        light: _model.ProtoOALightSymbol,
        detail: _model.ProtoOASymbol,
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
        self, schedule: list[_model.ProtoOAInterval], schedule_tz: str
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
        :param schedule_tz: The IANA zone the intervals are expressed in.
        :return: ``(opening_hours, session_starts, session_ends,
            session_schedules)``.
        """
        try:
            src = ZoneInfo(schedule_tz) if schedule_tz else ZoneInfo('UTC')
        except Exception:  # noqa: BLE001 - unknown zone name falls back to UTC
            src = ZoneInfo('UTC')
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
            period = self._period_value()
            from_ms = int(from_dt.timestamp() * 1000)
            to_ms = int(to_dt.timestamp() * 1000)
            cursor = from_ms
            while cursor < to_ms:
                end_ms = min(cursor + window * 1000, to_ms)
                response = cast(_oa.ProtoOAGetTrendbarsRes, await wire.send_request(
                    _oa.ProtoOAGetTrendbarsReq(
                        ctidTraderAccountId=account_id, symbolId=symbol_id,
                        period=period, fromTimestamp=cursor, toTimestamp=end_ms,
                    )
                ))
                bars = list(response.trendbar)
                if not bars:
                    cursor = end_ms
                    continue
                # The trendbars are bid-based; the ask side has no trendbars and
                # is reconstructed (only when requested) by bucketing ``ASK`` tick
                # history into the same bars (open/high/low/close per period).
                ask_bars = await self._fetch_ask_bars(
                    wire, account_id, symbol_id, cursor, end_ms, period_seconds
                ) if with_extra else {}
                last_ts = cursor
                for bar in bars:
                    candle = self._decode_trendbar(bar)
                    if from_ms <= candle.timestamp * 1000 < to_ms:
                        self.save_ohlcv_data(self._attach_ask(candle, ask_bars.get(candle.timestamp)))
                    last_ts = max(last_ts, (bar.utcTimestampInMinutes * 60 + period_seconds) * 1000)
                if on_progress is not None:
                    on_progress(datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
                                .replace(tzinfo=None))
                cursor = max(last_ts, cursor + window * 1000)

        self._run(self._authed_session(work))

    async def _fetch_ask_bars(
        self, wire, account_id: int, symbol_id: int, from_ms: int, to_ms: int,
        period_seconds: int,
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
        :param period_seconds: The bar length, used to bucket ticks.
        :return: Mapping of bar timestamp (epoch seconds) to ``(open, high, low,
            close)`` ask prices; empty when no tick history covers the window.
        """
        # ``[min_ts, open, max_ts, close, high, low]`` per bar, updated tick by
        # tick so ticks may arrive in any order across pages.
        buckets: dict[int, list[float]] = {}
        upper = to_ms
        while upper > from_ms:
            response = cast(_oa.ProtoOAGetTickDataRes, await wire.send_request(
                _oa.ProtoOAGetTickDataReq(
                    ctidTraderAccountId=account_id, symbolId=symbol_id,
                    type=_model.ProtoOAQuoteType.ASK,
                    fromTimestamp=from_ms, toTimestamp=upper,
                )
            ))
            ticks = self._decode_ticks(response.tickData)
            if not ticks:
                break
            for ts_ms, price in ticks:
                key = (ts_ms // 1000 // period_seconds) * period_seconds
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

    def _decode_trendbar(self, bar: _model.ProtoOATrendbar, *, is_closed: bool = True) -> OHLCV:
        """Decode a ``ProtoOATrendbar`` into an :class:`OHLCV`.

        The low carries the absolute price; open/high/close are deltas above it.
        Prices are integers in units of 1/100000.

        :param bar: The trendbar to decode.
        :param is_closed: Whether the bar is final.
        :return: The decoded OHLCV record.
        """
        low = bar.low
        return OHLCV(
            timestamp=bar.utcTimestampInMinutes * 60,
            open=(low + bar.deltaOpen) / _PRICE_SCALE,
            high=(low + bar.deltaHigh) / _PRICE_SCALE,
            low=low / _PRICE_SCALE,
            close=(low + bar.deltaClose) / _PRICE_SCALE,
            volume=float(bar.volume),
            is_closed=is_closed,
        )

    # --- live OHLCV ---------------------------------------------------------

    async def _subscribe_live(self, symbol: str, timeframe: str) -> None:
        """Subscribe spot quotes + live trendbars for the watched symbol.

        Tolerant of a half-completed earlier attempt: ``watch_ohlcv`` runs
        under the framework's ``asyncio.wait_for`` budget, so a subscribe
        request can reach the server while the coroutine is cancelled
        awaiting the response — the server-side subscription then exists
        with no local record, and the blind replay here would otherwise be
        rejected with ``ALREADY_SUBSCRIBED`` (cTrader subscribes are not
        idempotent). That state is exactly what we want, so the error is
        treated as success (mirroring ``ALREADY_LOGGED_IN`` in
        ``_send_account_auth``). On success the ``(symbol, timeframe)`` pair
        is recorded in ``_live_subscription`` so :meth:`on_reconnect` can
        replay it on a fresh connection.

        :param symbol: The cTrader symbol name.
        :param timeframe: Timeframe in TradingView format.
        :raises CTraderConnectionError: If the live connection is not open.
        """
        wire = self._wire
        if wire is None or self._live_account_id is None:
            raise CTraderConnectionError("live connection not established")
        symbol_id = await self._resolve_symbol_id(wire, self._live_account_id)
        period = _model.ProtoOATrendbarPeriod.Value(self.to_exchange_timeframe(timeframe))
        for request in (
            _oa.ProtoOASubscribeSpotsReq(
                ctidTraderAccountId=self._live_account_id, symbolId=[symbol_id],
            ),
            _oa.ProtoOASubscribeLiveTrendbarReq(
                ctidTraderAccountId=self._live_account_id, period=period,
                symbolId=symbol_id,
            ),
        ):
            try:
                await wire.send_request(request)
            except CTraderProtocolError as exc:
                if exc.error_code != 'ALREADY_SUBSCRIBED':
                    raise
        self._subscribed_symbols.add(symbol)
        self._watch_symbol_id = symbol_id
        self._live_subscription = (symbol, timeframe)

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
            await self._subscribe_live(*self._live_subscription)
            await self._backfill_live_gap(*self._live_subscription)

    async def _backfill_live_gap(self, symbol: str, timeframe: str) -> None:
        """Queue venue trendbars that closed since the last delivered live bar.

        The current slot is deliberately excluded because it is still forming
        and will arrive through the restored live subscription. Responses are
        sorted and filtered at the local cursor, making inclusive venue bounds
        and replayed edge bars harmless.

        :param symbol: The cTrader symbol name.
        :param timeframe: Timeframe in TradingView format.
        :raises CTraderConnectionError: If the live connection is not open.
        """
        anchor = self._last_live_closed_bar
        if anchor is None:
            return
        wire = self._wire
        account_id = self._live_account_id
        if wire is None or account_id is None:
            raise CTraderConnectionError('live connection not established')

        period_seconds = max(1, int(in_seconds(timeframe)))
        current_slot = (
            int(datetime.now(timezone.utc).timestamp()) // period_seconds
            * period_seconds
        )
        cursor = int(anchor.timestamp) + period_seconds
        if cursor >= current_slot:
            return

        symbol_id = await self._resolve_symbol_id(wire, account_id)
        period = _model.ProtoOATrendbarPeriod.Value(self.to_exchange_timeframe(timeframe))
        recovered_by_timestamp: dict[int, OHLCV] = {}
        window_seconds = period_seconds * 2000
        while cursor < current_slot:
            window_end = min(cursor + window_seconds, current_slot)
            response = cast(_oa.ProtoOAGetTrendbarsRes, await wire.send_request(
                _oa.ProtoOAGetTrendbarsReq(
                    ctidTraderAccountId=account_id,
                    symbolId=symbol_id,
                    period=period,
                    fromTimestamp=cursor * 1000,
                    toTimestamp=window_end * 1000,
                )
            ))
            for trendbar in response.trendbar:
                timestamp = trendbar.utcTimestampInMinutes * 60
                if cursor <= timestamp < window_end:
                    recovered_by_timestamp[timestamp] = self._decode_trendbar(trendbar)
            cursor = window_end

        recovered = [
            recovered_by_timestamp[timestamp]
            for timestamp in sorted(recovered_by_timestamp)
        ]
        self._pending_bars.extend(recovered)
        if recovered:
            logger.info(
                'Backfilled %d closed trendbar(s) after reconnect (%d..%d)',
                len(recovered),
                recovered[0].timestamp,
                recovered[-1].timestamp,
            )

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

        while True:
            if self._pending_bars:
                bar = self._pending_bars.popleft()
                if bar.is_closed:
                    self._last_live_closed_bar = bar
                return bar
            # The event router (see ``_CTraderBase._event_router_loop``) is the
            # sole consumer of ``wire.events`` and forwards spot events here, so
            # ``watch_ohlcv`` and ``watch_orders`` can stream concurrently
            # without racing on the shared queue.
            message = await spot_events.get()
            if not isinstance(message, _oa.ProtoOASpotEvent):
                continue
            if message.symbolId != self._watch_symbol_id:
                continue
            # Roll the trendbars first (they finalize the prior bar against the
            # bid/ask seen so far), then fold THIS event's quotes into the new
            # current bar.
            for bar in message.trendbar:
                self._ingest_live_bar(bar)
            if message.bid:
                self._last_bid = message.bid / _PRICE_SCALE
            if message.ask:
                self._track_ask(message.ask / _PRICE_SCALE)

    def _ingest_live_bar(self, bar: _model.ProtoOATrendbar) -> None:
        """Fold a live trendbar into the pending-bar buffer.

        When the bar's timestamp advances past the bar being tracked, the prior
        bar has closed: it is finalized (spot bid close, ask O/H/L/C) and queued,
        then the quote accumulators are reset for the new bar.
        """
        candle = self._decode_trendbar(bar, is_closed=False)
        if self._current_bar_ts is not None and candle.timestamp > self._current_bar_ts:
            if self._current_bar is not None:
                self._pending_bars.append(self._finalize_bar(self._current_bar, is_closed=True))
            self._reset_quotes()
        self._current_bar_ts = candle.timestamp
        self._current_bar = candle
        self._pending_bars.append(self._finalize_bar(candle, is_closed=False))

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
