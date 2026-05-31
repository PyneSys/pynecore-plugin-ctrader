"""Data-provider mix-in for the cTrader Open API plugin.

Implements the :class:`~pynecore.core.plugin.ProviderPlugin` /
:class:`~pynecore.core.plugin.LiveProviderPlugin` data surface on top of
:class:`~pynecore_ctrader._base._CTraderBase`:

- timeframe conversion between TradingView strings and ``ProtoOATrendbarPeriod``,
- broker and symbol listing (``--list-brokers`` / ``--list-symbols``),
- symbol metadata (:meth:`update_symbol_info`) from ``ProtoOASymbol`` plus the
  asset list, with the weekly trading schedule mapped to PyneCore sessions,
- historical OHLCV via paged ``ProtoOAGetTrendbarsReq``, and
- live OHLCV from ``ProtoOASpotEvent`` trendbars.

All cTrader trendbar prices are integers in units of 1/100000; the low carries
the absolute price and open/high/close are non-negative deltas above it.
"""
import logging
from collections import deque
from datetime import datetime, time, timedelta, timezone
from typing import Callable, cast
from zoneinfo import ZoneInfo

from pynecore.core.plugin import override
from pynecore.core.syminfo import SymInfo, SymInfoInterval, SymInfoSession
from pynecore.lib.timeframe import in_seconds
from pynecore.types.ohlcv import OHLCV

from . import auth
from ._base import _CTraderBase
from .config import CTraderConfig
from .messages import OpenApiMessages_pb2 as _oa
from .messages import OpenApiModelMessages_pb2 as _model
from .wire import CTraderConnectionError

logger = logging.getLogger(__name__)

#: cTrader prices are integers in units of 1/100000 of the quote currency.
_PRICE_SCALE = 100000.0

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
    def get_list_of_brokers(cls) -> list[str]:
        """List the broker titles the configured token grants accounts with.

        Unlike a static exchange list, cTrader's brokers come from the user's
        own account list, so this opens a short-lived authenticated socket. The
        config is loaded from the standard plugin path; this is the only
        provider method that reaches the CLI app state, and only on the
        ``--list-brokers`` path (never inside a security subprocess).

        :return: The distinct broker titles, sorted.
        """
        # Local import: keep the plugin import graph free of the CLI app module;
        # this classmethod only ever runs from the ``pyne data`` command.
        from pynecore.cli.app import app_state
        from pynecore.core.config import ensure_config

        config = ensure_config(
            CTraderConfig, app_state.config_dir / 'plugins' / 'ctrader.toml'
        )
        return cls(symbol=None, config=cast(CTraderConfig, config))._list_brokers()

    def _list_brokers(self) -> list[str]:
        """Enumerate the distinct broker titles for the configured host kind."""
        async def work(wire) -> list[str]:
            accounts = await self._get_accounts(wire)
            want_live = not self._demo
            titles = {a.brokerTitleShort for a in accounts
                      if a.isLive == want_live and a.brokerTitleShort}
            return sorted(titles)
        return cast(list[str], self._run(self._app_session(work)))

    # --- symbol listing + resolution ----------------------------------------

    @override
    def get_list_of_symbols(self, *args, **kwargs) -> list[str]:
        """List the tradable symbol names of the selected broker's account."""
        async def work(wire, account_id: int) -> list[str]:
            symbols = await self._fetch_light_symbols(wire, account_id)
            return sorted(s.symbolName for s in symbols if s.symbolName)
        return cast(list[str], self._run(self._authed_session(work)))

    async def _fetch_light_symbols(
        self, wire, account_id: int
    ) -> list[_model.ProtoOALightSymbol]:
        """Fetch the account's light-symbol list and cache name -> id."""
        response = await wire.send_request(
            _oa.ProtoOASymbolsListReq(ctidTraderAccountId=account_id)
        )
        response = cast(_oa.ProtoOASymbolsListRes, response)
        symbols = list(response.symbol)
        self._symbols_by_name = {s.symbolName: s.symbolId for s in symbols}
        return symbols

    # --- symbol info --------------------------------------------------------

    @override
    def update_symbol_info(self) -> SymInfo:
        """Fetch full symbol metadata and map it to a :class:`SymInfo`."""
        assert self.symbol is not None
        assert self.timeframe is not None

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
        assert self.timeframe is not None

        digits = detail.digits
        mintick = 10 ** -digits
        pricescale = 10 ** digits

        opening_hours, session_starts, session_ends = self._schedule_to_sessions(
            list(detail.schedule), detail.scheduleTimeZone
        )

        return SymInfo(
            prefix='CTRADER',
            description=light.description or light.symbolName,
            ticker=light.symbolName,
            currency=asset_names.get(light.quoteAssetId, ''),
            basecurrency=asset_names.get(light.baseAssetId) or None,
            period=self.timeframe,
            type='other',
            mintick=mintick,
            pricescale=pricescale,
            minmove=1,
            pointvalue=float(detail.lotSize) if detail.lotSize else 1.0,
            timezone=self.timezone,
            opening_hours=opening_hours,
            session_starts=session_starts,
            session_ends=session_ends,
        )

    def _schedule_to_sessions(
        self, schedule: list[_model.ProtoOAInterval], schedule_tz: str
    ) -> tuple[list[SymInfoInterval], list[SymInfoSession], list[SymInfoSession]]:
        """Map cTrader's weekly schedule to PyneCore sessions.

        Each :class:`ProtoOAInterval` is given in seconds from Sunday 00:00 in
        ``schedule_tz`` (start inclusive, end exclusive). The interval is anchored
        on the current week's Sunday (so the active DST offset applies), shifted
        into ``self.timezone`` and split at local midnight so a session that
        straddles midnight matches candles on both local weekdays — the same
        shape the session checker expects.

        :param schedule: The weekly trading intervals.
        :param schedule_tz: The IANA zone the intervals are expressed in.
        :return: ``(opening_hours, session_starts, session_ends)``.
        """
        try:
            src = ZoneInfo(schedule_tz) if schedule_tz else ZoneInfo('UTC')
        except Exception:  # noqa: BLE001 - unknown zone name falls back to UTC
            src = ZoneInfo('UTC')
        dst = ZoneInfo(self.timezone)

        now_src = datetime.now(src)
        days_since_sunday = (now_src.weekday() + 1) % 7
        sunday = datetime.combine(
            (now_src - timedelta(days=days_since_sunday)).date(), time(0, 0), tzinfo=src
        )

        opening_hours: list[SymInfoInterval] = []
        session_starts: list[SymInfoSession] = []
        session_ends: list[SymInfoSession] = []
        for interval in schedule:
            start_dt = (sunday + timedelta(seconds=interval.startSecond)).astimezone(dst)
            end_dt = (sunday + timedelta(seconds=interval.endSecond)).astimezone(dst)
            if end_dt <= start_dt:
                continue
            session_starts.append(SymInfoSession(day=start_dt.weekday(), time=start_dt.time()))
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

    # --- historical OHLCV ---------------------------------------------------

    @override
    def download_ohlcv(self, time_from: datetime, time_to: datetime,
                       on_progress: Callable[[datetime], None] | None = None,
                       limit: int | None = None) -> None:
        """Download historical OHLCV via paged ``ProtoOAGetTrendbarsReq``."""
        assert self.symbol is not None
        period_seconds = max(1, int(in_seconds(self.timeframe))) if self.timeframe else 60
        chunk = limit or 2000
        window = period_seconds * chunk

        async def work(wire, account_id: int) -> None:
            symbol_id = await self._resolve_symbol_id(wire, account_id)
            period = self._period_value()
            from_ms = int(time_from.timestamp() * 1000)
            to_ms = int(time_to.timestamp() * 1000)
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
                last_ts = cursor
                for bar in bars:
                    candle = self._decode_trendbar(bar)
                    if from_ms <= candle.timestamp * 1000 < to_ms:
                        self.save_ohlcv_data(candle)
                    last_ts = max(last_ts, (bar.utcTimestampInMinutes * 60 + period_seconds) * 1000)
                if on_progress is not None:
                    on_progress(datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc))
                cursor = max(last_ts, cursor + window * 1000)

        self._run(self._authed_session(work))

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
            symbol_id = await self._resolve_symbol_id(wire, self._live_account_id)
            period = _model.ProtoOATrendbarPeriod.Value(self.to_exchange_timeframe(timeframe))
            await wire.send_request(_oa.ProtoOASubscribeSpotsReq(
                ctidTraderAccountId=self._live_account_id, symbolId=[symbol_id],
            ))
            await wire.send_request(_oa.ProtoOASubscribeLiveTrendbarReq(
                ctidTraderAccountId=self._live_account_id, period=period, symbolId=symbol_id,
            ))
            self._subscribed_symbols.add(symbol)
            self._watch_symbol_id = symbol_id

        while True:
            if self._pending_bars:
                return self._pending_bars.popleft()
            message = await wire.events.get()
            if not isinstance(message, _oa.ProtoOASpotEvent):
                continue
            if message.symbolId != self._watch_symbol_id:
                continue
            for bar in message.trendbar:
                self._ingest_live_bar(bar)

    def _ingest_live_bar(self, bar: _model.ProtoOATrendbar) -> None:
        """Fold a live trendbar into the pending-bar buffer.

        When the bar's timestamp advances past the bar being tracked, the prior
        bar is flushed as closed before the new (forming) bar is queued.
        """
        candle = self._decode_trendbar(bar, is_closed=False)
        if self._current_bar_ts is not None and candle.timestamp > self._current_bar_ts:
            if self._current_bar is not None:
                self._pending_bars.append(self._current_bar._replace(is_closed=True))
        self._current_bar_ts = candle.timestamp
        self._current_bar = candle
        self._pending_bars.append(candle)
