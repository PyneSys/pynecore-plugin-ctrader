"""
@pyne

Regression coverage for cTrader historical OHLCV paging and ask reconstruction.
"""
from datetime import datetime, timezone

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model

#: Monthly openings of 2027 — the calendar unit whose length no fixed number of
#: seconds reproduces.
_MONTH_OPENS = [datetime(2027, month, 1, tzinfo=timezone.utc) for month in range(1, 13)]


def _trendbar(moment: datetime, price: int = 114_000) -> _model.ProtoOATrendbar:
    return _model.ProtoOATrendbar(
        utcTimestampInMinutes=int(moment.timestamp()) // 60,
        low=price,
        deltaOpen=1,
        deltaHigh=3,
        deltaClose=2,
        volume=7,
    )


class _HistoryWire:
    """Wire fake serving the trendbars that fall inside each requested window."""

    def __init__(self, opens: list[datetime], ticks: list[tuple[int, int]] | None = None,
                 page_cap: int | None = None, cap_newest: bool = False,
                 report_more: bool = False) -> None:
        self.opens = opens
        self.ticks = ticks or []
        self.page_cap = page_cap
        self.cap_newest = cap_newest
        self.report_more = report_more
        self.windows: list[tuple[int, int]] = []

    async def send_request(self, request):
        if isinstance(request, _oa.ProtoOAGetTrendbarsReq):
            self.windows.append((request.fromTimestamp, request.toTimestamp))
            matched = [
                _trendbar(moment) for moment in self.opens
                if request.fromTimestamp <= int(moment.timestamp() * 1000) < request.toTimestamp
            ]
            has_more = False
            if self.page_cap is not None and len(matched) > self.page_cap:
                matched = (matched[-self.page_cap:] if self.cap_newest
                           else matched[:self.page_cap])
                has_more = self.report_more
            return _oa.ProtoOAGetTrendbarsRes(trendbar=matched, hasMore=has_more)
        if isinstance(request, _oa.ProtoOAGetTickDataReq):
            return _oa.ProtoOAGetTickDataRes(
                tickData=[
                    _model.ProtoOATickData(timestamp=ts, tick=price)
                    for ts, price in self.ticks
                ],
                hasMore=False,
            )
        raise AssertionError(f"unexpected request: {type(request).__name__}")


def _provider(wire: _HistoryWire, timeframe: str) -> CTrader:
    config = CTraderConfig(demo=True, client_id="c", client_secret="s", account_id="999")
    provider = CTrader(symbol="broker:EURUSD", timeframe=timeframe, config=config)
    provider._wire = wire  # type: ignore[assignment]
    provider._live_account_id = 999
    provider._symbols_by_name = {"EURUSD": 1}
    saved: list = []
    provider.save_ohlcv_data = saved.append  # type: ignore[method-assign]
    provider.saved = saved  # type: ignore[attr-defined]

    async def _session(work):
        return await work(wire, 999)

    provider._authed_session = _session  # type: ignore[method-assign]
    return provider


def __test_monthly_download_skips_no_calendar_month__():
    """Paging must follow the venue's openings, not an average month length."""
    wire = _HistoryWire(_MONTH_OPENS)
    provider = _provider(wire, "1M")

    # A one-bar chunk makes every window shorter than the drift between the
    # average month and the real one, which is where the skip used to appear.
    provider.download_ohlcv(
        datetime(2027, 1, 2),
        datetime(2027, 5, 2),
        limit=1,
    )

    assert [
        datetime.fromtimestamp(candle.timestamp / 1000, timezone.utc).strftime("%Y-%m")
        for candle in provider.saved  # type: ignore[attr-defined]
    ] == ["2027-02", "2027-03", "2027-04", "2027-05"]


def __test_short_page_resumes_at_the_last_returned_bar__():
    """A page holding fewer bars than its window must not lose the remainder."""
    minute_opens = [
        datetime.fromtimestamp(1_800_000_000 + 60 * step, timezone.utc)
        for step in range(5)
    ]
    wire = _HistoryWire(minute_opens, page_cap=2)
    provider = _provider(wire, "1")

    provider.download_ohlcv(
        datetime.fromtimestamp(1_800_000_000, timezone.utc).replace(tzinfo=None),
        datetime.fromtimestamp(1_800_000_300, timezone.utc).replace(tzinfo=None),
        limit=5,
    )

    assert [candle.timestamp for candle in provider.saved] == [  # type: ignore[attr-defined]
        1_800_000_000_000 + 60_000 * step for step in range(5)
    ]


def __test_backend_capped_page_keeps_the_older_bars__():
    """A cap that answers with the newest rows must not lose the older ones."""
    minute_opens = [
        datetime.fromtimestamp(1_800_000_000 + 60 * step, timezone.utc)
        for step in range(5)
    ]
    wire = _HistoryWire(minute_opens, page_cap=2, cap_newest=True, report_more=True)
    provider = _provider(wire, "1")

    provider.download_ohlcv(
        datetime.fromtimestamp(1_800_000_000, timezone.utc).replace(tzinfo=None),
        datetime.fromtimestamp(1_800_000_300, timezone.utc).replace(tzinfo=None),
        limit=5,
    )

    assert [candle.timestamp for candle in provider.saved] == [  # type: ignore[attr-defined]
        1_800_000_000_000 + 60_000 * step for step in range(5)
    ]


def __test_capped_page_does_not_bucket_foreign_ticks_into_its_last_bar__():
    """Ticks past the newest opening a capped page bounds belong to no bar here."""
    base_ms = 1_800_000_000_000
    minute_opens = [
        datetime.fromtimestamp(1_800_000_000 + 60 * step, timezone.utc)
        for step in range(5)
    ]
    wire = _HistoryWire(
        minute_opens,
        # Newest-first delta chain: a spike in the fifth minute, then the only
        # quote that really belongs to the second minute.
        ticks=[(base_ms + 240_000, 114_900), (-180_000, -700)],
        page_cap=2,
        report_more=True,
    )
    provider = _provider(wire, "1")

    provider.download_ohlcv(
        datetime.fromtimestamp(1_800_000_000, timezone.utc).replace(tzinfo=None),
        datetime.fromtimestamp(1_800_000_300, timezone.utc).replace(tzinfo=None),
        limit=5,
        with_extra=True,
    )

    second = provider.saved[1]  # type: ignore[attr-defined]
    assert second.timestamp == base_ms + 60_000
    assert second.extra_fields is not None
    assert second.extra_fields['ask_high'] == 1.142


def __test_oldest_first_cap_below_the_first_opening_is_not_called_covered__():
    """A narrowed request that adds nothing leaves the newest rows unread."""
    base_ms = 1_800_000_000_000
    minute_opens = [
        datetime.fromtimestamp(1_800_000_000 + 60 * step, timezone.utc)
        for step in range(5)
    ]
    wire = _HistoryWire(
        minute_opens,
        ticks=[(base_ms + 240_000, 114_900), (-180_000, -700)],
        page_cap=2,
        report_more=True,
    )
    provider = _provider(wire, "1")

    # The window starts below the first opening, so narrowing it down to that
    # opening answers with nothing at all — and an empty answer carries no
    # ``hasMore``, which must not be read as "the whole window was delivered".
    provider.download_ohlcv(
        datetime.fromtimestamp(1_800_000_000 - 30, timezone.utc).replace(tzinfo=None),
        datetime.fromtimestamp(1_800_000_300, timezone.utc).replace(tzinfo=None),
        limit=5,
        with_extra=True,
    )

    second = provider.saved[1]  # type: ignore[attr-defined]
    assert second.timestamp == base_ms + 60_000
    assert second.extra_fields is not None
    assert second.extra_fields['ask_high'] == 1.142


def __test_monthly_ask_ticks_bucket_into_the_venue_bars__():
    """Ask ticks are keyed by the real bar opening, not a fixed-width grid."""
    july = _MONTH_OPENS[6]
    july_ms = int(july.timestamp() * 1000)
    wire = _HistoryWire(
        [july],
        # Newest-first delta chain: absolute head, then cumulative deltas.
        ticks=[(july_ms + 10 * 86_400_000, 114_500), (-86_400_000, -300)],
    )
    provider = _provider(wire, "1M")

    provider.download_ohlcv(
        july.replace(tzinfo=None), _MONTH_OPENS[7].replace(tzinfo=None), with_extra=True,
    )

    candle = provider.saved[0]  # type: ignore[attr-defined]
    assert candle.timestamp == july_ms
    assert candle.extra_fields is not None
    assert candle.extra_fields['ask_high'] == 1.145
    assert candle.extra_fields['ask_low'] == 1.142
