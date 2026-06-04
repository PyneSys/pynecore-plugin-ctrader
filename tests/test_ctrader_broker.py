"""
@pyne
"""
import asyncio

import pytest

from pynecore.core.broker.exceptions import (
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.models import (
    CancelDispositionOutcome,
    CancelIntent,
    CapabilityLevel,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExitIntent,
    LegType,
    OrderStatus,
    OrderType,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    create_entry_order_row,
    mark_disposition_unknown,
)

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.exceptions import CTraderProtocolError, map_error_code
from pynecore_ctrader.helpers import (
    money_value,
    quantize_volume,
    round_price,
    volume_to_units,
)
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model
from pynecore_ctrader.models import _SymbolRules

_RULES = _SymbolRules(
    symbol_id=1, digits=5, min_volume=1000, step_volume=1000, max_volume=10_000_000,
)


class _FakeBroker(CTrader):
    """cTrader broker with the wire layer stubbed out.

    Records every dispatched order request and returns a canned execution
    event, so the ``execute_*`` request-building and conversion logic can be
    exercised without a live connection.
    """

    def __init__(self, *, reconcile=None):
        super().__init__(symbol=None, config=_make_config())
        self._live_account_id = 999
        self._symbols_by_name = {'EURUSD': 1}
        self._symbols_by_id = {1: 'EURUSD'}
        self._symbol_rules = {'EURUSD': _RULES}
        self.sent: list = []
        self._canned_event: _oa.ProtoOAExecutionEvent | None = None
        self._reconcile_res = reconcile
        self._raise_on_dispatch: Exception | None = None
        self.coid_at_dispatch: str | None = None
        self.state_at_dispatch: str | None = None

    async def _get_symbol_rules(self, symbol: str) -> _SymbolRules:
        return _RULES

    async def _dispatch_order(self, req, *, coid, context):
        self.sent.append(req)
        self.coid_at_dispatch = coid
        if self.store_ctx is not None:
            row = self.store_ctx.get_order(coid)
            self.state_at_dispatch = row.state if row is not None else None
        if self._raise_on_dispatch is not None:
            raise self._raise_on_dispatch
        if self._canned_event is not None:
            return self._canned_event
        return _exec_event(_model.ProtoOAExecutionType.ORDER_ACCEPTED)

    async def _reconcile(self):
        return self._reconcile_res


def _make_config(**overrides) -> CTraderConfig:
    defaults = dict(demo=True, client_id="cid", client_secret="sec", account_id="999")
    defaults.update(overrides)
    return CTraderConfig(**defaults)


def _make_order(*, order_id=111, volume=1000, side=_model.ProtoOATradeSide.BUY,
                executed=0, price=0.0, order_type=_model.ProtoOAOrderType.MARKET,
                status=_model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED,
                position_id=0, closing=False, limit=0.0, stop=0.0):
    return _model.ProtoOAOrder(
        orderId=order_id, orderType=order_type, orderStatus=status,
        executedVolume=executed, executionPrice=price, positionId=position_id,
        closingOrder=closing, limitPrice=limit, stopPrice=stop,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=volume, tradeSide=side),
    )


def _exec_event(exec_type, *, order=None, deal=None):
    ev = _oa.ProtoOAExecutionEvent(executionType=exec_type)
    ev.order.CopyFrom(order if order is not None else _make_order())
    if deal is not None:
        ev.deal.CopyFrom(deal)
    return ev


def _envelope(intent, run_tag="ab12"):
    return DispatchEnvelope(
        intent=intent, run_tag=run_tag, bar_ts_ms=1_700_000_000_000, retry_seq=0,
    )


# === Unit conversions =====================================================

def __test_quantize_volume_snaps_to_step__():
    # 10 units * 100 = 1000 centi-units, already on a 1000 step.
    assert quantize_volume(10.0, 1000) == 1000
    # 12.3 units -> 1230 centi, snapped to nearest 1000 -> 1000.
    assert quantize_volume(12.3, 1000) == 1000
    # 15.0 units -> 1500 centi, snapped to nearest 1000 -> 2000.
    assert quantize_volume(15.0, 1000) == 2000
    # Zero step falls back to a plain round of the centi value.
    assert quantize_volume(7.0, 0) == 700


def __test_volume_round_trip__():
    assert volume_to_units(1000) == 10.0
    assert volume_to_units(2500) == 25.0


def __test_round_price_uses_digits__():
    assert round_price(1.234567, 5) == 1.23457
    assert round_price(1.2, 3) == 1.2


def __test_money_value_uses_money_digits__():
    assert money_value(123456, 2) == 1234.56
    assert money_value(5_000_000, 5) == 50.0


# === Capabilities =========================================================

def __test_capabilities_native_where_expected__():
    caps = _FakeBroker().get_capabilities()
    assert caps.watch_orders is CapabilityLevel.NATIVE
    assert caps.fetch_position is CapabilityLevel.NATIVE
    assert caps.tp_sl_bracket is CapabilityLevel.NATIVE
    assert caps.stop_order is CapabilityLevel.NATIVE
    assert caps.trailing_stop is CapabilityLevel.NATIVE
    # Conservative until live verification (M2 open questions).
    assert caps.amend_order is CapabilityLevel.PARTIAL_NATIVE
    assert caps.idempotency is CapabilityLevel.PARTIAL_NATIVE
    # Software-upheld semantics.
    assert caps.reduce_only is CapabilityLevel.SOFTWARE
    assert caps.partial_qty_bracket_exit is CapabilityLevel.SOFTWARE
    assert caps.oca_cancel is CapabilityLevel.SOFTWARE


# === execute_entry: order mapping =========================================

def __test_entry_market_request__():
    broker = _FakeBroker()
    intent = EntryIntent(pine_id="Long", symbol="EURUSD", side="buy",
                         qty=10.0, order_type=OrderType.MARKET)
    orders = asyncio.run(broker.execute_entry(_envelope(intent)))
    req = broker.sent[0]
    assert isinstance(req, _oa.ProtoOANewOrderReq)
    assert req.orderType == _model.ProtoOAOrderType.MARKET
    assert req.tradeSide == _model.ProtoOATradeSide.BUY
    assert req.volume == 1000
    assert req.clientOrderId  # non-empty deterministic id
    assert len(orders) == 1 and orders[0].order_type is OrderType.MARKET


def __test_entry_limit_request_rounds_price__():
    broker = _FakeBroker()
    intent = EntryIntent(pine_id="Long", symbol="EURUSD", side="buy", qty=10.0,
                         order_type=OrderType.LIMIT, limit=1.234567)
    asyncio.run(broker.execute_entry(_envelope(intent)))
    req = broker.sent[0]
    assert req.orderType == _model.ProtoOAOrderType.LIMIT
    assert req.limitPrice == 1.23457
    assert req.timeInForce == _model.ProtoOATimeInForce.GOOD_TILL_CANCEL


def __test_entry_stop_request__():
    broker = _FakeBroker()
    intent = EntryIntent(pine_id="Short", symbol="EURUSD", side="sell", qty=10.0,
                         order_type=OrderType.STOP, stop=1.5)
    asyncio.run(broker.execute_entry(_envelope(intent)))
    req = broker.sent[0]
    assert req.orderType == _model.ProtoOAOrderType.STOP
    assert req.tradeSide == _model.ProtoOATradeSide.SELL
    assert req.stopPrice == 1.5


def __test_entry_below_min_volume_skipped__():
    broker = _FakeBroker()
    intent = EntryIntent(pine_id="Long", symbol="EURUSD", side="buy", qty=1.0,
                         order_type=OrderType.MARKET)
    with pytest.raises(OrderSkippedByPlugin):
        asyncio.run(broker.execute_entry(_envelope(intent)))
    assert broker.sent == []  # nothing reached the wire


def __test_entry_above_max_volume_skipped__():
    broker = _FakeBroker()
    intent = EntryIntent(pine_id="Long", symbol="EURUSD", side="buy",
                         qty=1_000_000.0, order_type=OrderType.MARKET)
    with pytest.raises(OrderSkippedByPlugin):
        asyncio.run(broker.execute_entry(_envelope(intent)))
    assert broker.sent == []


# === execute_entry: persist-first dispatch rows ===========================

def _open_store(tmp_path, broker) -> None:
    """Attach an in-memory-backed BrokerStore run context to ``broker``."""
    store = BrokerStore(tmp_path / "broker.sqlite", plugin_name=broker.plugin_name)
    identity = RunIdentity(
        strategy_id="persist", symbol="EURUSD", timeframe="60",
        account_id="persist-account",
    )
    broker.store_ctx = store.open_run(identity, script_source="// persist")


def _only_live_order(broker):
    rows = list(broker.store_ctx.iter_live_orders())
    assert len(rows) == 1
    return rows[0]


def __test_entry_persists_submitted_before_wire_send__(tmp_path):
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    intent = EntryIntent(pine_id="Long", symbol="EURUSD", side="buy",
                         qty=10.0, order_type=OrderType.MARKET)
    asyncio.run(broker.execute_entry(_envelope(intent)))
    # The dispatch row already existed in ``submitted`` at the instant the
    # wire send ran — persist-first ordering, not persist-after-ack.
    assert broker.state_at_dispatch == 'submitted'
    # ... and the ack promoted that same row to ``confirmed``.
    assert _only_live_order(broker).state == 'confirmed'


def __test_entry_timeout_marks_disposition_unknown__(tmp_path):
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    broker._raise_on_dispatch = OrderDispositionUnknownError(
        "cTrader entry timed out; disposition unknown", client_order_id="x",
    )
    intent = EntryIntent(pine_id="Long", symbol="EURUSD", side="buy",
                         qty=10.0, order_type=OrderType.MARKET)
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_entry(_envelope(intent)))
    # The order may have reached the broker — keep the row for recovery.
    assert _only_live_order(broker).state == 'disposition_unknown'


def __test_entry_reject_marks_rejected__(tmp_path):
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    broker._raise_on_dispatch = ExchangeOrderRejectedError("cTrader rejected the order")
    intent = EntryIntent(pine_id="Long", symbol="EURUSD", side="buy",
                         qty=10.0, order_type=OrderType.MARKET)
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.execute_entry(_envelope(intent)))
    # Definitive reject — the persist-first row lands terminal.
    assert _only_live_order(broker).state == 'rejected'


# === execute_exit / close: bracket + position close =======================

def __test_exit_amends_position_sltp__():
    res = _oa.ProtoOAReconcileRes()
    res.position.append(_model.ProtoOAPosition(
        positionId=555,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=1000,
                                          tradeSide=_model.ProtoOATradeSide.BUY),
    ))
    broker = _FakeBroker(reconcile=res)
    intent = ExitIntent(pine_id="Exit", from_entry="Long", symbol="EURUSD",
                        side="sell", qty=10.0, tp_price=1.30, sl_price=1.10)
    legs = asyncio.run(broker.execute_exit(_envelope(intent)))
    req = broker.sent[0]
    assert isinstance(req, _oa.ProtoOAAmendPositionSLTPReq)
    assert req.positionId == 555
    assert req.stopLoss == 1.1 and req.takeProfit == 1.3
    leg_ids = {leg.id for leg in legs}
    assert leg_ids == {"555:tp", "555:sl"}


def __test_close_request_uses_position_and_volume__():
    res = _oa.ProtoOAReconcileRes()
    res.position.append(_model.ProtoOAPosition(
        positionId=777,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=1000,
                                          tradeSide=_model.ProtoOATradeSide.BUY),
    ))
    broker = _FakeBroker(reconcile=res)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=888, executed=1000, price=1.25,
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
    )
    intent = CloseIntent(pine_id="Long", symbol="EURUSD", side="sell", qty=10.0)
    order = asyncio.run(broker.execute_close(_envelope(intent)))
    req = broker.sent[0]
    assert isinstance(req, _oa.ProtoOAClosePositionReq)
    assert req.positionId == 777 and req.volume == 1000
    assert order.reduce_only is True


# === watch_orders: execution-event translation ============================

def __test_translate_entry_fill__():
    broker = _FakeBroker()
    deal = _model.ProtoOADeal(dealId=5, filledVolume=1000, executionPrice=1.2345,
                              commission=12, moneyDigits=2)
    ev = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(executed=1000, price=1.2345,
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
        deal=deal,
    )
    out = broker._translate_exec_event(ev)
    assert out is not None
    assert out.event_type == 'filled'
    assert out.fill_price == 1.2345
    assert out.fill_qty == 10.0
    assert out.fee == 0.12


@pytest.mark.parametrize("exec_type,expected", [
    (_model.ProtoOAExecutionType.ORDER_ACCEPTED, 'created'),
    (_model.ProtoOAExecutionType.ORDER_PARTIAL_FILL, 'partial'),
    (_model.ProtoOAExecutionType.ORDER_CANCELLED, 'cancelled'),
    (_model.ProtoOAExecutionType.ORDER_REJECTED, 'rejected'),
    (_model.ProtoOAExecutionType.ORDER_EXPIRED, 'cancelled'),
])
def __test_translate_event_types__(exec_type, expected):
    broker = _FakeBroker()
    out = broker._translate_exec_event(_exec_event(exec_type))
    assert out is not None and out.event_type == expected


def __test_translate_skips_non_lifecycle__():
    broker = _FakeBroker()
    out = broker._translate_exec_event(_exec_event(_model.ProtoOAExecutionType.SWAP))
    assert out is None


# === Parked-entry fill recovery by clientOrderId echo =====================

def __test_translate_recovers_parked_entry_fill_by_coid__(tmp_path):
    # A MARKET entry that PARKED on an ambiguous timeout (disposition_unknown, no
    # order_id/position_id ref recorded) but in fact FILLED: the PUSH fill echoes
    # our coid, so it must be attributed to THIS entry — not mis-dropped as
    # external, which would strand an unmanaged open position until the next
    # restart's recovery re-entry.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    create_entry_order_row(
        broker.store_ctx, coid='c1', symbol='EURUSD', side='buy', qty=10.0,
        intent_key='pine1', pine_entry_id='pine1',
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    mark_disposition_unknown(broker.store_ctx, coid='c1')
    broker.store_ctx.record_park('c1', 'pine1')
    assert 'c1' in broker.store_ctx.replay()[1]

    order = _make_order(order_id=111, executed=1000, position_id=222,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    order.clientOrderId = 'c1'
    deal = _model.ProtoOADeal(dealId=5, filledVolume=1000, executionPrice=1.2345)
    out = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order, deal=deal))

    # Recovered (not dropped) and attributed to the parked entry.
    assert out is not None
    assert out.event_type == 'filled'
    assert out.pine_id == 'pine1'
    assert out.leg_type is LegType.ENTRY
    # Broker refs backfilled so every LATER event + the reconcile snapshot can
    # reverse-map, and the durable fill cursor advanced.
    assert broker.store_ctx.find_by_ref('order_id', '111') is not None
    assert broker.store_ctx.find_by_ref('position_id', '222') is not None
    row = broker.store_ctx.get_order('c1')
    assert row.state == 'confirmed'
    assert row.filled_qty == 10.0
    # The now-resolved park is dropped (a filled MARKET never re-surfaces in
    # get_open_orders, so it would otherwise be replayed forever).
    assert 'c1' not in broker.store_ctx.replay()[1]


def __test_translate_external_fill_without_coid_still_dropped__(tmp_path):
    # A fill that reverse-maps to no row this run placed (no coid echo, no ref)
    # stays external — the coid fallback must not weaken the external-drop policy.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    order = _make_order(order_id=999, executed=1000, position_id=888,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    out = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order))
    assert out is None


def __test_translate_closing_fill_with_coid_not_recovered_as_entry__(tmp_path):
    # The closingOrder guard keeps a coid match from ever reclassifying a close
    # fill as an entry (a close carries no ProtoOAClosePositionReq clientOrderId,
    # but lock the guard regardless).
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    create_entry_order_row(
        broker.store_ctx, coid='c2', symbol='EURUSD', side='buy', qty=10.0,
        intent_key='pine2', pine_entry_id='pine2',
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    mark_disposition_unknown(broker.store_ctx, coid='c2')
    order = _make_order(order_id=222, executed=1000, position_id=333,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
                        closing=True)
    order.clientOrderId = 'c2'
    out = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order))
    assert out is None
    assert broker.store_ctx.find_by_ref('order_id', '222') is None


# === state mapping ========================================================

def __test_get_open_orders_maps_working_order__():
    res = _oa.ProtoOAReconcileRes()
    res.order.append(_make_order(order_id=42, order_type=_model.ProtoOAOrderType.LIMIT,
                                 limit=1.2345, volume=2000))
    broker = _FakeBroker(reconcile=res)
    orders = asyncio.run(broker.get_open_orders())
    assert len(orders) == 1
    o = orders[0]
    assert o.id == "42" and o.symbol == "EURUSD"
    assert o.order_type is OrderType.LIMIT and o.price == 1.2345
    assert o.qty == 20.0


def __test_get_position_returns_single_netting_row__():
    res = _oa.ProtoOAReconcileRes()
    res.position.append(_model.ProtoOAPosition(
        positionId=9, price=1.2,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=3000,
                                          tradeSide=_model.ProtoOATradeSide.SELL),
    ))
    broker = _FakeBroker(reconcile=res)
    pos = asyncio.run(broker.get_position("EURUSD"))
    assert pos is not None
    assert pos.side == "short" and pos.size == 30.0 and pos.entry_price == 1.2


def __test_get_position_flat_returns_none__():
    broker = _FakeBroker(reconcile=_oa.ProtoOAReconcileRes())
    assert asyncio.run(broker.get_position("EURUSD")) is None


# === HEDGED one-way emulation =============================================

def _position(*, position_id, volume, side=_model.ProtoOATradeSide.BUY,
              price=1.20, open_ts=1000):
    return _model.ProtoOAPosition(
        positionId=position_id, price=price,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(
            symbolId=1, volume=volume, tradeSide=side, openTimestamp=open_ts,
        ),
    )


def _hedged_broker(*positions):
    res = _oa.ProtoOAReconcileRes()
    for pos in positions:
        res.position.append(pos)
    broker = _FakeBroker(reconcile=res)
    broker._hedging_enabled = True
    return broker


def __test_hedging_get_position_aggregates_legs__():
    broker = _hedged_broker(
        _position(position_id=1, volume=1000, price=1.10, open_ts=1000),
        _position(position_id=2, volume=3000, price=1.20, open_ts=2000),
    )
    pos = asyncio.run(broker.get_position("EURUSD"))
    assert pos is not None
    assert pos.side == "long" and pos.size == 40.0
    # Volume-weighted: (10*1.10 + 30*1.20) / 40 = 1.175.
    assert pos.entry_price == 1.175


def __test_fetch_raw_positions_returns_legs_oldest_first__():
    # Raw legs (no aggregation), oldest open_time first — the surface the core
    # one-way emulator nets / FIFO-selects over.
    broker = _hedged_broker(
        _position(position_id=2, volume=1000, open_ts=2000),  # newer
        _position(position_id=1, volume=3000, open_ts=1000),  # older
    )
    legs = asyncio.run(broker.fetch_raw_positions("EURUSD"))
    assert [leg.leg_id for leg in legs] == ["1", "2"]
    assert legs[0].qty == 30.0 and legs[1].qty == 10.0
    assert all(leg.side == "buy" for leg in legs)


# === PositionPort transport primitives (one-way emulation) ================
#
# The FIFO / reversal / bracket-replication LOGIC lives in core
# (``OneWayEmulator``, test_039); these verify only the plugin's per-entity
# cTrader wire shapes — the primitive bodies the core emulator drives through
# the ``PositionPort`` once a HEDGED account opts in.


def __test_close_leg_builds_close_request__():
    broker = _FakeBroker()
    asyncio.run(broker.close_leg("EURUSD", "7", 2000, "coid-c"))
    closes = [r for r in broker.sent if isinstance(r, _oa.ProtoOAClosePositionReq)]
    assert len(closes) == 1
    assert closes[0].positionId == 7 and closes[0].volume == 2000


def __test_place_leg_builds_new_order_request__():
    broker = _FakeBroker()
    intent = EntryIntent(pine_id="L", symbol="EURUSD", side="buy", qty=10.0,
                         order_type=OrderType.MARKET)
    orders = asyncio.run(broker.place_leg(_envelope(intent), 10.0))
    opens = [r for r in broker.sent if isinstance(r, _oa.ProtoOANewOrderReq)]
    assert len(opens) == 1
    assert opens[0].tradeSide == _model.ProtoOATradeSide.BUY
    assert opens[0].volume == 1000
    assert len(orders) == 1


def __test_amend_bracket_builds_sltp_request__():
    broker = _FakeBroker()
    asyncio.run(broker.amend_bracket(
        "EURUSD", "3", side="sell", tp_price=1.30, sl_price=1.10,
        trail_offset=None, coid="coid-b",
    ))
    amends = [r for r in broker.sent if isinstance(r, _oa.ProtoOAAmendPositionSLTPReq)]
    assert len(amends) == 1
    assert amends[0].positionId == 3
    assert amends[0].stopLoss == 1.1 and amends[0].takeProfit == 1.3


def __test_amend_bracket_all_none_clears_protection__():
    # cTrader overwrites the whole protection set, so an all-None amend carries
    # no SL/TP/trailing fields and clears the bracket wholesale.
    broker = _FakeBroker()
    asyncio.run(broker.amend_bracket(
        "EURUSD", "3", side="sell", tp_price=None, sl_price=None,
        trail_offset=None, coid="coid-clear",
    ))
    amends = [r for r in broker.sent if isinstance(r, _oa.ProtoOAAmendPositionSLTPReq)]
    assert len(amends) == 1
    set_fields = {f.name for f, _ in amends[0].ListFields()}
    assert set_fields.isdisjoint({"stopLoss", "takeProfit", "trailingStopLoss"})


def __test_reject_out_of_range_below_min_skips__():
    # 5 units -> 500 centi, below the 1000 minVolume -> non-halting skip.
    broker = _FakeBroker()
    intent = EntryIntent(pine_id="L", symbol="EURUSD", side="buy", qty=5.0,
                         order_type=OrderType.MARKET)
    with pytest.raises(OrderSkippedByPlugin):
        asyncio.run(broker.reject_out_of_range(_envelope(intent), 5.0))


def __test_get_volume_quantizer_snaps_to_step__():
    broker = _FakeBroker()
    quantize = asyncio.run(broker.get_volume_quantizer("EURUSD"))
    assert quantize(10.0) == 1000  # 1000 centi already on the 1000 step
    assert quantize(15.0) == 2000  # 1500 centi snapped up to 2000


# === error taxonomy =======================================================

def __test_map_error_code_margin__():
    from pynecore.core.broker.exceptions import InsufficientMarginError
    assert isinstance(map_error_code('NOT_ENOUGH_MONEY'), InsufficientMarginError)


def __test_map_error_code_rate_limit__():
    from pynecore.core.broker.exceptions import ExchangeRateLimitError
    assert isinstance(map_error_code('REQUEST_FREQUENCY_EXCEEDED'),
                      ExchangeRateLimitError)


def __test_map_error_code_generic_reject__():
    from pynecore.core.broker.exceptions import ExchangeOrderRejectedError
    err = map_error_code('SOMETHING_ELSE', 'bad order')
    assert isinstance(err, ExchangeOrderRejectedError)


# === Cancel disposition idempotency (2.6) =================================
# The engine's cancel-tentative state machine drives a working order's cancel
# to resolution by RE-invoking execute_cancel_with_outcome each reconcile
# cycle. Its M3 plugin contract is idempotency: re-cancelling an already
# gone / already filled order must be a benign UNKNOWN (keep the tentative
# armed), never an exception and never a false confirmed no-fill cancel.

def _seed_working_entry(broker, *, pine_id='Long', order_id=111):
    broker.store_ctx.upsert_order(
        f'coid-{order_id}', symbol='EURUSD', side='buy', qty=10.0, filled_qty=0.0,
        state='confirmed', pine_entry_id=pine_id, exchange_order_id=str(order_id),
        extras={'order_id': str(order_id), 'position_id': None},
    )
    broker.store_ctx.add_ref(f'coid-{order_id}', 'order_id', str(order_id))


def _cancel(broker, *, pine_id='Long'):
    intent = CancelIntent(pine_id=pine_id, symbol='EURUSD')
    return asyncio.run(broker.execute_cancel_with_outcome(_envelope(intent)))


def __test_cancel_confirmed_when_broker_cancels__(tmp_path):
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_working_entry(broker)
    broker._canned_event = _exec_event(_model.ProtoOAExecutionType.ORDER_CANCELLED)
    assert _cancel(broker) is CancelDispositionOutcome.CANCEL_CONFIRMED


def __test_cancel_already_filled_when_broker_fills__(tmp_path):
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_working_entry(broker)
    broker._canned_event = _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED)
    assert _cancel(broker) is CancelDispositionOutcome.ALREADY_FILLED


def __test_cancel_rejected_race_is_unknown_not_too_late__(tmp_path):
    # ORDER_CANCEL_REJECTED is a cancel/modify race: the order may still be
    # live or may have filled, so it maps to UNKNOWN (the cancel-tentative
    # stays armed) — never a confirmed no-fill cancel that could double-open.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_working_entry(broker)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_CANCEL_REJECTED)
    assert _cancel(broker) is CancelDispositionOutcome.UNKNOWN


def __test_cancel_not_found_is_unknown_not_raise__(tmp_path):
    # Re-cancelling an already-gone order: a NOT_FOUND reject is a benign
    # idempotent no-op (UNKNOWN keeps the tentative armed for a later fill /
    # cancel signal), NOT an exception that would halt the retry loop.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_working_entry(broker)
    err = ExchangeOrderRejectedError("order gone")
    err.__cause__ = CTraderProtocolError('ORDER_NOT_FOUND', '')
    broker._raise_on_dispatch = err
    assert _cancel(broker) is CancelDispositionOutcome.UNKNOWN


def __test_cancel_no_live_order_is_unknown_without_dispatch__(tmp_path):
    # No live working row for the pine id (already cancelled / filled): the
    # cancel is a no-op UNKNOWN and no cancel request is sent.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    assert _cancel(broker) is CancelDispositionOutcome.UNKNOWN
    assert broker.sent == []
