"""
@pyne
"""
import asyncio

import pytest

from pynecore.core.broker.exceptions import OrderSkippedByPlugin
from pynecore.core.broker.models import (
    CapabilityLevel,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExitIntent,
    LegType,
    OrderStatus,
    OrderType,
)

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.exceptions import map_error_code
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

    async def _get_symbol_rules(self, symbol: str) -> _SymbolRules:
        return _RULES

    async def _dispatch_order(self, req, *, coid, context):
        self.sent.append(req)
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
