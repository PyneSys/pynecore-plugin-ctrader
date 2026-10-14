"""
@pyne
"""
import asyncio
import time

import pytest

from pynecore.core.broker.exceptions import (
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
    UnexpectedCancelError,
    VenueWritesQuarantinedError,
)
from pynecore.core.broker.models import (
    BracketAttachRejectContext,
    CancelDispositionOutcome,
    CancelIntent,
    CapabilityLevel,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExitIntent,
    LegType,
    OrderType,
    QuarantineKind,
)
from pynecore.core.broker.native_failsafe_manager import NativeBracketSnapshot
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    create_bracket_ownership_row,
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
from pynecore_ctrader.wire import WireClient

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

    async def _dispatch_order(
            self, req, *, coid, context, predecessor_cancel_ids=None,
    ):
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


def __test_quantize_volume_never_rounds_nonzero_to_zero__():
    """A nonzero request below step/2 snaps UP to one step, never to 0.

    Reachable only on a venue quoting ``minVolume < stepVolume / 2`` (the
    min/max gate runs on the raw centi-units before quantization); a
    zero-volume ``ProtoOANewOrderReq`` must never be emitted.
    """
    # 4 units -> 400 centi, below step/2 of 1000 -> snapped up, not to 0.
    assert quantize_volume(4.0, 1000) == 1000
    # A genuinely zero request stays zero.
    assert quantize_volume(0.0, 1000) == 0


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

def __test_pre_write_guard_with_connection_down_raises_engine_connection_error__():
    # Regression (ctrader cycle 60, post-sleep offline window): with the live
    # connection down (_wire = None) the dispatch pre-write reads — symbol
    # rules, the open-position lookup via _reconcile, the state symbol-id
    # resolve and get_balance — used to raise the raw
    # CTraderConnectionError("live connection not established"). Escaping an
    # order dispatch untranslated, the sync engine's write bridge classified it
    # as an unrecoverable fault and halted the run with
    # BrokerManualInterventionError. No request was sent at these guards, so
    # they must surface the parkable ExchangeConnectionError instead.
    broker = CTrader(symbol=None, config=_make_config())
    broker._wire = None
    broker._live_account_id = None
    intent = ExitIntent(pine_id="Exit", from_entry="Long", symbol="EURUSD",
                        side="sell", qty=10.0, tp_price=1.30, sl_price=1.10)
    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker.execute_exit(_envelope(intent)))
    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker._reconcile())
    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker._resolve_state_symbol_id("EURUSD"))
    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker.get_balance())


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


def _open_eurusd_position(position_id=777, volume=1000):
    res = _oa.ProtoOAReconcileRes()
    res.position.append(_model.ProtoOAPosition(
        positionId=position_id,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=volume,
                                          tradeSide=_model.ProtoOATradeSide.BUY),
    ))
    return res


def _position_not_found_reject() -> ExchangeOrderRejectedError:
    reject = ExchangeOrderRejectedError(
        "cTrader rejected the order (POSITION_NOT_FOUND)")
    reject.__cause__ = CTraderProtocolError('POSITION_NOT_FOUND', '')
    return reject


def __test_close_racing_the_native_failsafe_is_a_non_halting_skip__():
    # The native fail-safe SL took the whole position in the same tick the
    # software SL-partial close dispatched: the venue answers the close with
    # POSITION_NOT_FOUND. The book is flat there — surface "nothing to close"
    # instead of the fatal close reject (measured live, ctrader cycle 103).
    broker = _FakeBroker(reconcile=_open_eurusd_position())
    broker._raise_on_dispatch = _position_not_found_reject()
    intent = CloseIntent(
        pine_id="__pyne_partial_trigger__L-X1\0L\0sl_partial", symbol="EURUSD",
        side="sell", qty=5.0, synthetic_kind='partial_trigger', target_entry_id='L',
    )
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(broker.execute_close(_envelope(intent)))
    assert exc.value.reason == 'nothing_to_close'
    assert 'POSITION_NOT_FOUND' in str(exc.value)
    assert isinstance(broker.sent[0], _oa.ProtoOAClosePositionReq)


def _position_locked_reject() -> ExchangeOrderRejectedError:
    reject = ExchangeOrderRejectedError(
        "cTrader rejected the order (POSITION_LOCKED)")
    reject.__cause__ = CTraderProtocolError('POSITION_LOCKED', '')
    return reject


def __test_close_racing_the_native_failsafe_fill_in_flight_is_a_non_halting_skip__():
    # The same race one step earlier: the fail-safe fill is still executing,
    # so the venue answers POSITION_LOCKED rather than not-found (measured
    # live, ctrader cycle 116). The position may still hold size — surface a
    # distinct non-halting skip so the leg re-arms against the next snapshot.
    broker = _FakeBroker(reconcile=_open_eurusd_position())
    broker._raise_on_dispatch = _position_locked_reject()
    intent = CloseIntent(
        pine_id="__pyne_partial_trigger__L-X1\0L\0sl_partial", symbol="EURUSD",
        side="sell", qty=5.0, synthetic_kind='partial_trigger', target_entry_id='L',
    )
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(broker.execute_close(_envelope(intent)))
    assert exc.value.reason == 'position_locked'
    assert 'POSITION_LOCKED' in str(exc.value)
    assert isinstance(broker.sent[0], _oa.ProtoOAClosePositionReq)
    broker = _FakeBroker(reconcile=_open_eurusd_position())
    broker._raise_on_dispatch = _position_locked_reject()
    intent = CloseIntent(pine_id="Long", symbol="EURUSD", side="sell", qty=10.0)
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(broker.execute_close(_envelope(intent)))
    assert exc.value.reason == 'position_locked'


def __test_defensive_close_on_locked_position_keeps_the_reject__():
    broker = _FakeBroker(reconcile=_open_eurusd_position())
    broker._raise_on_dispatch = _position_locked_reject()
    intent = CloseIntent(
        pine_id="__pyne_defensive_close__L", symbol="EURUSD", side="sell",
        qty=10.0, synthetic_kind='defensive_close', target_position_coid='coid-L',
    )
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.execute_close(_envelope(intent)))


def __test_script_close_on_vanished_position_is_a_non_halting_skip__():
    broker = _FakeBroker(reconcile=_open_eurusd_position())
    broker._raise_on_dispatch = _position_not_found_reject()
    intent = CloseIntent(pine_id="Long", symbol="EURUSD", side="sell", qty=10.0)
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(broker.execute_close(_envelope(intent)))
    assert exc.value.reason == 'nothing_to_close'


def __test_script_close_without_open_position_is_a_non_halting_skip__():
    # No open position on the symbol at lookup time: same "already satisfied"
    # outcome, no request goes to the wire.
    broker = _FakeBroker(reconcile=_oa.ProtoOAReconcileRes())
    intent = CloseIntent(pine_id="Long", symbol="EURUSD", side="sell", qty=10.0)
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(broker.execute_close(_envelope(intent)))
    assert exc.value.reason == 'nothing_to_close'
    assert broker.sent == []


def __test_defensive_close_on_vanished_position_keeps_the_reject__():
    # Defensive / reversal closes have their own recovery contracts in the
    # engine — the not-found race stays a loud reject for them.
    broker = _FakeBroker(reconcile=_open_eurusd_position())
    broker._raise_on_dispatch = _position_not_found_reject()
    intent = CloseIntent(
        pine_id="__pyne_defensive_close__L", symbol="EURUSD", side="sell",
        qty=10.0, synthetic_kind='defensive_close', target_position_coid='coid-L',
    )
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.execute_close(_envelope(intent)))
    broker = _FakeBroker(reconcile=_oa.ProtoOAReconcileRes())
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.execute_close(_envelope(intent)))


def __test_close_reject_for_another_reason_propagates__():
    broker = _FakeBroker(reconcile=_open_eurusd_position())
    reject = ExchangeOrderRejectedError("cTrader rejected the order (SOMETHING_ELSE)")
    reject.__cause__ = CTraderProtocolError('SOMETHING_ELSE', '')
    broker._raise_on_dispatch = reject
    intent = CloseIntent(pine_id="Long", symbol="EURUSD", side="sell", qty=10.0)
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.execute_close(_envelope(intent)))


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
    # The broker-native dealId is stamped as the canonical fill_id so the
    # engine's duplicate-fill gate can drop a redelivered copy of this fill.
    assert out.fill_id == "5"


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


def __test_translate_never_resurrects_a_closed_row_by_coid__(tmp_path):
    """A coid echo on a row the journal already CLOSED is external, not a recovery.

    The reported rotation incident: a predecessor instance's resting STOP entry
    kept working at the venue under a coid whose journal row had since been
    reused, filled and closed by a later entry cycle. When the STOP finally
    fired, the coid matched the closed row, the recovery re-confirmed it and the
    engine booked the fill onto a position it no longer managed. A closed row
    proves the cycle is over — the fill is dropped as external activity and the
    reconcile verdict surfaces the unmanaged leg instead.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    create_entry_order_row(
        broker.store_ctx, coid='c1', symbol='EURUSD', side='buy', qty=10.0,
        intent_key='pine1', pine_entry_id='pine1',
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    broker.store_ctx.upsert_order('c1', state='confirmed', filled_qty=10.0)
    broker.store_ctx.close_order('c1')

    order = _make_order(order_id=111, executed=1000, position_id=222,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    order.clientOrderId = 'c1'
    deal = _model.ProtoOADeal(dealId=5, filledVolume=1000, executionPrice=1.2345)
    out = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order, deal=deal))

    assert out is None
    assert len(_ignored_external(broker)) == 1
    # The closed row stays closed: no ref backfill, no state rewrite.
    assert broker.store_ctx.find_by_ref('order_id', '111') is None
    assert broker.store_ctx.get_order('c1').closed_ts_ms is not None


def __test_translate_suppresses_push_already_counted_by_reconcile__(tmp_path):
    """A PUSH entry fill whose cumulative the reconcile working-row path already
    counted (durable cursor ahead) is suppressed — not re-applied — while later
    genuine progress still emits.

    The reconcile working-row path advances the filled_qty cursor from
    order.executedVolume but cannot enumerate deals, so it never seeds
    _seen_deal_ids; a delayed PUSH for that same slice would otherwise double-
    apply in record_fill. The cursor snapshot at event entry catches it.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    create_entry_order_row(
        broker.store_ctx, coid='c1', symbol='EURUSD', side='buy', qty=10.0,
        intent_key='pine1', pine_entry_id='pine1',
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    # Reconcile already counted 6 units of the entry: order_id ref + cursor at 6.
    broker.store_ctx.add_ref('c1', 'order_id', '111')
    broker.store_ctx.set_filled('c1', 6.0)

    # Delayed PUSH for that same 6-unit progress (executedVolume=600 == cursor).
    order = _make_order(order_id=111, executed=600, position_id=222,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED)
    deal = _model.ProtoOADeal(dealId=7, filledVolume=600, executionPrice=1.2345)
    out = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_PARTIAL_FILL, order=order, deal=deal))
    assert out is None  # suppressed — reconcile already counted this cumulative
    assert 7 in broker._seen_deal_ids  # recorded so a later PUSH replay is caught too

    # A genuinely new slice (cumulative 6 -> 10) still emits its delta.
    order2 = _make_order(order_id=111, executed=1000, position_id=222,
                         status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    deal2 = _model.ProtoOADeal(dealId=8, filledVolume=400, executionPrice=1.2360)
    out2 = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order2, deal=deal2))
    assert out2 is not None
    assert out2.event_type == 'filled'
    assert out2.fill_qty == 4.0
    assert out2.fill_id == "8"


def __test_translate_malformed_push_does_not_burn_dealid__(tmp_path):
    """A malformed PUSH (no executionPrice, which record_fill ignores) must not
    burn its dealId or advance the cursor, so a corrected redelivery carrying the
    same dealId still applies.

    The plugin records a dealId and touches the fill cursor only for a fill it
    will actually apply (mirrors the engine's _is_duplicate_fill gate).
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    create_entry_order_row(
        broker.store_ctx, coid='c1', symbol='EURUSD', side='buy', qty=10.0,
        intent_key='pine1', pine_entry_id='pine1',
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    broker.store_ctx.add_ref('c1', 'order_id', '111')

    order = _make_order(order_id=111, executed=1000, position_id=222,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    # Malformed: dealId present but no execution price -> record_fill ignores it.
    bad_deal = _model.ProtoOADeal(dealId=9, filledVolume=1000, executionPrice=0.0)
    broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order, deal=bad_deal))
    assert 9 not in broker._seen_deal_ids
    assert broker.store_ctx.get_order('c1').filled_qty == 0.0  # cursor not advanced

    # Corrected redelivery with the SAME dealId -> applied (not dropped/suppressed).
    good_deal = _model.ProtoOADeal(dealId=9, filledVolume=1000, executionPrice=1.2345)
    out = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order, deal=good_deal))
    assert out is not None
    assert out.event_type == 'filled'
    assert out.fill_qty == 10.0
    assert out.fill_id == "9"


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


def __test_translate_partial_close_fill_foreign_coid_persists_no_row__(tmp_path):
    """A partial-close fill with a venue-supplied foreign coid must not upsert a row.

    The venue's close order carries its own ``clientOrderId`` (observed live as
    ``'27'``) that is not one of ours. The position-linking step used to pass it
    to ``upsert_order``, which raised ``ValueError`` on the under-specified new
    row and killed ``watch_orders`` mid-campaign. Linking is entry-row-only:
    the close fill must translate to a CLOSE leg for its entry and leave the
    store without a foreign row.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    create_entry_order_row(
        broker.store_ctx, coid='c1', symbol='EURUSD', side='buy', qty=20.0,
        intent_key='pineL', pine_entry_id='pineL',
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    broker.store_ctx.add_ref('c1', 'order_id', '111')
    broker.store_ctx.add_ref('c1', 'position_id', '52695200')
    # A self-close always registers the dispatch record (execute_close /
    # close_leg). This is the run-owned handle that attributes the close fill;
    # the shared ``position_id`` ref alone is deliberately NOT enough (it is
    # present in every run that entered the netted position — see
    # ``_resolve_identity`` run-ownership isolation). The own-close record
    # marks the fill as this run's, not a manual close.
    broker._close_dispatch_pine_by_position[52695200] = 'pineL'
    broker._open_own_close(52695200)

    order = _make_order(order_id=777, executed=1000, position_id=52695200,
                        closing=True,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    order.clientOrderId = '27'
    deal = _model.ProtoOADeal(dealId=42, filledVolume=1000, executionPrice=1.1)
    ev = _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order,
                     deal=deal)
    # The post-execution snapshot reports the position still OPEN (partial
    # close: 20-unit entry reduced by 10).
    ev.position.CopyFrom(_model.ProtoOAPosition(
        positionId=52695200,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=1000,
                                          tradeSide=_model.ProtoOATradeSide.BUY),
    ))

    out = broker._translate_exec_event(ev)

    assert out is not None
    assert out.event_type == 'filled'
    assert out.leg_type is LegType.CLOSE
    assert out.from_entry == 'pineL'
    assert out.fill_qty == 10.0
    # The foreign coid never became a store row, and the entry row survived.
    assert broker.store_ctx.get_order('27') is None
    assert broker.store_ctx.get_order('c1') is not None
    assert broker._deferred_interventions == []


def __test_translate_close_fill_on_adopted_position_maps_via_dispatch_record__(tmp_path):
    """A close fill on a startup-adopted position maps through the dispatch record.

    No entry row of this run links the ``positionId`` (the position was opened
    by a prior process and adopted), so the ref index misses and the fill of
    our OWN close used to be dropped as external activity — the strategy never
    observed its flatten. ``execute_close`` records the position's Pine id so
    ``_resolve_identity`` can attribute the fill as a CLOSE leg.
    """
    res = _oa.ProtoOAReconcileRes()
    res.position.append(_model.ProtoOAPosition(
        positionId=777,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=1000,
                                          tradeSide=_model.ProtoOATradeSide.BUY),
    ))
    broker = _FakeBroker(reconcile=res)
    _open_store(tmp_path, broker)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=888, executed=1000, price=1.25,
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
    )
    intent = CloseIntent(pine_id="Long", symbol="EURUSD", side="sell", qty=10.0)
    asyncio.run(broker.execute_close(_envelope(intent)))

    # The PUSH copy of the close fill: closingOrder, no refs for its ids.
    order = _make_order(order_id=888, executed=1000, position_id=777,
                        closing=True,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    deal = _model.ProtoOADeal(dealId=77, filledVolume=1000, executionPrice=1.25)
    out = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order,
                    deal=deal))

    assert out is not None
    assert out.event_type == 'filled'
    assert out.leg_type is LegType.CLOSE
    assert out.from_entry == 'Long'
    assert out.fill_qty == 10.0


def __test_translate_foreign_run_entry_fill_on_shared_position_dropped__(tmp_path):
    """A concurrent run's entry fill on the SHARED netted positionId is dropped.

    On a one-way account both runs attach their entries to the same venue
    ``positionId`` and each records a ``position_id`` ref for it. This run's
    PUSH stream also sees the OTHER run's entry fill (a different, un-journaled
    ``orderId`` but the same shared ``positionId``). Attributing it through the
    shared ``position_id`` ref would grow this run's position past its own
    slice. Only the run-unique ``order_id`` may attribute an entry, so the
    foreign fill is dropped as external activity.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    create_entry_order_row(
        broker.store_ctx, coid='c1', symbol='EURUSD', side='buy', qty=10.0,
        intent_key='pineL', pine_entry_id='pineL',
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    # This run's own entry: order 111 -> shared position 999.
    broker.store_ctx.add_ref('c1', 'order_id', '111')
    broker.store_ctx.add_ref('c1', 'position_id', '999')

    # The other run's entry fill: a DIFFERENT orderId, the SAME shared position.
    order = _make_order(order_id=222, executed=1000, position_id=999,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    deal = _model.ProtoOADeal(dealId=7, filledVolume=1000, executionPrice=1.2)
    out = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order,
                    deal=deal))
    assert out is None


def __test_translate_foreign_run_close_fill_on_shared_position_dropped__(tmp_path):
    """A concurrent run's close fill on the SHARED netted positionId is dropped.

    This run entered the netted position (so it holds a ``position_id`` ref) but
    did NOT dispatch this close. The other run's close of the shared net reaches
    this run's PUSH stream as a ``closingOrder`` fill on the shared
    ``positionId``. Booking it through the shared ``position_id`` ref would
    record a phantom exit against this run's still-open slice; only a close THIS
    run dispatched (``_close_dispatch_pine_by_position``) is ours.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    create_entry_order_row(
        broker.store_ctx, coid='c1', symbol='EURUSD', side='buy', qty=10.0,
        intent_key='pineL', pine_entry_id='pineL',
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    broker.store_ctx.add_ref('c1', 'order_id', '111')
    broker.store_ctx.add_ref('c1', 'position_id', '999')
    # This run dispatched NO close -> _close_dispatch_pine_by_position is empty.

    order = _make_order(order_id=888, executed=1000, position_id=999,
                        closing=True,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    deal = _model.ProtoOADeal(dealId=8, filledVolume=1000, executionPrice=1.2)
    out = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order,
                    deal=deal))
    assert out is None
    # This run's own entry row is untouched by the foreign close.
    assert broker.store_ctx.get_order('c1') is not None


def _seed_owned_entry(broker, *, coid='c1', pine_id='L', position_id=54302872,
                      order_id='70211174'):
    """Seed a filled entry row of this run holding ``position_id``."""
    create_entry_order_row(
        broker.store_ctx, coid=coid, symbol='EURUSD', side='buy', qty=10.0,
        intent_key=pine_id, pine_entry_id=pine_id,
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    broker.store_ctx.upsert_order(
        coid, state='confirmed', filled_qty=10.0,
        exchange_order_id=str(position_id),
        extras={'kind': 'position', 'order_type': 'market',
                'order_id': order_id, 'position_id': position_id},
    )
    broker.store_ctx.add_ref(coid, 'order_id', order_id)


def _venue_bracket_close_event(*, position_id, order_id=70215164):
    """A filled closing execution fired by the venue's own SL/TP bracket."""
    order = _make_order(order_id=order_id, executed=1000,
                        position_id=position_id, closing=True,
                        order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    deal = _model.ProtoOADeal(dealId=60775467, filledVolume=1000,
                             executionPrice=1.1)
    return _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order,
                       deal=deal)


def _ignored_external(broker) -> list:
    return list(broker.store_ctx.iter_events_by_kind_since(
        'external_activity_ignored', 0))


def __test_venue_bracket_close_attributes_to_owning_entry__(tmp_path):
    """The venue's own SL/TP fill on a run-owned position is NOT external.

    Neither run-unique handle matches: the closing order's ``orderId`` is the
    venue's (never journaled) and this run dispatched no close, so the fill used
    to be dropped as external activity while the engine's book kept the stale
    open size for the rest of the cycle. The live bracket-ownership row (and the
    entry row's own ``position_id`` mirror) proves the protection that fired is
    ours, so the fill must surface as that entry's CLOSE leg.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    create_bracket_ownership_row(
        broker.store_ctx, coid='bo:L\0L:54302872', symbol='EURUSD',
        side='sell', qty=10.0, intent_key='L\0L', pine_entry_id='exitL',
        from_entry='L', leg_id='54302872', attach_coid='attach-1',
        tp_price=None, sl_price=1.05, trail_price=None, trail_offset=None,
    )

    out = broker._translate_exec_event(
        _venue_bracket_close_event(position_id=54302872))

    assert out is not None
    assert out.event_type == 'filled'
    assert out.leg_type is LegType.CLOSE
    assert out.from_entry == 'L'
    assert out.pine_id is None
    assert out.fill_qty == 10.0
    assert _ignored_external(broker) == []


def __test_venue_bracket_close_attributes_via_entry_position_mirror__(tmp_path):
    """A NETTING account has no ownership row — the entry mirror attributes it.

    cTrader protective levels are position attributes, so a netting run never
    writes a per-leg ownership row (those come from the core one-way emulator on
    a hedging account). The live entry row's ``extras['position_id']`` is then
    the run's exposure proof.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)

    out = broker._translate_exec_event(
        _venue_bracket_close_event(position_id=54302872))

    assert out is not None
    assert out.leg_type is LegType.CLOSE
    assert out.from_entry == 'L'
    assert _ignored_external(broker) == []


def __test_venue_close_on_unowned_position_stays_external__(tmp_path):
    """A closing fill on a position this run does not own is still external.

    Same shape as the attributed case, but the ``positionId`` matches neither an
    ownership row nor any live entry row of this run (a manual trade or another
    bot on the account). Attributing it would corrupt this strategy's position,
    so it must be dropped and audited.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)

    out = broker._translate_exec_event(
        _venue_bracket_close_event(position_id=99999999))

    assert out is None
    ignored = _ignored_external(broker)
    assert len(ignored) == 1
    assert ignored[0]['position_id'] == 99999999


def _partial_close_event(*, position_id, deal_id, closed_cents, remaining_cents,
                         order_id=70215164):
    """A run-owned close fill that leaves the position OPEN with a remainder."""
    order = _make_order(order_id=order_id, executed=closed_cents,
                        position_id=position_id, closing=True,
                        order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    deal = _model.ProtoOADeal(dealId=deal_id, filledVolume=closed_cents,
                              executionPrice=1.1)
    ev = _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order,
                     deal=deal)
    ev.position.CopyFrom(_model.ProtoOAPosition(
        positionId=position_id,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(
            symbolId=1, volume=remaining_cents,
            tradeSide=_model.ProtoOATradeSide.BUY),
    ))
    return ev


def __test_partial_close_fill_retires_exposure_not_the_watermark__(tmp_path):
    """A run-owned partial close books its quantity as retired exposure.

    The close executes under the venue's close ``orderId`` (never a journal
    row of ours), so before the fix NOTHING in the durable book recorded that
    the position shrank: the entry row kept its full ``filled_qty`` and the
    cycle-end venue-vs-journal audit read 10.0 owned against a 6.0 venue
    remainder. The ``filled_qty`` watermark must stay untouched (it is the
    de-dup cursor the PUSH / reconcile paths diff ``executedVolume`` against);
    the shrink lands in the ``journal_exposure_retired`` extras counter, and
    consecutive partials accumulate it.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)

    out = broker._translate_exec_event(_partial_close_event(
        position_id=54302872, deal_id=61, closed_cents=400,
        remaining_cents=600))

    assert out is not None
    assert out.leg_type is LegType.CLOSE
    assert out.fill_qty == 4.0
    row = broker.store_ctx.get_order('c1')
    assert row is not None
    assert row.closed_ts_ms is None, "a partial close must keep the entry live"
    assert row.filled_qty == 10.0, "the execution watermark must not move"
    assert (row.extras or {})['journal_exposure_retired'] == pytest.approx(4.0)

    # A second partial (its own deal) accumulates onto the same counter.
    out = broker._translate_exec_event(_partial_close_event(
        position_id=54302872, deal_id=62, closed_cents=300,
        remaining_cents=300, order_id=70215165))
    assert out is not None
    row = broker.store_ctx.get_order('c1')
    assert row.filled_qty == 10.0
    assert (row.extras or {})['journal_exposure_retired'] == pytest.approx(7.0)


def __test_partial_close_retire_caps_at_the_rows_filled_exposure__(tmp_path):
    """An overshooting close clamps the counter at the row's own fill.

    The venue can report a closing volume larger than what the journal
    attributes to this run (another run's slice folded into the same netted
    position). The counter must saturate at ``filled_qty`` — a negative owned
    contribution would flip the row's sign in the ownership sum.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)

    out = broker._translate_exec_event(_partial_close_event(
        position_id=54302872, deal_id=63, closed_cents=1500,
        remaining_cents=500))

    assert out is not None
    row = broker.store_ctx.get_order('c1')
    assert (row.extras or {})['journal_exposure_retired'] == pytest.approx(10.0)


def __test_partial_close_retire_reduces_pyramid_rows_fifo__(tmp_path):
    """With pyramid rows on one netted position the oldest absorbs first."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker, coid='c1', pine_id='L1')
    _seed_owned_entry(broker, coid='c2', pine_id='L2', order_id='70211175')
    broker.store_ctx.upsert_order('c1', filled_qty=4.0)
    broker.store_ctx.upsert_order('c2', filled_qty=6.0)

    out = broker._translate_exec_event(_partial_close_event(
        position_id=54302872, deal_id=64, closed_cents=500,
        remaining_cents=500))

    assert out is not None
    first = broker.store_ctx.get_order('c1')
    second = broker.store_ctx.get_order('c2')
    assert (first.extras or {})['journal_exposure_retired'] == pytest.approx(4.0)
    assert (second.extras or {})['journal_exposure_retired'] == pytest.approx(1.0)
    assert first.filled_qty == 4.0 and second.filled_qty == 6.0


def __test_full_close_still_retires_the_rows_wholesale__(tmp_path):
    """A flat close keeps taking the ``_mark_position_closed`` branch."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)

    out = broker._translate_exec_event(
        _venue_bracket_close_event(position_id=54302872))

    assert out is not None
    row = broker.store_ctx.get_order('c1')
    assert row.closed_ts_ms is not None, "a flat close must retire the row"
    assert 'journal_exposure_retired' not in (row.extras or {})


def __test_watch_orders_survives_translation_failure__():
    """One poisonous PUSH message must not terminate the order-event stream.

    A translation error strands the live strategy on an open position if it
    tears ``watch_orders`` down; the stream must log, drop the message and
    keep serving later events (the reconcile pass gap-fills the dropped one).
    """
    broker = _FakeBroker()
    poison = _exec_event(_model.ProtoOAExecutionType.ORDER_ACCEPTED)
    good = _exec_event(_model.ProtoOAExecutionType.ORDER_ACCEPTED)

    orig = broker._translate_exec_event

    def flaky(message):
        if message is poison:
            raise RuntimeError("translation boom")
        return orig(message)

    broker._translate_exec_event = flaky

    async def run():
        broker._exec_events = asyncio.Queue()
        broker._exec_events.put_nowait(poison)
        broker._exec_events.put_nowait(good)
        agen = broker.watch_orders()
        try:
            return await asyncio.wait_for(agen.__anext__(), timeout=5.0)
        finally:
            await agen.aclose()

    event = asyncio.run(run())
    # The poison message was skipped; the following event still surfaced.
    assert event is not None
    assert event.event_type == 'created'


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


def _own_entry_row(broker, *, coid, position_id, qty=10.0):
    """Seed a live entry row owning ``position_id`` (as a fill would)."""
    create_entry_order_row(
        broker.store_ctx, coid=coid, symbol='EURUSD', side='buy', qty=qty,
        intent_key=coid, pine_entry_id=coid,
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    broker.store_ctx.upsert_order(
        coid, state='confirmed', filled_qty=qty,
        exchange_order_id=str(position_id),
        extras={'kind': 'position', 'order_type': 'market',
                'position_id': position_id},
    )


def __test_fetch_raw_positions_excludes_foreign_run_leg__(tmp_path):
    # On a shared account+symbol scope the reconcile snapshot carries every
    # run's leg. The one-way emulator's leg source must return ONLY the leg this
    # run's journal owns, or a close / reversal would plan over another run's leg.
    broker = _hedged_broker(
        _position(position_id=1, volume=1000, open_ts=1000),  # this run's
        _position(position_id=2, volume=1000, open_ts=2000),  # another run's
    )
    _open_store(tmp_path, broker)
    _own_entry_row(broker, coid='c1', position_id=1)
    legs = asyncio.run(broker.fetch_raw_positions("EURUSD"))
    assert [leg.leg_id for leg in legs] == ["1"]


def __test_get_position_excludes_foreign_run_leg_netting__(tmp_path):
    # Netting single-position read: a fresh run whose journal owns nothing must
    # NOT adopt a concurrent run's open position on the same account+symbol.
    res = _oa.ProtoOAReconcileRes()
    res.position.append(_position(position_id=2, volume=1000))
    broker = _FakeBroker(reconcile=res)
    _open_store(tmp_path, broker)
    # This run's journal is empty (owns no positionId) -> flat, not adoption.
    assert asyncio.run(broker.get_position("EURUSD")) is None
    # Once it owns position 2, the same read returns it (genuine restart).
    _own_entry_row(broker, coid='c1', position_id=2, qty=10.0)
    pos = asyncio.run(broker.get_position("EURUSD"))
    assert pos is not None and pos.size == 10.0


def __test_get_open_orders_excludes_foreign_run_working_order__(tmp_path):
    # A concurrent run's resting working order on the shared scope must not be
    # returned as one the engine should verify / track.
    res = _oa.ProtoOAReconcileRes()
    res.order.append(_make_order(order_id=42, order_type=_model.ProtoOAOrderType.LIMIT,
                                 limit=1.2345, volume=2000))
    res.order.append(_make_order(order_id=99, order_type=_model.ProtoOAOrderType.LIMIT,
                                 limit=1.2000, volume=1000))
    broker = _FakeBroker(reconcile=res)
    _open_store(tmp_path, broker)
    # This run journaled only order 42 (order_id ref).
    create_entry_order_row(
        broker.store_ctx, coid='c1', symbol='EURUSD', side='buy', qty=20.0,
        intent_key='c1', pine_entry_id='c1',
        kind=ENTRY_KIND_WORKING, order_type='limit',
    )
    broker.store_ctx.add_ref('c1', 'order_id', '42')
    orders = asyncio.run(broker.get_open_orders("EURUSD"))
    assert [o.id for o in orders] == ["42"]


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


def __test_close_leg_racing_the_native_failsafe_fill_is_a_plugin_skip__():
    # The one-way fan path of the cycle-116 race (measured live, ctrader cycle
    # 121): the fail-safe fill is still executing on the leg the engine-trigger
    # partial close targets, so the venue answers POSITION_LOCKED. The leg
    # must surface as a plugin skip the core emulator can route, never as a
    # raw reject.
    broker = _FakeBroker()
    broker._raise_on_dispatch = _position_locked_reject()
    with pytest.raises(OrderSkippedByPlugin) as info:
        asyncio.run(broker.close_leg("EURUSD", "7", 2000, "coid-c"))
    assert info.value.reason == 'position_locked'
    assert info.value.intent_key == "coid-c"
    assert info.value.context == {'symbol': "EURUSD", 'leg_id': "7"}
    assert isinstance(info.value.__cause__, ExchangeOrderRejectedError)


def __test_close_leg_on_a_vanished_position_is_a_nothing_to_close_skip__():
    broker = _FakeBroker()
    broker._raise_on_dispatch = _position_not_found_reject()
    with pytest.raises(OrderSkippedByPlugin) as info:
        asyncio.run(broker.close_leg("EURUSD", "7", 2000, "coid-c"))
    assert info.value.reason == 'nothing_to_close'


def __test_close_leg_other_rejects_propagate__():
    broker = _FakeBroker()
    reject = ExchangeOrderRejectedError("cTrader rejected the order (TRADING_DISABLED)")
    reject.__cause__ = CTraderProtocolError('TRADING_DISABLED', '')
    broker._raise_on_dispatch = reject
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(broker.close_leg("EURUSD", "7", 2000, "coid-c"))


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
    # cTrader retains an omitted trailing flag but rejects explicit False
    # without a Stop Loss. Disable with the authenticated anchor, then clear.
    position = _model.ProtoOAPosition(
        positionId=3,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        stopLoss=1.1,
        trailingStopLoss=True,
        tradeData=_model.ProtoOATradeData(
            symbolId=1,
            volume=1000,
            tradeSide=_model.ProtoOATradeSide.BUY,
        ),
    )
    broker = _FakeBroker(reconcile=_oa.ProtoOAReconcileRes(position=[position]))
    asyncio.run(broker.amend_bracket(
        "EURUSD", "3", side="sell", tp_price=None, sl_price=None,
        trail_offset=None, coid="coid-clear",
    ))
    amends = [r for r in broker.sent if isinstance(r, _oa.ProtoOAAmendPositionSLTPReq)]
    assert len(amends) == 2
    assert amends[0].stopLoss == 1.1
    assert amends[0].HasField("trailingStopLoss")
    assert amends[0].trailingStopLoss is False
    set_fields = {f.name for f, _ in amends[1].ListFields()}
    assert set_fields.isdisjoint({"stopLoss", "takeProfit", "trailingStopLoss"})


def __test_amend_bracket_clear_of_a_bare_position_is_an_idempotent_noop__():
    """``INVALID_REQUEST: Nothing to amend.`` on a clear must not raise.

    Live incident (pyramid cycle): the reversal pre-clear raced the
    close-leg fills — the first clear had already stripped the shared
    bracket, and the engine's forced-cancel retry re-drove the clear on
    the now-bare position. The venue rejects an amend with no fields on
    a position with no protection, but that IS the clear's goal state;
    the raw ``ExchangeOrderRejectedError`` escaped the retry loop and
    crashed the run.
    """
    position = _model.ProtoOAPosition(
        positionId=3,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(
            symbolId=1,
            volume=1000,
            tradeSide=_model.ProtoOATradeSide.BUY,
        ),
    )
    broker = _FakeBroker(reconcile=_oa.ProtoOAReconcileRes(position=[position]))
    reject = ExchangeOrderRejectedError(
        "cTrader rejected the order: Nothing to amend.",
    )
    reject.__cause__ = CTraderProtocolError("INVALID_REQUEST", "Nothing to amend.")
    broker._raise_on_dispatch = reject
    asyncio.run(broker.amend_bracket(
        "EURUSD", "3", side="sell", tp_price=None, sl_price=None,
        trail_offset=None, coid="coid-clear-bare",
    ))
    amends = [r for r in broker.sent if isinstance(r, _oa.ProtoOAAmendPositionSLTPReq)]
    assert len(amends) == 1


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
    assert isinstance(map_error_code('BLOCKED_PAYLOAD_TYPE'),
                      ExchangeRateLimitError)


def __test_map_error_code_market_closed__():
    """Session-pause and holiday codes map to the typed market-closed reject."""
    from pynecore.core.broker.exceptions import MarketClosedError
    err = map_error_code('MARKET_CLOSED', 'Trading is not available: Market is closed.')
    assert isinstance(err, MarketClosedError)
    assert 'MARKET_CLOSED' in str(err)
    assert isinstance(map_error_code('SYMBOL_HAS_HOLIDAY'), MarketClosedError)


def __test_map_error_code_generic_reject__():
    """A generic rejection names the venue reason code and its description."""
    from pynecore.core.broker.exceptions import ExchangeOrderRejectedError
    err = map_error_code('SOMETHING_ELSE', 'bad order')
    assert isinstance(err, ExchangeOrderRejectedError)
    assert str(err) == "cTrader rejected the order (SOMETHING_ELSE: bad order)"


def __test_map_error_code_bare_code_reject_still_names_the_reason__():
    """``ProtoOAExecutionEvent`` rejections carry only a code — it must survive
    into the message, otherwise the live log shows a reasonless rejection."""
    err = map_error_code('POSITION_NOT_FOUND', '')
    assert str(err) == "cTrader rejected the order (POSITION_NOT_FOUND)"
    assert str(map_error_code('', '')) == "cTrader rejected the order"


def __test_protocol_error_connection_class_is_retryable__():
    """Server-side connectivity/maintenance codes mark the error transient."""
    from pynecore.core.plugin import is_retryable_provider_error
    for code in ('CANT_ROUTE_REQUEST', 'SERVER_IS_UNDER_MAINTENANCE',
                 'CH_SERVER_NOT_REACHABLE', 'TIMEOUT_ERROR'):
        err = CTraderProtocolError(code, 'broker maintenance')
        assert err.retryable is True, code
        assert is_retryable_provider_error(err) is True, code


def __test_protocol_error_permanent_codes_not_retryable__():
    """Symbol / auth / business rejections stay permanent: no retry."""
    from pynecore.core.plugin import is_retryable_provider_error
    for code in ('SYMBOL_NOT_FOUND', 'CH_CLIENT_AUTH_FAILURE', 'NOT_ENOUGH_MONEY'):
        err = CTraderProtocolError(code)
        assert err.retryable is False, code
        assert is_retryable_provider_error(err) is False, code


def __test_connection_and_timeout_errors_are_retryable__():
    """Wire-level connection drops and timeouts are always transient."""
    from pynecore.core.plugin import is_retryable_provider_error
    from pynecore_ctrader.wire import CTraderConnectionError, CTraderTimeoutError
    assert is_retryable_provider_error(CTraderConnectionError("lost")) is True
    assert is_retryable_provider_error(CTraderTimeoutError("no reply")) is True


def __test_protocol_error_retryable_survives_provider_error_wrap__():
    """A wrapped CANT_ROUTE_REQUEST is still recognised as retryable."""
    from pynecore.core.plugin import ProviderError, is_retryable_provider_error
    try:
        try:
            raise CTraderProtocolError('CANT_ROUTE_REQUEST', 'maintenance')
        except CTraderProtocolError as inner:
            raise ProviderError("warmup download failed") from inner
    except ProviderError as outer:
        assert is_retryable_provider_error(outer) is True


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


def __test_cancel_confirmed_closes_broker_store_row__(tmp_path):
    # A synchronous ORDER_CANCELLED is consumed by the dispatch path, so no
    # PUSH cancelled event retires the row — the confirmed-cancel path must
    # itself close the working row, or a graceful shutdown before the reconcile
    # grace window leaves a venue-cancelled order live in the store.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_working_entry(broker)
    broker._canned_event = _exec_event(_model.ProtoOAExecutionType.ORDER_CANCELLED)
    assert _cancel(broker) is CancelDispositionOutcome.CANCEL_CONFIRMED
    assert list(broker.store_ctx.iter_live_orders()) == []
    row = broker.store_ctx.get_order('coid-111')
    assert row is not None and row.closed_ts_ms is not None
    # Refs are dropped by close_order, so a duplicate signal is a benign no-op.
    assert broker.store_ctx.find_by_ref('order_id', '111') is None


def __test_execute_cancel_bool_confirmed_closes_broker_store_row__(tmp_path):
    # The bool-only execute_cancel path (diff drop / OCA cascade / forced
    # cancel) must also retire the working row on a confirmed ORDER_CANCELLED.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_working_entry(broker)
    broker._canned_event = _exec_event(_model.ProtoOAExecutionType.ORDER_CANCELLED)
    intent = CancelIntent(pine_id='Long', symbol='EURUSD')
    assert asyncio.run(broker.execute_cancel(_envelope(intent))) is True
    assert list(broker.store_ctx.iter_live_orders()) == []


def _seed_adopted_working_entry(broker, *, pine_id='Long', order_id=111,
                                position_id=52709805):
    """Seed the restart-adoption shape of a working-order row.

    Mirrors the live adopted row: ``extras`` carries the venue ``order_id``
    AND a linked ``position_id`` (cTrader assigns one to pending orders), the
    ``exchange_order_id`` column holds the position id, and the ``order_id``
    ref exists (BrokerStore adoption migrates refs across run instances).
    """
    coid = f'coid-{order_id}'
    broker.store_ctx.upsert_order(
        coid, symbol='EURUSD', side='buy', qty=10.0, filled_qty=0.0,
        state='confirmed', pine_entry_id=pine_id,
        exchange_order_id=str(position_id),
        extras={'order_id': str(order_id), 'position_id': position_id,
                'kind': 'working', 'order_type': 'stop'},
    )
    broker.store_ctx.add_ref(coid, 'order_id', str(order_id))
    return coid


def _store_event_kinds(tmp_path, coid):
    import sqlite3
    conn = sqlite3.connect(tmp_path / 'broker.sqlite')
    try:
        rows = conn.execute(
            "SELECT kind FROM events WHERE client_order_id = ? ORDER BY id",
            (coid,),
        ).fetchall()
    finally:
        conn.close()
    return [kind for (kind,) in rows]


def __test_sync_cancel_ack_reinjects_cancelled_event_and_logs_terminal__(tmp_path):
    # LIVE sequence (2026-07-21 restart-adoption repro): phase B adopts the
    # venue STOP order, strategy.cancel lands, the ONLY ORDER_CANCELLED
    # terminal comes back as the correlated dispatch response and is consumed
    # by send_request — nothing ever reaches watch_orders, so no
    # strategy-visible cancelled OrderEvent and no durable cancellation
    # terminal exist: the local runner never terminalizes the cancel. The
    # confirmed sync ack must (a) re-inject the consumed execution event onto
    # the order-event stream (like correlated fills) and (b) write a durable
    # 'cancelled' audit event alongside the row close.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    coid = _seed_adopted_working_entry(broker)
    cancelled = _exec_event(
        _model.ProtoOAExecutionType.ORDER_CANCELLED,
        order=_make_order(order_id=111, order_type=_model.ProtoOAOrderType.STOP,
                          stop=1.30, position_id=52709805))
    broker._canned_event = cancelled

    async def run():
        broker._exec_events = asyncio.Queue()
        intent = CancelIntent(pine_id='Long', symbol='EURUSD')
        assert await broker.execute_cancel(_envelope(intent)) is True
        # The consumed ack is back on the stream for watch_orders to emit.
        assert broker._exec_events.qsize() == 1
        agen = broker.watch_orders()
        try:
            return await asyncio.wait_for(agen.__anext__(), timeout=5.0)
        finally:
            await agen.aclose()

    event = asyncio.run(run())
    # Strategy-visible cancellation terminal, keyed by the broker order id the
    # engine registered as strategy-cancel-expected.
    assert event is not None and event.event_type == 'cancelled'
    assert event.order.id == '111'
    # Durable terminal: the row is closed AND the close is identifiable as a
    # cancellation (generic order_closed alone is not terminal progress).
    row = broker.store_ctx.get_order(coid)
    assert row is not None and row.closed_ts_ms is not None
    assert 'cancelled' in _store_event_kinds(tmp_path, coid)


def __test_cancel_with_outcome_reinjects_cancelled_event__(tmp_path):
    # The outcome-based cancel path (cancel-tentative retry loop) consumes the
    # same correlated ORDER_CANCELLED — it must surface the terminal on the
    # stream too, or a tentative resolved by return value still leaves no
    # strategy-visible / durable cancellation terminal.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    coid = _seed_adopted_working_entry(broker)
    broker._canned_event = _exec_event(_model.ProtoOAExecutionType.ORDER_CANCELLED)

    async def run():
        broker._exec_events = asyncio.Queue()
        intent = CancelIntent(pine_id='Long', symbol='EURUSD')
        outcome = await broker.execute_cancel_with_outcome(_envelope(intent))
        assert outcome is CancelDispositionOutcome.CANCEL_CONFIRMED
        return broker._exec_events.qsize()

    assert asyncio.run(run()) == 1
    assert 'cancelled' in _store_event_kinds(tmp_path, coid)


def __test_reinjected_cancel_translation_survives_dropped_refs__(tmp_path):
    # By the time watch_orders drains the re-injected ack, close_order has
    # already deleted the order_id refs — identity resolution misses. A
    # cancelled event must still be emitted (identity-less), never dropped as
    # external activity; the duplicate retire must be a benign no-op.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_adopted_working_entry(broker)
    cancelled = _exec_event(_model.ProtoOAExecutionType.ORDER_CANCELLED)
    broker._retire_cancelled_working_order(111)  # refs now gone
    event = broker._translate_exec_event(cancelled)
    assert event is not None and event.event_type == 'cancelled'
    assert event.order.id == '111'


def __test_cancel_confirmed_keeps_partially_filled_row_live__(tmp_path):
    # A cancelled UNFILLED residual of a partially filled entry leaves a live
    # position under the row — retiring it would strand that exposure. The
    # confirmed-cancel close must skip a row carrying fills.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_working_entry(broker)
    broker.store_ctx.set_filled('coid-111', 4.0)
    broker._canned_event = _exec_event(_model.ProtoOAExecutionType.ORDER_CANCELLED)
    assert _cancel(broker) is CancelDispositionOutcome.CANCEL_CONFIRMED
    assert [r.client_order_id for r in broker.store_ctx.iter_live_orders()] == ['coid-111']


def __test_push_cancelled_closes_broker_store_row__(tmp_path):
    # An external / expiry cancel that reaches the PUSH stream (not consumed by
    # the dispatch path) must also retire its working-order row.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_working_entry(broker)
    event = broker._translate_exec_event(
        _exec_event(_model.ProtoOAExecutionType.ORDER_CANCELLED,
                    order=_make_order(order_id=111)))
    assert event is not None and event.event_type == 'cancelled'
    assert list(broker.store_ctx.iter_live_orders()) == []


# === Bracket-attach-reject residual cleanup ================================
# A rejected ProtoOAAmendPositionSLTPReq references only the positionId, so
# the unfilled remainder of a partially filled parent LIMIT/STOP stays live
# on the broker — without enumeration + cancel it could fill into an
# unmanaged position after the defensive close.

def _reject_context(*, from_entry: str | None = 'Long', symbol='EURUSD'):
    return BracketAttachRejectContext(
        intent_key='exit-key', position_coid='coid-parent',
        position_side='buy', qty=10.0, symbol=symbol, from_entry=from_entry,
    )


def _dispatch_limit_entry(broker, *, pine_id='Long', order_id=111):
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=order_id,
                          order_type=_model.ProtoOAOrderType.LIMIT),
    )
    intent = EntryIntent(pine_id=pine_id, symbol='EURUSD', side='buy',
                         qty=10.0, order_type=OrderType.LIMIT, limit=1.20)
    asyncio.run(broker.execute_entry(_envelope(intent)))


def __test_persist_entry_preserves_kind_in_extras__(tmp_path):
    # upsert_order REPLACES the extras blob — the confirm-time persist must
    # merge, or the working/position discriminator written by the
    # persist-first row is lost.
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    row = _only_live_order(broker)
    assert row.extras['kind'] == ENTRY_KIND_WORKING
    assert row.extras['order_id'] == '111'


def __test_residual_enumerates_partial_filled_working_parent__(tmp_path):
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker, order_id=111)
    coid = broker.coid_at_dispatch
    assert coid is not None
    # Partial-fill progress recorded — the row stays live and working-kind.
    broker.store_ctx.set_filled(coid, 4.0)
    refs = broker.get_residual_orders_after_bracket_attach_reject(
        _reject_context())
    assert refs == ['111']
    # Repeated enumeration with the same context is stable (idempotency).
    assert broker.get_residual_orders_after_bracket_attach_reject(
        _reject_context()) == ['111']


def __test_residual_skips_market_entry_and_other_pine_ids__(tmp_path):
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    # MARKET parent -> position kind, nothing cancellable.
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=222, executed=1000, position_id=555),
    )
    intent = EntryIntent(pine_id='Long', symbol='EURUSD', side='buy',
                         qty=10.0, order_type=OrderType.MARKET)
    asyncio.run(broker.execute_entry(_envelope(intent)))
    # A live working order under a DIFFERENT pine id is a managed sibling the
    # engine owns — the residual sweep must not touch it.
    _dispatch_limit_entry(broker, pine_id='Other', order_id=333)
    assert broker.get_residual_orders_after_bracket_attach_reject(
        _reject_context()) == []


def __test_residual_empty_without_store_or_from_entry__(tmp_path):
    broker = _FakeBroker()
    assert broker.get_residual_orders_after_bracket_attach_reject(
        _reject_context()) == []
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    assert broker.get_residual_orders_after_bracket_attach_reject(
        _reject_context(from_entry=None)) == []


def __test_cancel_broker_order_ref_sends_cancel_request__():
    broker = _FakeBroker()
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_CANCELLED)
    asyncio.run(broker.cancel_broker_order_ref('111'))
    cancels = [r for r in broker.sent
               if isinstance(r, _oa.ProtoOACancelOrderReq)]
    assert len(cancels) == 1
    assert cancels[0].orderId == 111


def __test_cancel_broker_order_ref_not_found_is_noop__():
    # Already filled / cancelled residual: the base idempotency contract
    # requires a silent no-op, never an exception.
    broker = _FakeBroker()
    err = ExchangeOrderRejectedError("order gone")
    err.__cause__ = CTraderProtocolError('ORDER_NOT_FOUND', '')
    broker._raise_on_dispatch = err
    asyncio.run(broker.cancel_broker_order_ref('111'))


def __test_cancel_broker_order_ref_cancel_rejected_raises_unknown__():
    # ORDER_CANCEL_REJECTED is a non-error execution event: the order is
    # still live and may fill — raise disposition-unknown so the engine's
    # residual loop retries instead of declaring the recovery complete.
    broker = _FakeBroker()
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_CANCEL_REJECTED)
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.cancel_broker_order_ref('111'))


# === Venue-write quarantine gate ==========================================

_WRITE_REQUEST_TYPES = (
    _oa.ProtoOANewOrderReq,
    _oa.ProtoOACancelOrderReq,
    _oa.ProtoOAAmendOrderReq,
    _oa.ProtoOAAmendPositionSLTPReq,
    _oa.ProtoOAClosePositionReq,
)


class _GateWire:
    """Recording wire that answers every request with the canned reconcile snapshot."""

    def __init__(self, reconcile):
        self._reconcile = reconcile
        self.requests: list = []

    async def send_request(self, req, *, write_gate=None):
        if write_gate is not None:
            write_gate()
        self.requests.append(req)
        return self._reconcile


class _GatedBroker(CTrader):
    """cTrader broker with the REAL ``_dispatch_order`` over a recording wire.

    The venue-write hook reports a latched external-intervention quarantine, and
    the snapshot holds one open EURUSD position (555) without protection.
    """

    def __init__(self):
        super().__init__(symbol=None, config=_make_config())
        self._live_account_id = 999
        self._symbols_by_name = {'EURUSD': 1}
        self._symbols_by_id = {1: 'EURUSD'}
        self._symbol_rules = {'EURUSD': _RULES}
        res = _oa.ProtoOAReconcileRes()
        res.position.append(_model.ProtoOAPosition(
            positionId=555,
            positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
            tradeData=_model.ProtoOATradeData(symbolId=1, volume=1000,
                                              tradeSide=_model.ProtoOATradeSide.BUY),
        ))
        self.wire = _GateWire(res)
        self._wire = self.wire
        self.venue_writes_blocked = lambda: True


def _sent_writes(broker: _GatedBroker) -> list:
    return [r for r in broker.wire.requests if isinstance(r, _WRITE_REQUEST_TYPES)]


@pytest.mark.parametrize('req', [
    _oa.ProtoOANewOrderReq(ctidTraderAccountId=999, symbolId=1, volume=1000),
    _oa.ProtoOACancelOrderReq(ctidTraderAccountId=999, orderId=111),
    _oa.ProtoOAAmendOrderReq(ctidTraderAccountId=999, orderId=111, volume=1000),
    _oa.ProtoOAAmendPositionSLTPReq(ctidTraderAccountId=999, positionId=555),
    _oa.ProtoOAClosePositionReq(ctidTraderAccountId=999, positionId=555, volume=1000),
], ids=lambda r: type(r).__name__)
def __test_dispatch_order_refuses_every_write_while_quarantined__(req):
    broker = _GatedBroker()
    with pytest.raises(VenueWritesQuarantinedError) as info:
        asyncio.run(broker._dispatch_order(req, coid='coid-q', context='gate'))
    assert type(req).__name__ in str(info.value)
    assert broker.wire.requests == []


class _RecordingWriter:
    """``StreamWriter`` stand-in that records every frame written to it."""

    def __init__(self):
        self.writes: list[bytes] = []

    @staticmethod
    def is_closing() -> bool:
        return False

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        return None


class _SignallingLock(asyncio.Lock):
    """Send lock that signals once a second sender starts waiting for it."""

    def __init__(self):
        super().__init__()
        self.contended = asyncio.Event()

    async def acquire(self):
        if self.locked():
            self.contended.set()
        return await super().acquire()


def __test_quarantine_latched_while_waiting_for_the_send_lock_writes_nothing__():
    # The gate runs inside the REAL wire's send lock: an order request queued
    # behind another write (or a heartbeat drain) must not reach the socket if
    # the quarantine latches before it gets the lock.
    async def scenario() -> None:
        broker = CTrader(symbol=None, config=_make_config())
        broker._live_account_id = 999
        quarantined = [False]
        broker.venue_writes_blocked = lambda: quarantined[0]
        wire = WireClient('127.0.0.1', 1)
        writer = _RecordingWriter()
        wire._writer = writer  # type: ignore[assignment]
        lock = _SignallingLock()
        wire._send_lock = lock
        broker._wire = wire
        req = _oa.ProtoOACancelOrderReq(ctidTraderAccountId=999, orderId=111)

        async with lock:
            task = asyncio.create_task(
                broker._dispatch_order(req, coid='coid-q', context='cancel'))
            await lock.contended.wait()
            quarantined[0] = True
        with pytest.raises(VenueWritesQuarantinedError):
            await task
        assert writer.writes == []
        assert wire._pending == {}

    asyncio.run(asyncio.wait_for(scenario(), timeout=5.0))


def __test_dispatch_order_sends_once_the_quarantine_lifts__():
    broker = _GatedBroker()
    broker.venue_writes_blocked = lambda: False
    req = _oa.ProtoOACancelOrderReq(ctidTraderAccountId=999, orderId=111)
    with pytest.raises(ExchangeOrderRejectedError):
        # The canned reconcile snapshot is not an execution event; what matters
        # is that the request reached the wire.
        asyncio.run(broker._dispatch_order(req, coid='coid-q', context='gate'))
    assert broker.wire.requests == [req]


def __test_quarantine_refuses_every_write_path_but_reads_still_go_out__(tmp_path):
    broker = _GatedBroker()
    _open_store(tmp_path, broker)
    _seed_working_entry(broker)
    _seed_owned_entry(broker, coid='parent', pine_id='P', position_id=555)
    entry = EntryIntent(pine_id="Long", symbol="EURUSD", side="buy",
                        qty=10.0, order_type=OrderType.LIMIT, limit=1.05)
    exit_intent = ExitIntent(pine_id="Exit", from_entry="Long", symbol="EURUSD",
                             side="sell", qty=10.0, tp_price=1.30, sl_price=1.10)
    writes = {
        'execute_entry': lambda: broker.execute_entry(_envelope(entry)),
        'place_leg': lambda: broker.place_leg(_envelope(entry, run_tag="cd34"), 10.0),
        'execute_exit': lambda: broker.execute_exit(_envelope(exit_intent)),
        'modify_exit': lambda: broker.modify_exit(
            _envelope(exit_intent), _envelope(exit_intent, run_tag="cd34")),
        'modify_entry': lambda: broker.modify_entry(
            _envelope(entry), _envelope(entry, run_tag="cd34")),
        'execute_close': lambda: broker.execute_close(_envelope(CloseIntent(
            pine_id="Long", symbol="EURUSD", side="sell", qty=10.0))),
        'execute_cancel': lambda: broker.execute_cancel(_envelope(CancelIntent(
            pine_id='Long', symbol='EURUSD'))),
        'execute_cancel_with_outcome': lambda: broker.execute_cancel_with_outcome(
            _envelope(CancelIntent(pine_id='Long', symbol='EURUSD'))),
        'cancel_broker_order_ref': lambda: broker.cancel_broker_order_ref('111'),
        'close_leg': lambda: broker.close_leg("EURUSD", "555", 1000, "coid-c"),
        'amend_bracket': lambda: broker.amend_bracket(
            "EURUSD", "555", side="sell", tp_price=1.30, sl_price=1.10,
            trail_offset=None, coid="coid-b"),
        'clear_bracket': lambda: broker.amend_bracket(
            "EURUSD", "555", side="sell", tp_price=None, sl_price=None,
            trail_offset=None, coid="coid-clear"),
        'publish_native_failsafe_sl': lambda: broker.publish_native_failsafe_sl(
            NativeBracketSnapshot(
                parent_entry_dispatch_ref='parent', symbol='EURUSD',
                parent_side='long', stop_level=1.10, profit_level=None,
                trailing_stop=None, generation=1)),
    }
    for name, write in writes.items():
        with pytest.raises(VenueWritesQuarantinedError):
            asyncio.run(write())
        assert _sent_writes(broker) == [], name

    # Nothing was sent, so no journal row moved as if a write had happened: the
    # working order and the parent stay live, the entries stay ``submitted``.
    working = broker.store_ctx.get_order('coid-111')
    assert working is not None and working.closed_ts_ms is None
    assert working.state == 'confirmed'
    parent = broker.store_ctx.get_order('parent')
    assert parent is not None and parent.closed_ts_ms is None
    submitted = [r for r in broker.store_ctx.iter_live_orders()
                 if r.client_order_id not in ('coid-111', 'parent')]
    assert submitted and all(r.state == 'submitted' for r in submitted)

    # Reads are not venue writes: the snapshot read still goes out.
    res = asyncio.run(broker._reconcile())
    assert [p.positionId for p in res.position] == [555]
    assert isinstance(broker.wire.requests[-1], _oa.ProtoOAReconcileReq)


# === External intervention: moved SL/TP, manual close =====================

_PID = 54302872


def _spy_quarantine(broker) -> list:
    """Capture every external-intervention quarantine request of ``broker``."""
    calls: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: calls.append(
        (reason, context, kind))
    return calls


def _protected_position(*, sl=None, tp=None, trailing=None, volume=1000,
                        position_id=_PID):
    position = _model.ProtoOAPosition(
        positionId=position_id,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=volume,
                                          tradeSide=_model.ProtoOATradeSide.BUY),
    )
    if sl is not None:
        position.stopLoss = sl
    if tp is not None:
        position.takeProfit = tp
    if trailing is not None:
        position.trailingStopLoss = trailing
    return position


def _set_venue(broker, **levels) -> None:
    """Make the canned reconcile snapshot carry one position with ``levels``."""
    res = _oa.ProtoOAReconcileRes()
    res.position.append(_protected_position(**levels))
    broker._reconcile_res = res


def _guarded_broker(tmp_path, *, sl=1.1, tp=1.3, trail_offset=None):
    """A broker owning position ``_PID`` whose bracket it attached itself."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker)
    intent = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                        side="sell", qty=10.0, tp_price=tp, sl_price=sl,
                        trail_offset=trail_offset)
    asyncio.run(broker.execute_exit(_envelope(intent)))
    return broker


def _snapshot_pass(broker) -> list:
    async def collect():
        return [event async for event in broker._reconcile_snapshot()]
    return asyncio.run(collect())


def _replaced_push(**levels):
    sltp = _make_order(order_id=70300000, position_id=_PID, volume=0,
                       order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT)
    event = _exec_event(_model.ProtoOAExecutionType.ORDER_REPLACED, order=sltp)
    event.position.CopyFrom(_protected_position(**levels))
    return event


def __test_moved_stop_via_replaced_push_reports_once__(tmp_path):
    """Someone else moving the SL is quarantined once two pushes agree.

    ORDER_REPLACED reaches no engine event; its position snapshot feeds the
    guard. One push alone is never judged: the bot's own amend responses
    arrive the same way, and a push may describe an earlier state.
    """
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)

    assert broker._translate_exec_event(_replaced_push(sl=1.05, tp=1.3)) is None
    assert calls == []
    assert broker._translate_exec_event(_replaced_push(sl=1.05, tp=1.3)) is None
    assert len(calls) == 1
    _reason, context, kind = calls[0]
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert context['protection_key'] == f"position:{_PID}"
    assert context['client_order_id'] == 'c1'
    assert context['observed'][0] == pytest.approx(1.05)

    broker._translate_exec_event(_replaced_push(sl=1.05, tp=1.3))
    assert len(calls) == 1


def __test_moved_take_profit_via_snapshot_reports_once__(tmp_path):
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)

    _snapshot_pass(broker)
    assert calls == [], "the venue carries the bot's own levels"
    _set_venue(broker, sl=1.1, tp=1.35)
    _snapshot_pass(broker)
    assert calls == []
    _snapshot_pass(broker)
    assert len(calls) == 1
    _snapshot_pass(broker)
    assert len(calls) == 1


def __test_removed_stop_via_snapshot_reports__(tmp_path):
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _set_venue(broker, sl=1.1, tp=1.3)
    _snapshot_pass(broker)

    _set_venue(broker, tp=1.3)
    _snapshot_pass(broker)
    _snapshot_pass(broker)

    assert len(calls) == 1


class _HookedBroker(_FakeBroker):
    """Runs ``on_dispatch`` while a request is in flight."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.on_dispatch = None

    async def _dispatch_order(self, req, *, coid, context, predecessor_cancel_ids=None):
        if self.on_dispatch is not None:
            await self.on_dispatch(req)
        return await super()._dispatch_order(
            req, coid=coid, context=context,
            predecessor_cancel_ids=predecessor_cancel_ids)


def __test_own_modify_exit_never_reports__(tmp_path):
    """The bot's own amend is never an intervention, before, during or after it.

    Snapshot passes while the amend is in flight (old or new levels) are not
    judged; a snapshot read that started before the write is skipped; once
    settled, the venue carrying the new levels matches.
    """
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker, sl=1.1, tp=1.3)
    old = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                     side="sell", qty=10.0, tp_price=1.3, sl_price=1.1)
    new = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                     side="sell", qty=10.0, tp_price=1.4, sl_price=1.15)
    asyncio.run(broker.execute_exit(_envelope(old)))
    calls = _spy_quarantine(broker)
    stale_read_started_at = time.time() - 1.0

    async def reconcile_in_flight(_req):
        _set_venue(broker, sl=1.1, tp=1.3)
        _ = [e async for e in broker._reconcile_snapshot()]
        _ = [e async for e in broker._reconcile_snapshot()]
        _set_venue(broker, sl=1.15, tp=1.4)
        _ = [e async for e in broker._reconcile_snapshot()]

    broker.on_dispatch = reconcile_in_flight
    asyncio.run(broker.modify_exit(_envelope(old), _envelope(new, run_tag="cd34")))
    broker.on_dispatch = None
    assert calls == []

    # A snapshot whose read started before the amend shows the old levels.
    stale = {_PID: _protected_position(sl=1.1, tp=1.3)}
    for _ in range(2):
        asyncio.run(broker._observe_protection_snapshot(
            stale, read_started_at=stale_read_started_at, filled_positions=set()))
    assert calls == []

    for _ in range(3):
        _snapshot_pass(broker)
    assert calls == []

    # A late push of the replaced own levels next to fresh snapshots.
    broker._translate_exec_event(_replaced_push(sl=1.1, tp=1.3))
    _snapshot_pass(broker)
    assert calls == []


def __test_sub_tick_noise_never_reports__(tmp_path):
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _set_venue(broker, sl=1.100004, tp=1.299996)

    for _ in range(3):
        _snapshot_pass(broker)
        broker._translate_exec_event(_replaced_push(sl=1.100004, tp=1.299996))

    assert calls == []


def __test_trailing_stop_moving_never_reports__(tmp_path):
    """The venue ratchets a trailing stop by itself: never an intervention."""
    broker = _FakeBroker()
    broker._last_bid = 1.2
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker)
    intent = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                        side="sell", qty=10.0, trail_offset=0.01)
    asyncio.run(broker.execute_exit(_envelope(intent)))
    assert broker.sent[-1].trailingStopLoss is True
    calls = _spy_quarantine(broker)

    for stop in (1.19, 1.193, 1.196, 1.199):
        _set_venue(broker, sl=stop, trailing=True)
        _snapshot_pass(broker)
        broker._translate_exec_event(_replaced_push(sl=stop, trailing=True))

    assert calls == []


def __test_trailing_switched_off_externally_reports__(tmp_path):
    broker = _FakeBroker()
    broker._last_bid = 1.2
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker)
    intent = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                        side="sell", qty=10.0, trail_offset=0.01)
    asyncio.run(broker.execute_exit(_envelope(intent)))
    calls = _spy_quarantine(broker)

    _set_venue(broker, sl=1.195, trailing=False)
    _snapshot_pass(broker)
    _snapshot_pass(broker)

    assert len(calls) == 1


def __test_restart_restores_levels_without_a_false_report__(tmp_path):
    """The accepted levels persist on the entry row and survive a restart."""
    first = _guarded_broker(tmp_path)
    _set_venue(first, sl=1.1, tp=1.3)
    _snapshot_pass(first)

    restarted = _FakeBroker()
    restarted.store_ctx = first.store_ctx
    restarted._wire = object()
    calls = _spy_quarantine(restarted)
    _set_venue(restarted, sl=1.1, tp=1.3)
    for _ in range(3):
        _snapshot_pass(restarted)
    assert calls == []

    _set_venue(restarted, sl=1.0, tp=1.3)
    _snapshot_pass(restarted)
    _snapshot_pass(restarted)
    assert len(calls) == 1


def __test_failsafe_owned_position_is_left_to_the_core_manager__(tmp_path):
    """A position whose SL the native fail-safe wrote is not guarded here.

    The core fail-safe manager compares its own levels and quarantines on an
    external edit; a second report from the plugin guard would be a duplicate.
    """
    broker = _guarded_broker(tmp_path)
    broker.store_ctx.upsert_order('c1', exchange_order_id=str(_PID))
    calls = _spy_quarantine(broker)
    asyncio.run(broker.publish_native_failsafe_sl(NativeBracketSnapshot(
        parent_entry_dispatch_ref='c1', symbol='EURUSD', parent_side='long',
        stop_level=1.08, profit_level=None, trailing_stop=None, generation=1)))

    _set_venue(broker, sl=1.02)
    for _ in range(3):
        _snapshot_pass(broker)

    assert calls == []
    row = broker.store_ctx.get_order('c1')
    assert 'protection_levels' not in (row.extras or {})


def __test_cleared_bracket_is_not_guarded__(tmp_path):
    """An operator adding protection to a position the bot left bare is no edit."""
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _set_venue(broker, sl=1.1, tp=1.3)
    asyncio.run(broker.execute_cancel(_envelope(CancelIntent(
        pine_id='Exit', symbol='EURUSD', from_entry='L'))))

    _set_venue(broker, sl=1.0)
    for _ in range(3):
        _snapshot_pass(broker)

    assert calls == []


def __test_failed_first_protection_write_never_reports__(tmp_path):
    """A first amend that never reached the venue leaves nothing to judge.

    The position still carries the protection it had before the write, which
    the guard never knew; judging it against the unsent levels would
    quarantine the bot for its own failed request.
    """
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker)
    calls = _spy_quarantine(broker)

    async def drop(_req):
        raise ExchangeConnectionError("connection lost")

    broker.on_dispatch = drop
    intent = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                        side="sell", qty=10.0, tp_price=1.3, sl_price=1.1)
    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker.execute_exit(_envelope(intent)))
    broker.on_dispatch = None

    for _ in range(3):
        _snapshot_pass(broker)
    assert calls == []
    assert not broker._protection_guard().known(f"position:{_PID}")


def __test_unknown_first_protection_write_never_reports__(tmp_path):
    """A first amend whose outcome is unknown leaves nothing to judge either.

    The venue may never have received it; the protection it would then still
    carry was never known to the guard.
    """
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker)
    calls = _spy_quarantine(broker)

    async def timeout(_req):
        raise OrderDispositionUnknownError(
            "cTrader bracket attach timed out; disposition unknown",
            client_order_id='c1')

    broker.on_dispatch = timeout
    intent = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                        side="sell", qty=10.0, tp_price=1.3, sl_price=1.1)
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_exit(_envelope(intent)))
    broker.on_dispatch = None

    for _ in range(3):
        _snapshot_pass(broker)
    assert calls == []
    assert not broker._protection_guard().known(f"position:{_PID}")


def _modify_with_failure(tmp_path, failure: BaseException):
    """A guarded broker whose amend from 1.1 / 1.3 to 1.15 / 1.4 failed with ``failure``."""
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker, sl=1.1, tp=1.3)
    old = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                     side="sell", qty=10.0, tp_price=1.3, sl_price=1.1)
    new = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                     side="sell", qty=10.0, tp_price=1.4, sl_price=1.15)
    asyncio.run(broker.execute_exit(_envelope(old)))
    _snapshot_pass(broker)

    async def fail(_req):
        raise failure

    broker.on_dispatch = fail
    with pytest.raises(type(failure)):
        asyncio.run(broker.modify_exit(_envelope(old), _envelope(new, run_tag="cd34")))
    broker.on_dispatch = None
    return broker


def __test_refused_protection_amend_stops_being_acceptable__(tmp_path):
    """The venue refused the amend: its levels showing up later are someone else's."""
    reject = ExchangeOrderRejectedError("cTrader rejected the order (TRADING_BAD_STOPS)")
    reject.__cause__ = CTraderProtocolError('TRADING_BAD_STOPS', '')
    broker = _modify_with_failure(tmp_path, reject)
    calls = _spy_quarantine(broker)

    _snapshot_pass(broker)
    assert calls == [], "the venue still carries the bot's previous levels"
    _set_venue(broker, sl=1.15, tp=1.4)
    _snapshot_pass(broker)
    _snapshot_pass(broker)
    assert len(calls) == 1


def __test_unknown_protection_amend_stays_acceptable__(tmp_path):
    """An amend whose outcome is unknown may have landed: its levels are the bot's."""
    broker = _modify_with_failure(tmp_path, OrderDispositionUnknownError(
        "cTrader amend bracket timed out; disposition unknown", client_order_id='c1'))
    calls = _spy_quarantine(broker)

    _set_venue(broker, sl=1.15, tp=1.4)
    for _ in range(3):
        _snapshot_pass(broker)
    assert calls == []


def _overlapping_protection_writes(tmp_path, levels, answers, *, baseline=True):
    """Send one amend per ``levels`` at once; answer them in ``answers`` order.

    :param levels: ``(stop, take_profit)`` of each write, in write order.
    :param answers: ``(write index, failure or None)`` in answer order.
    :param baseline: Guard ``_PID`` from 1.1 / 1.3 before the writes;
        otherwise they are the position's first protection writes.
    :return: The broker.
    """
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker, sl=1.1, tp=1.3)
    if baseline:
        asyncio.run(broker.execute_exit(_envelope(ExitIntent(
            pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
            tp_price=1.3, sl_price=1.1))))
        _snapshot_pass(broker)

    async def scenario():
        gates = [asyncio.Event() for _ in levels]
        failures: dict[int, BaseException | None] = {}
        arrived: list[int] = []
        all_arrived = asyncio.Event()

        async def hold(_req):
            index = len(arrived)
            arrived.append(index)
            if len(arrived) == len(levels):
                all_arrived.set()
            await gates[index].wait()
            failure = failures[index]
            if failure is not None:
                raise failure

        broker.on_dispatch = hold
        tasks = [
            asyncio.create_task(broker._write_protection(
                _oa.ProtoOAAmendPositionSLTPReq(
                    ctidTraderAccountId=999, positionId=_PID, stopLoss=stop,
                    takeProfit=take_profit),
                position_id=_PID, row_coid='c1', coid=f'w{index}', context='amend'))
            for index, (stop, take_profit) in enumerate(levels)
        ]
        await all_arrived.wait()
        for index, failure in answers:
            failures[index] = failure
            gates[index].set()
            await asyncio.gather(tasks[index], return_exceptions=True)
        broker.on_dispatch = None

    asyncio.run(scenario())
    return broker


def _refused_amend() -> ExchangeOrderRejectedError:
    reject = ExchangeOrderRejectedError("cTrader rejected the order (TRADING_BAD_STOPS)")
    reject.__cause__ = CTraderProtocolError('TRADING_BAD_STOPS', '')
    return reject


def __test_overlapping_same_level_refusals_settle_their_own_write__(tmp_path):
    """Two writes of the same levels, both refused, answered out of order.

    Each answer closes its own write, so the refused levels stop being the
    bot's at once; someone setting them later is an edit.
    """
    broker = _overlapping_protection_writes(
        tmp_path, [(1.04, 1.2), (1.04, 1.2)],
        [(1, _refused_amend()), (0, _refused_amend())])
    calls = _spy_quarantine(broker)

    _snapshot_pass(broker)
    assert calls == [], "the venue still carries the bot's previous levels"
    _set_venue(broker, sl=1.04, tp=1.2)
    _snapshot_pass(broker)
    _snapshot_pass(broker)
    assert len(calls) == 1


def _unknown_amend() -> OrderDispositionUnknownError:
    return OrderDispositionUnknownError(
        "cTrader amend bracket timed out; disposition unknown", client_order_id='c1')


@pytest.mark.parametrize('answers', [
    [(1, None), (0, 'unknown')],
    [(0, 'unknown'), (1, None)],
], ids=['second-confirmed-first', 'first-failed-first'])
def __test_unknown_first_write_keeps_an_overlapping_confirmed_write__(tmp_path, answers):
    """The position's first write fails with an unknown outcome while a second
    write of it is confirmed: the confirmed write stays guarded."""
    broker = _overlapping_protection_writes(
        tmp_path, [(1.04, 1.2), (1.03, 1.2)],
        [(index, _unknown_amend() if failure else None) for index, failure in answers],
        baseline=False)
    calls = _spy_quarantine(broker)
    assert broker._protection_guard().known(f"position:{_PID}")

    _set_venue(broker, sl=1.04, tp=1.2)
    _snapshot_pass(broker)
    _set_venue(broker, sl=1.03, tp=1.2)
    _snapshot_pass(broker)
    _snapshot_pass(broker)
    assert calls == [], "both writes are the bot's"
    _set_venue(broker, sl=1.0, tp=1.2)
    _snapshot_pass(broker)
    _snapshot_pass(broker)
    assert len(calls) == 1


def __test_overlapping_first_writes_all_unknown_leave_nothing_to_judge__(tmp_path):
    broker = _overlapping_protection_writes(
        tmp_path, [(1.04, 1.2), (1.03, 1.2)],
        [(1, _unknown_amend()), (0, _unknown_amend())], baseline=False)
    calls = _spy_quarantine(broker)

    assert not broker._protection_guard().known(f"position:{_PID}")
    for _ in range(3):
        _snapshot_pass(broker)
    assert calls == []


def __test_overlapping_writes_answered_out_of_order_never_report__(tmp_path):
    """Writes A, B, A again; the first A refused, the last A and then B accepted.

    The late answer to B, written before the last A, never outranks it: the
    venue showing B before it settles on A is the bot's own history.
    """
    broker = _overlapping_protection_writes(
        tmp_path, [(1.04, 1.2), (1.03, 1.2), (1.04, 1.2)],
        [(0, _refused_amend()), (2, None), (1, None)])
    calls = _spy_quarantine(broker)

    _set_venue(broker, sl=1.03, tp=1.2)
    _snapshot_pass(broker)
    _set_venue(broker, sl=1.04, tp=1.2)
    _snapshot_pass(broker)
    _snapshot_pass(broker)
    assert calls == []


def __test_failed_bracket_clear_keeps_the_guard__(tmp_path):
    """A clear that fails before it changed anything leaves the levels guarded."""
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _set_venue(broker, sl=1.1, tp=1.3)
    canned = broker._reconcile_res

    async def failed_read():
        raise ExchangeConnectionError("snapshot read failed")

    broker._reconcile = failed_read
    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker._clear_position_bracket(_PID, coid='clear', context='clear'))
    del broker._reconcile
    broker._reconcile_res = canned
    assert broker._protection_guard().known(f"position:{_PID}")

    _snapshot_pass(broker)
    assert calls == []
    _set_venue(broker, sl=1.0, tp=1.3)
    _snapshot_pass(broker)
    _snapshot_pass(broker)
    assert len(calls) == 1


def _closing_fill(*, order_id, closed_cents=1000, remaining_cents=0,
                  order_type=_model.ProtoOAOrderType.MARKET, deal_id=None,
                  stop_out=False, server_event=False, partial=False):
    order = _make_order(order_id=order_id, executed=closed_cents,
                        volume=closed_cents, position_id=_PID, closing=True,
                        order_type=order_type,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    if stop_out:
        order.isStopOut = True
    deal = _model.ProtoOADeal(dealId=deal_id or order_id + 1,
                              filledVolume=closed_cents, executionPrice=1.1)
    exec_type = (_model.ProtoOAExecutionType.ORDER_PARTIAL_FILL if partial
                 else _model.ProtoOAExecutionType.ORDER_FILLED)
    event = _exec_event(exec_type, order=order, deal=deal)
    if server_event:
        event.isServerEvent = True
    status = (_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN if remaining_cents
              else _model.ProtoOAPositionStatus.POSITION_STATUS_CLOSED)
    event.position.CopyFrom(_model.ProtoOAPosition(
        positionId=_PID, positionStatus=status,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=remaining_cents,
                                          tradeSide=_model.ProtoOATradeSide.BUY),
    ))
    return event


def _owned_broker(tmp_path):
    broker = _FakeBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    return broker


@pytest.mark.parametrize('remaining_cents', [0, 600])
def __test_manual_close_is_booked_and_reported__(tmp_path, remaining_cents):
    """A MARKET close of the bot's position nobody of the bot sent.

    The fill is booked exactly like a close of the run's position, so the
    engine sees the true size; the report follows the delivered event.
    """
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=1000 - remaining_cents,
        remaining_cents=remaining_cents))

    assert out is not None
    assert out.leg_type is LegType.CLOSE and out.from_entry == 'L'
    assert out.fill_qty == pytest.approx((1000 - remaining_cents) / 100)
    assert calls == [], "reported only after the stream yielded the fill"
    broker._flush_external_interventions()
    assert len(calls) == 1
    _reason, context, kind = calls[0]
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert context['position_id'] == _PID and context['order_id'] == 70400000


def __test_manual_close_report_lost_with_its_process_is_replayed__(tmp_path):
    """The fill was judged external, but the process died before the report."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    assert broker._translate_exec_event(_closing_fill(order_id=70400000)) is not None
    assert calls == []
    broker.store_ctx.close()

    restarted = _FakeBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, restarted)
    restarted_calls = _spy_quarantine(restarted)
    restarted._flush_external_interventions()
    assert len(restarted_calls) == 1
    assert restarted_calls[0][1]['order_id'] == 70400000
    restarted._flush_external_interventions()
    assert len(restarted_calls) == 1


def __test_no_venue_write_leaves_before_a_replayed_report_is_handled__(tmp_path):
    """A restarted process must not trade before the lost report is handled."""
    broker = _owned_broker(tmp_path)
    _spy_quarantine(broker)
    assert broker._translate_exec_event(_closing_fill(order_id=70400000)) is not None
    broker.store_ctx.close()

    restarted = _GatedBroker()
    restarted.venue_writes_blocked = lambda: False
    _open_store(tmp_path, restarted)
    calls = _spy_quarantine(restarted)
    req = _oa.ProtoOANewOrderReq(ctidTraderAccountId=999, symbolId=1, volume=1000)
    with pytest.raises(VenueWritesQuarantinedError):
        asyncio.run(restarted._dispatch_order(req, coid='coid-r', context='entry'))
    assert _sent_writes(restarted) == [] and calls == []

    async def start_stream():
        restarted._exec_events = asyncio.Queue()
        agen = restarted.watch_orders()
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(agen.__anext__(), timeout=0.05)
        finally:
            await agen.aclose()

    asyncio.run(start_stream())
    assert len(calls) == 1 and calls[0][1]['order_id'] == 70400000
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(restarted._dispatch_order(req, coid='coid-r', context='entry'))
    assert _sent_writes(restarted) == [req]


def __test_manual_close_report_follows_the_fill_through_watch_orders__(tmp_path):
    broker = _owned_broker(tmp_path)
    broker.on_unexpected_cancel = 'halt'

    async def run():
        broker._exec_events = asyncio.Queue()
        broker._exec_events.put_nowait(_closing_fill(order_id=70400000))
        agen = broker.watch_orders()
        try:
            first = await asyncio.wait_for(agen.__anext__(), timeout=5.0)
            with pytest.raises(UnexpectedCancelError):
                await asyncio.wait_for(agen.__anext__(), timeout=5.0)
            return first
        finally:
            await agen.aclose()

    event = asyncio.run(run())
    assert event.leg_type is LegType.CLOSE and event.fill_qty == 10.0


@pytest.mark.parametrize('kind', ['sltp', 'stop_out', 'server_event'])
def __test_venue_closes_are_never_reported__(tmp_path, kind):
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000,
        order_type=(_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT if kind == 'sltp'
                    else _model.ProtoOAOrderType.MARKET),
        stop_out=kind == 'stop_out', server_event=kind == 'server_event'))
    broker._flush_external_interventions()

    assert out is not None and out.leg_type is LegType.CLOSE
    assert calls == []


def _close(broker, qty=10.0):
    intent = CloseIntent(pine_id="L", symbol="EURUSD", side="sell", qty=qty)
    return asyncio.run(broker.execute_close(_envelope(intent)))


def __test_own_close_correlated_by_order_id_is_not_reported__(tmp_path):
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=888, position_id=_PID, closing=True))
    _close(broker)

    out = broker._translate_exec_event(_closing_fill(order_id=888))
    broker._flush_external_interventions()

    assert out is not None and out.leg_type is LegType.CLOSE
    assert calls == []
    assert broker._own_closes == []


def __test_own_close_fill_before_the_response_is_not_reported__(tmp_path):
    """The fill push can overtake the close response that names its order."""
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    calls = _spy_quarantine(broker)
    seen: list = []

    async def fill_overtakes_response(_req):
        seen.append(broker._translate_exec_event(_closing_fill(
            order_id=889, closed_cents=400, remaining_cents=600, partial=True)))

    broker.on_dispatch = fill_overtakes_response
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=889, position_id=_PID, closing=True))
    _close(broker)
    broker.on_dispatch = None
    seen.append(broker._translate_exec_event(_closing_fill(
        order_id=889, closed_cents=1000, deal_id=9001)))
    broker._flush_external_interventions()

    assert all(out is not None and out.leg_type is LegType.CLOSE for out in seen)
    assert calls == []
    assert broker._own_closes == []


def __test_own_close_with_unknown_outcome_claims_its_fill__(tmp_path):
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._raise_on_dispatch = OrderDispositionUnknownError(
        "timed out", client_order_id="x")
    with pytest.raises(OrderDispositionUnknownError):
        _close(broker)
    broker._raise_on_dispatch = None

    broker._translate_exec_event(_closing_fill(order_id=890))
    broker._flush_external_interventions()

    assert calls == []


def __test_rejected_own_close_does_not_mask_a_manual_close__(tmp_path):
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._raise_on_dispatch = _position_locked_reject()
    with pytest.raises(OrderSkippedByPlugin):
        _close(broker)
    broker._raise_on_dispatch = None

    broker._translate_exec_event(_closing_fill(order_id=891))
    broker._flush_external_interventions()

    assert len(calls) == 1


def __test_manual_close_after_an_own_partial_close_is_reported__(tmp_path):
    """Regression: one own close must not make every later close look own."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=888, position_id=_PID, closing=True))
    _close(broker, qty=4.0)
    broker._translate_exec_event(_closing_fill(
        order_id=888, closed_cents=400, remaining_cents=600))
    broker._flush_external_interventions()
    assert calls == []

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=600))
    broker._flush_external_interventions()

    assert out is not None and out.leg_type is LegType.CLOSE
    assert len(calls) == 1


def __test_close_by_an_own_entry_order_is_not_reported__(tmp_path):
    """A netting reversal entry closing the position echoes our coid."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    create_entry_order_row(
        broker.store_ctx, coid='rev', symbol='EURUSD', side='sell', qty=20.0,
        intent_key='S', pine_entry_id='S',
        kind=ENTRY_KIND_POSITION, order_type='market',
    )
    fill = _closing_fill(order_id=70500000)
    fill.order.clientOrderId = 'rev'

    broker._translate_exec_event(fill)
    broker._flush_external_interventions()

    assert calls == []


def __test_pyramid_exits_persist_on_one_row_across_a_restart__(tmp_path):
    """Exits of two pyramid entries write one netted position's protection.

    The levels must live on one row: a stale copy on the first entry's row
    would be restored after a restart and flag the current levels as edited.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker, coid='c1', pine_id='L1')
    _seed_owned_entry(broker, coid='c2', pine_id='L2', order_id='70211175')
    _set_venue(broker)
    for from_entry, sl in (('L1', 1.1), ('L2', 1.12)):
        intent = ExitIntent(pine_id="Exit", from_entry=from_entry, symbol="EURUSD",
                            side="sell", qty=10.0, tp_price=1.3, sl_price=sl)
        asyncio.run(broker.execute_exit(_envelope(intent)))

    restarted = _FakeBroker()
    restarted.store_ctx = broker.store_ctx
    restarted._wire = object()
    calls = _spy_quarantine(restarted)
    _set_venue(restarted, sl=1.12, tp=1.3)
    for _ in range(3):
        _snapshot_pass(restarted)

    assert calls == []


def __test_own_entry_fill_accepts_the_protection_it_leaves__(tmp_path):
    """Whatever an own fill does to the position's protection is not an edit."""
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    order = _make_order(order_id=70211174, executed=1000, position_id=_PID,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    fill = _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order,
                       deal=_model.ProtoOADeal(dealId=5150, filledVolume=1000,
                                               executionPrice=1.2))
    fill.position.CopyFrom(_protected_position(volume=2000))
    broker.store_ctx.upsert_order('c1', filled_qty=0.0)

    out = broker._translate_exec_event(fill)
    _set_venue(broker, volume=2000)
    for _ in range(3):
        _snapshot_pass(broker)

    assert out is not None and out.leg_type is LegType.ENTRY
    assert calls == []
