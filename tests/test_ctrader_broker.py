"""
@pyne
"""
import asyncio
import time

import pytest

from pynecore.core.broker.exceptions import (
    BrokerManualInterventionError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
    UnexpectedCancelError,
    VenueWritesQuarantinedError,
)
from pynecore.core.broker.models import (
    CANCEL_REASON_VENUE_REJECTED,
    VENUE_DRIVEN_CANCEL_REASONS,
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
from pynecore.core.broker.protection_guard import ProtectionLevels, rebaseline_after_resume
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
    # Definitive reject — the persist-first row lands terminal and closed:
    # nothing of it can reach the venue, so the run owns no live order.
    assert list(broker.store_ctx.iter_live_orders()) == []
    rows = broker.store_ctx._store._conn.execute(
        "SELECT state, closed_ts_ms FROM orders",
    ).fetchall()
    assert [(row['state'], row['closed_ts_ms'] is not None) for row in rows] == [
        ('rejected', True),
    ]


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


# === Venue-driven cancels of the bot's pending entries ====================

_FOREIGN_EXPIRY_MS = 1_700_000_900_000


def _entry_cancel_push(exec_type, *, server_event=False, expiry=None):
    """The bot's pending LIMIT entry (order 111) ended by ``exec_type``."""
    status = (_model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED
              if exec_type == _model.ProtoOAExecutionType.ORDER_EXPIRED
              else _model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED)
    order = _make_order(order_id=111, order_type=_model.ProtoOAOrderType.LIMIT,
                        status=status, limit=1.2)
    if expiry is not None:
        order.expirationTimestamp = expiry
    event = _exec_event(exec_type, order=order)
    event.isServerEvent = server_event
    return event


@pytest.mark.parametrize('exec_type, server_event', [
    (_model.ProtoOAExecutionType.ORDER_EXPIRED, False),
    (_model.ProtoOAExecutionType.ORDER_EXPIRED, True),
    (_model.ProtoOAExecutionType.ORDER_CANCELLED, True),
], ids=['expired', 'expired-server-event', 'server-cancelled'])
def __test_entry_cancel_the_venue_caused_carries_its_reason__(tmp_path, exec_type, server_event):
    """The venue expired or cancelled the bot's pending entry on its own: the
    event names the venue's reason, so the engine never runs the
    unexpected-cancel policy for it, and the retired row's audit keeps it."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker, order_id=111)
    coid = _only_live_order(broker).client_order_id

    out = broker._translate_exec_event(_entry_cancel_push(exec_type, server_event=server_event))

    assert out is not None and out.event_type == 'cancelled' and out.pine_id == 'Long'
    assert out.cancel_reason == CANCEL_REASON_VENUE_REJECTED
    assert out.cancel_reason in VENUE_DRIVEN_CANCEL_REASONS
    assert broker.store_ctx.get_order(coid).closed_ts_ms is not None
    assert _store_events(broker, 'cancelled')[-1] == {'cancel_reason': 'venue_rejected'}


@pytest.mark.parametrize('push', ['manual_cancel', 'foreign_expiry'])
def __test_entry_cancel_nothing_proves_the_venue_caused_has_no_reason__(tmp_path, push):
    """A cancel the venue does not flag as its own, or an expiry at a time the
    bot never set (someone else's edit of its order), stays unexplained: the
    engine's unexpected-cancel policy decides."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker, order_id=111)
    event = (_entry_cancel_push(_model.ProtoOAExecutionType.ORDER_CANCELLED)
             if push == 'manual_cancel'
             else _entry_cancel_push(_model.ProtoOAExecutionType.ORDER_EXPIRED,
                                     expiry=_FOREIGN_EXPIRY_MS))

    out = broker._translate_exec_event(event)

    assert out is not None and out.event_type == 'cancelled'
    assert out.cancel_reason is None


def __test_entry_expiry_the_venue_answer_showed_is_the_bots__(tmp_path):
    """An expiry the venue's answer to the bot's own write showed is the bot's,
    so the order expiring at it is the venue's mechanics."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker, order_id=111)
    answered = _make_order(order_id=111, order_type=_model.ProtoOAOrderType.LIMIT, limit=1.2)
    answered.expirationTimestamp = _FOREIGN_EXPIRY_MS
    broker._remember_own_order_attrs(_only_live_order(broker).client_order_id, answered)

    out = broker._translate_exec_event(_entry_cancel_push(
        _model.ProtoOAExecutionType.ORDER_EXPIRED, expiry=_FOREIGN_EXPIRY_MS))

    assert out is not None and out.cancel_reason == CANCEL_REASON_VENUE_REJECTED


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
    broker._settle_own_close(broker._open_own_close(52695200), 777)

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


@pytest.mark.parametrize('by', ['coid', 'label', 'foreign'])
def __test_fill_of_an_own_order_without_a_row_is_an_error__(tmp_path, by):
    """A fill of an order carrying the run's tag whose journal row was lost
    cannot be booked, but it is the bot's own: ERROR + an audit of its own."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    run_tag = broker.store_ctx.run_tag
    order = _make_order(order_id=555, executed=1000, position_id=58367450,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    if by == 'coid':
        order.clientOrderId = f"{run_tag}-7rxpdapr-0mv9ub05c-e0"
    elif by == 'label':
        order.tradeData.label = run_tag
    else:
        order.clientOrderId = 'intruder-1'
    event = _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order,
                        deal=_model.ProtoOADeal(dealId=4242, filledVolume=1000,
                                                executionPrice=1.1))

    assert broker._translate_exec_event(event) is None

    own = _store_events(broker, 'ctrader_own_fill_untracked')
    assert len(own) == (0 if by == 'foreign' else 1)
    assert len(_ignored_external(broker)) == 1


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
    """An overshooting retire clamps the counter at the row's own fill.

    The quantity to retire can exceed what the journal attributes to this run.
    The counter must saturate at ``filled_qty`` — a negative owned
    contribution would flip the row's sign in the ownership sum.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)

    broker._retire_partial_close_exposure(54302872, 15.0)

    row = broker.store_ctx.get_order('c1')
    assert (row.extras or {})['journal_exposure_retired'] == pytest.approx(10.0)


def __test_venue_close_of_a_merged_position_books_what_it_took_of_the_share__(tmp_path):
    """A NETTING SL / TP closing part of a merged position takes the bot's
    share only where it left the venue below that share.

    The bot holds 10 of a 20-unit merged position; the venue's protection
    closes 15 and leaves 5. At most 5 can still be the bot's, so 5 of its 10
    were closed: the event books 5 and the journal retires 5, not the 15 the
    venue closed for everyone.
    """
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_partial_close_event(
        position_id=54302872, deal_id=63, closed_cents=1500,
        remaining_cents=500))
    broker._flush_external_interventions()

    assert out is not None and out.leg_type is LegType.CLOSE
    assert out.fill_qty == pytest.approx(5.0)
    row = broker.store_ctx.get_order('c1')
    assert (row.extras or {})['journal_exposure_retired'] == pytest.approx(5.0)
    assert calls == []


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
        self.on_reconcile = None

    async def _dispatch_order(self, req, *, coid, context, predecessor_cancel_ids=None):
        if self.on_dispatch is not None:
            await self.on_dispatch(req)
        return await super()._dispatch_order(
            req, coid=coid, context=context,
            predecessor_cancel_ids=predecessor_cancel_ids)

    async def _reconcile(self):
        if self.on_reconcile is not None:
            await self.on_reconcile()
        return await super()._reconcile()


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


# === Bare positions: protection someone adds is an edit ===================

def _clear_bracket(broker) -> None:
    asyncio.run(broker.execute_cancel(_envelope(CancelIntent(
        pine_id='Exit', symbol='EURUSD', from_entry='L'))))


def __test_protection_added_to_a_cleared_position_is_reported__(tmp_path):
    """The bot cleared its bracket: no protection is its choice, so a stop
    someone else adds to the position changes the bot's protection."""
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _set_venue(broker, sl=1.1, tp=1.3)
    _clear_bracket(broker)

    _set_venue(broker, sl=1.0)
    _snapshot_pass(broker)
    assert calls == []
    _snapshot_pass(broker)
    _snapshot_pass(broker)

    assert len(calls) == 1
    _reason, context, kind = calls[0]
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert context['protection_key'] == f"position:{_PID}"
    assert context['expected'] == [None, None, False, None]
    assert context['observed'][0] == pytest.approx(1.0)


def __test_late_view_of_a_cleared_bracket_is_no_edit_until_the_venue_shows_it_gone__(
        tmp_path):
    """A view of the levels the bot itself just cleared may arrive late; once
    the venue was seen without them, the same levels showing again were set by
    someone else."""
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _set_venue(broker, sl=1.1, tp=1.3)
    _clear_bracket(broker)

    for _ in range(2):
        broker._translate_exec_event(_replaced_push(sl=1.1, tp=1.3))
    assert calls == []

    _set_venue(broker)
    _snapshot_pass(broker)
    for _ in range(2):
        broker._translate_exec_event(_replaced_push(sl=1.1, tp=1.3))
    assert len(calls) == 1


def __test_protection_added_after_clearing_an_anchorless_trailing_stop_is_reported__(
        tmp_path):
    """The bot's trailing stop was written outside the guard; its two-step
    clear leaves the position bare. Late views of the trailing stop and of the
    clear's own first step are the bot's; a stop added afterwards is an edit."""
    broker = _owned_broker(tmp_path)
    broker._wire = object()
    _set_venue(broker)
    asyncio.run(broker.execute_exit(_envelope(ExitIntent(
        pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
        trail_offset=0.01))))
    assert not broker.sent[-1].HasField('stopLoss')
    _set_venue(broker, sl=1.19, trailing=True)
    _clear_bracket(broker)
    assert [req.HasField('stopLoss') for req in broker.sent[-2:]] == [True, False]
    calls = _spy_quarantine(broker)

    for push in (_replaced_push(sl=1.192, trailing=True), _replaced_push(sl=1.195, trailing=True),
                 _replaced_push(sl=1.19), _replaced_push(sl=1.19)):
        broker._translate_exec_event(push)
    assert calls == []

    _set_venue(broker)
    _snapshot_pass(broker)
    _set_venue(broker, sl=1.0)
    _snapshot_pass(broker)
    _snapshot_pass(broker)
    assert len(calls) == 1


def _written_outside_the_guard(tmp_path, writer) -> tuple[_FakeBroker, float]:
    """A process wrote the trailing protection of position ``_PID`` outside
    the guard; returns the broker and the stop the venue carries."""
    broker = _owned_broker(tmp_path)
    broker._wire = object()
    _set_venue(broker)
    if writer == 'native_failsafe':
        _failsafe(broker, stop=1.08, trailing=0.01)
        return broker, 1.08
    asyncio.run(broker.execute_exit(_envelope(ExitIntent(
        pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
        trail_offset=0.01))))
    assert not broker.sent[-1].HasField('stopLoss')
    return broker, 1.19


@pytest.mark.parametrize('writer', ['anchorless_trailing', 'native_failsafe'])
def __test_late_view_of_protection_an_earlier_process_wrote_is_no_edit_after_its_clear__(
        tmp_path, writer):
    """An earlier process wrote the trailing stop outside the guard; the
    restarted bot clears it. What the clear removed, as its own snapshot read
    showed it, is the bot's: late views of the trailing stop are no edit, a
    stop added after the venue showed the position bare is."""
    first, stop = _written_outside_the_guard(tmp_path, writer)
    broker = _restart(first)
    _set_venue(broker, sl=stop, trailing=True)
    _snapshot_pass(broker)
    _clear_bracket(broker)
    calls = _spy_quarantine(broker)

    for push in (_replaced_push(sl=stop + 0.002, trailing=True),
                 _replaced_push(sl=stop + 0.005, trailing=True)):
        broker._translate_exec_event(push)
    assert calls == []

    _set_venue(broker)
    _snapshot_pass(broker)
    _set_venue(broker, sl=1.0)
    _snapshot_pass(broker)
    _snapshot_pass(broker)
    assert len(calls) == 1


def _own_second_entry_fill(broker, *, volume=2000, closed_cents=0, order_id=70211175,
                           deal_id=5252, **levels):
    """The bot's own second entry (``c2``, 10 units) fills on position
    ``_PID``; its view shows the position with ``levels``. With
    ``closed_cents`` the entry is a sell that closed that much of the long
    position (a NETTING reduce)."""
    side = 'sell' if closed_cents else 'buy'
    create_entry_order_row(
        broker.store_ctx, coid='c2', symbol='EURUSD', side=side, qty=10.0,
        intent_key='L2', pine_entry_id='L2', kind=ENTRY_KIND_POSITION, order_type='market')
    broker.store_ctx.upsert_order(
        'c2', state='confirmed', filled_qty=0.0,
        extras={'kind': 'position', 'order_type': 'market', 'order_id': str(order_id)})
    broker.store_ctx.add_ref('c2', 'order_id', str(order_id))
    deal = _model.ProtoOADeal(dealId=deal_id, filledVolume=1000, executionPrice=1.2)
    if closed_cents:
        deal.closePositionDetail.CopyFrom(_model.ProtoOAClosePositionDetail(
            entryPrice=1.1, closedVolume=closed_cents, balance=0, moneyDigits=2))
    entry = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=order_id, executed=1000, position_id=_PID,
                          side=(_model.ProtoOATradeSide.SELL if closed_cents
                                else _model.ProtoOATradeSide.BUY),
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
        deal=deal)
    entry.position.CopyFrom(_protected_position(volume=volume, **levels))
    out = broker._translate_exec_event(entry)
    assert out is not None and out.leg_type is LegType.ENTRY and out.fill_qty == 10.0


def __test_own_fill_during_a_clear_of_protection_written_outside_the_guard_is_no_edit__(
        tmp_path):
    """After a restart the guard holds nothing for a position an earlier process
    protected outside it. The bot's own add fills while the bot clears that
    protection: the fill's view is not taken as the position's protection, the
    clear is the bot's newest write, so the venue bare afterwards is no edit
    and a stop added later is."""
    first, _stop = _written_outside_the_guard(tmp_path, 'anchorless_trailing')
    broker = _HookedBroker()
    broker.store_ctx = first.store_ctx
    broker._wire = object()
    _set_venue(broker, sl=1.19, trailing=True)
    _snapshot_pass(broker)
    calls = _spy_quarantine(broker)
    fired: list = []

    async def fill_meanwhile(_req):
        if not fired:
            fired.append(True)
            _own_second_entry_fill(broker, sl=1.19, trailing=True)

    broker.on_dispatch = fill_meanwhile
    _clear_bracket(broker)
    broker.on_dispatch = None
    assert fired and _PID in broker._bare_positions

    _set_venue(broker, volume=2000)
    for _ in range(3):
        _snapshot_pass(broker)
    assert calls == []

    _set_venue(broker, volume=2000, sl=1.0)
    _snapshot_pass(broker)
    _snapshot_pass(broker)
    assert len(calls) == 1
    assert calls[0][1]['expected'] == [None, None, False, None]


def __test_own_fill_during_the_snapshot_read_of_a_clear_outside_the_guard_is_no_edit__(
        tmp_path):
    """As above, but the bot's own add fill is translated while the clear still
    reads its snapshot: the fill's view is not taken as the position's
    protection either, so the clear leaves the position bare as the bot's own
    choice."""
    first, _stop = _written_outside_the_guard(tmp_path, 'anchorless_trailing')
    broker = _HookedBroker()
    broker.store_ctx = first.store_ctx
    broker._wire = object()
    _set_venue(broker, sl=1.19, trailing=True)
    _snapshot_pass(broker)
    calls = _spy_quarantine(broker)
    reads: list = []
    fired: list = []

    async def fill_meanwhile():
        # The first read finds the position, the second is the clear's own.
        reads.append(True)
        if len(reads) == 2:
            fired.append(True)
            _own_second_entry_fill(broker, sl=1.19, trailing=True)

    broker.on_reconcile = fill_meanwhile
    _clear_bracket(broker)
    broker.on_reconcile = None
    assert fired and _PID in broker._bare_positions

    _set_venue(broker, volume=2000)
    for _ in range(3):
        _snapshot_pass(broker)
    assert calls == []

    _set_venue(broker, volume=2000, sl=1.0)
    _snapshot_pass(broker)
    _snapshot_pass(broker)
    assert len(calls) == 1
    assert calls[0][1]['expected'] == [None, None, False, None]


def _own_fill_opened(tmp_path, *, hedged=False, volume=1000, **levels) -> _FakeBroker:
    """The bot's own 10-unit entry fill opened (or merged into) position
    ``_PID``; the fill shows the position with ``levels``."""
    broker = _owned_broker(tmp_path)
    broker._hedging_enabled = hedged
    broker._wire = object()
    broker.store_ctx.upsert_order('c1', filled_qty=0.0)
    entry = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=70211174, executed=1000, position_id=_PID,
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
        deal=_model.ProtoOADeal(dealId=5151, filledVolume=1000, executionPrice=1.2))
    entry.position.CopyFrom(_protected_position(volume=volume, **levels))
    assert broker._translate_exec_event(entry) is not None
    _set_venue(broker, volume=volume, **levels)
    return broker


def _opened_bare(tmp_path, *, hedged=False) -> _FakeBroker:
    """The bot's own entry fill opened position ``_PID`` without protection."""
    broker = _own_fill_opened(tmp_path, hedged=hedged)
    assert _PID in broker._bare_positions
    return broker


@pytest.mark.parametrize('hedged', [False, True])
def __test_stop_pushed_onto_a_position_the_bot_opened_bare_is_reported__(tmp_path, hedged):
    broker = _opened_bare(tmp_path, hedged=hedged)
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_replaced_push(sl=1.0))
    assert calls == []
    broker._translate_exec_event(_replaced_push(sl=1.0))

    assert len(calls) == 1
    assert calls[0][1]['expected'] == [None, None, False, None]
    assert calls[0][1]['observed'][0] == pytest.approx(1.0)


@pytest.mark.parametrize('hedged', [False, True])
@pytest.mark.parametrize('levels', [
    {'sl': 1.0}, {'tp': 1.4}, {'sl': 1.15, 'trailing': True},
], ids=['stop-loss', 'take-profit', 'trailing-stop'])
def __test_protection_added_to_a_position_the_bot_opened_bare_is_reported__(
        tmp_path, hedged, levels):
    """A stop-loss, take-profit or trailing stop added while the stream was
    down reaches the bot through the snapshots."""
    broker = _opened_bare(tmp_path, hedged=hedged)
    calls = _spy_quarantine(broker)
    _snapshot_pass(broker)
    assert calls == []

    _set_venue(broker, **levels)
    _snapshot_pass(broker)
    assert calls == []
    _snapshot_pass(broker)

    assert len(calls) == 1


def __test_stop_the_bot_s_own_add_fill_first_shows_on_its_bare_position_is_reported__(
        tmp_path):
    """Someone stops the bare NETTING position and the first view showing it
    is the bot's own add fill. An add fill leaves the protection as it was and
    the bot sends none with its orders, so the stop is someone else's: the
    view is judged like any other, not accepted as the bot's."""
    broker = _opened_bare(tmp_path)
    calls = _spy_quarantine(broker)
    _snapshot_pass(broker)
    _snapshot_pass(broker)

    _own_second_entry_fill(broker, volume=2000, sl=1.0)
    assert calls == []
    _set_venue(broker, volume=2000, sl=1.0)
    for _ in range(3):
        _snapshot_pass(broker)

    assert len(calls) == 1
    assert calls[0][1]['expected'] == [None, None, False, None]
    assert calls[0][1]['observed'][0] == pytest.approx(1.0)
    assert _store_events(broker, 'ctrader_fill_protection_unexplained') == []


def __test_own_bracket_on_a_position_the_bot_opened_bare_is_no_edit__(tmp_path):
    """The bot attaches its own bracket to the bare position: a late view of
    the bare position and every later view of its own levels are the bot's."""
    broker = _opened_bare(tmp_path)
    calls = _spy_quarantine(broker)
    _snapshot_pass(broker)

    asyncio.run(broker.execute_exit(_envelope(ExitIntent(
        pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
        tp_price=1.3, sl_price=1.1))))
    for push in (_replaced_push(), _replaced_push(), _replaced_push(sl=1.1, tp=1.3)):
        broker._translate_exec_event(push)
    _set_venue(broker, sl=1.1, tp=1.3)
    for _ in range(3):
        _snapshot_pass(broker)

    assert calls == []
    assert _PID not in broker._bare_positions


def __test_protection_added_to_a_bare_position_after_a_restart_is_reported__(tmp_path):
    """The bot's empty protection is kept on the journal row: a restarted
    process judges the venue against it."""
    restarted = _restart(_opened_bare(tmp_path))
    calls = _spy_quarantine(restarted)
    _snapshot_pass(restarted)
    assert calls == []

    _set_venue(restarted, tp=1.4)
    _snapshot_pass(restarted)
    _snapshot_pass(restarted)

    assert len(calls) == 1


def __test_failsafe_protection_of_a_position_the_bot_opened_bare_is_left_to_the_core__(
        tmp_path):
    """The native fail-safe protects the bare position: its levels are the core
    fail-safe manager's to judge, not the plugin guard's."""
    broker = _opened_bare(tmp_path)
    _failsafe(broker)
    calls = _spy_quarantine(broker)

    _set_venue(broker, sl=1.02)
    for _ in range(3):
        _snapshot_pass(broker)

    assert calls == []


def _bad_stops_reject() -> ExchangeOrderRejectedError:
    reject = ExchangeOrderRejectedError("cTrader rejected the order (TRADING_BAD_STOPS)")
    reject.__cause__ = CTraderProtocolError('TRADING_BAD_STOPS', '')
    return reject


def _failsafe_failed_on_bare(tmp_path, failure: Exception, *, hedged: bool) -> _FakeBroker:
    """The bot opened position ``_PID`` bare; the native fail-safe write of
    SL 1.08 then failed with ``failure``."""
    broker = _opened_bare(tmp_path, hedged=hedged)
    broker._raise_on_dispatch = failure
    with pytest.raises(type(failure)):
        _failsafe(broker)
    broker._raise_on_dispatch = None
    return broker


@pytest.mark.parametrize('hedged', [False, True])
def __test_stop_firing_on_a_bare_position_after_a_refused_failsafe_is_reported__(
        tmp_path, hedged):
    """The venue refused the fail-safe write, so the position stayed bare:
    a stop firing on it was set by someone else."""
    broker = _failsafe_failed_on_bare(tmp_path, _bad_stops_reject(), hedged=hedged)
    assert _PID in broker._bare_positions
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_sltp_fill(stop=1.19))
    broker._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert len(calls) == 1


@pytest.mark.parametrize('hedged', [False, True])
def __test_stop_added_to_a_bare_position_after_a_refused_failsafe_is_reported__(
        tmp_path, hedged):
    """The refused fail-safe left the bot's empty protection in place: a stop
    someone adds is an edit, even one at the refused fail-safe level."""
    broker = _failsafe_failed_on_bare(tmp_path, _bad_stops_reject(), hedged=hedged)
    calls = _spy_quarantine(broker)
    _snapshot_pass(broker)
    assert calls == []

    _set_venue(broker, sl=1.08)
    _snapshot_pass(broker)
    _snapshot_pass(broker)

    assert len(calls) == 1


@pytest.mark.parametrize('hedged', [False, True])
@pytest.mark.parametrize(('stop', 'reported'), [(1.08, 0), (1.19, 1)],
                         ids=['failsafe-level', 'foreign-level'])
def __test_stop_firing_after_an_unknown_failsafe_on_a_bare_position_is_judged__(
        tmp_path, hedged, stop, reported):
    """The fail-safe write's outcome is unknown, so the bare position carries
    either nothing or the fail-safe stop: only the fail-safe level firing is
    the bot's."""
    broker = _failsafe_failed_on_bare(tmp_path, _unknown_amend(), hedged=hedged)
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(stop=stop))
    broker._flush_external_interventions()

    assert len(calls) == reported


def __test_protection_added_after_the_bot_closed_its_whole_bare_share_is_not_reported__(
        tmp_path):
    """The bot opened bare, someone else added 5 and the bot closed its own 10:
    a stop on the remainder protects only the other party's exposure."""
    broker = _opened_bare(tmp_path)
    calls = _spy_quarantine(broker)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=888, position_id=_PID, closing=True))
    _close(broker, qty=10.0)
    own = broker._translate_exec_event(_closing_fill(
        order_id=888, closed_cents=1000, remaining_cents=500))
    assert own is not None and own.fill_qty == pytest.approx(10.0)

    for _ in range(2):
        broker._translate_exec_event(_replaced_push(sl=1.0, volume=500))
    _set_venue(broker, sl=1.0, volume=500)
    for _ in range(3):
        _snapshot_pass(broker)

    assert calls == []


# === Protection the bot's own fill found on its position ==================

def _merged_into_protected(tmp_path, *, hedged=False) -> _FakeBroker:
    """The bot's 10-unit entry merged into a 5-unit position someone else had
    protected with SL 1.05 / TP 1.25."""
    broker = _own_fill_opened(tmp_path, hedged=hedged, volume=1500, sl=1.05, tp=1.25)
    assert _PID not in broker._bare_positions
    return broker


@pytest.mark.parametrize('hedged', [False, True])
def __test_moved_protection_the_bot_s_fill_found_on_its_position_is_reported__(
        tmp_path, hedged):
    """That protection covers the bot's position from the fill on: moving it
    changes the bot's protection."""
    broker = _merged_into_protected(tmp_path, hedged=hedged)
    calls = _spy_quarantine(broker)
    _snapshot_pass(broker)
    assert calls == []

    _set_venue(broker, volume=1500, sl=1.0, tp=1.25)
    _snapshot_pass(broker)
    _snapshot_pass(broker)

    assert len(calls) == 1
    assert calls[0][1]['expected'][:2] == pytest.approx([1.05, 1.25])


@pytest.mark.parametrize('hedged', [False, True])
def __test_protection_the_bot_s_fill_found_is_audited_only_where_no_merge_explains_it__(
        tmp_path, hedged):
    """On a HEDGED account the bot's entry opens a position of its own and
    sends no protection with it, so protection its fill view shows is
    explained by no merge: it is still taken as the venue truth, with an ERROR
    and an audit event. On NETTING the entry merged into a position someone
    else protected, which explains it."""
    broker = _merged_into_protected(tmp_path, hedged=hedged)

    audits = _store_events(broker, 'ctrader_fill_protection_unexplained')
    if hedged:
        assert len(audits) == 1
        assert audits[0]['levels'][:2] == pytest.approx([1.05, 1.25])
        assert audits[0]['hedged'] is True
    else:
        assert audits == []


def __test_protection_on_a_netting_fill_no_other_volume_explains_is_audited__(tmp_path):
    """The NETTING position the bot's fill shows holds only the bot's own 10
    units, so no merge into someone else's protected position explains its
    SL / TP (someone may have attached them to the bot's working entry): it is
    taken as the venue truth, with an ERROR and an audit event."""
    broker = _own_fill_opened(tmp_path, volume=1000, sl=1.05, tp=1.25)

    assert broker._protection_guard().known(f"position:{_PID}")
    audits = _store_events(broker, 'ctrader_fill_protection_unexplained')
    assert len(audits) == 1
    assert audits[0]['levels'][:2] == pytest.approx([1.05, 1.25])
    assert audits[0]['hedged'] is False


def __test_protection_an_earlier_own_write_explains_on_a_hedged_fill_is_not_audited__(
        tmp_path):
    """The fill's view shows the protection the bot itself wrote outside the
    guard: nothing to audit."""
    broker = _owned_broker(tmp_path)
    broker._hedging_enabled = True
    broker._remember_own_levels(f"position:{_PID}", ProtectionLevels(stop=1.05, limit=1.25))
    broker._wire = object()
    broker.store_ctx.upsert_order('c1', filled_qty=0.0)
    entry = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=70211174, executed=1000, position_id=_PID,
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
        deal=_model.ProtoOADeal(dealId=5151, filledVolume=1000, executionPrice=1.2))
    entry.position.CopyFrom(_protected_position(sl=1.05, tp=1.25))
    assert broker._translate_exec_event(entry) is not None

    assert broker._protection_guard().known(f"position:{_PID}")
    assert _store_events(broker, 'ctrader_fill_protection_unexplained') == []


def __test_removed_protection_the_bot_s_fill_found_on_its_position_is_reported__(tmp_path):
    broker = _merged_into_protected(tmp_path)
    calls = _spy_quarantine(broker)

    for _ in range(2):
        broker._translate_exec_event(_replaced_push(volume=1500, tp=1.25))

    assert len(calls) == 1


def __test_protection_the_bot_s_fill_found_firing_at_its_level_is_not_reported__(tmp_path):
    """The protection covering the bot's position fired where it stood: venue
    mechanics."""
    broker = _merged_into_protected(tmp_path)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_sltp_fill(stop=1.05))
    broker._flush_external_interventions()

    assert out is not None and calls == []


def __test_own_bracket_over_protection_the_bot_s_fill_found_is_no_edit__(tmp_path):
    """The bot replaces the protection it found with its own bracket: a late
    view of the protection it found and the views of its own are the bot's."""
    broker = _merged_into_protected(tmp_path)
    calls = _spy_quarantine(broker)

    asyncio.run(broker.execute_exit(_envelope(ExitIntent(
        pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
        tp_price=1.3, sl_price=1.1))))
    for push in (_replaced_push(volume=1500, sl=1.05, tp=1.25),
                 _replaced_push(volume=1500, sl=1.05, tp=1.25),
                 _replaced_push(volume=1500, sl=1.1, tp=1.3)):
        broker._translate_exec_event(push)
    _set_venue(broker, volume=1500, sl=1.1, tp=1.3)
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
                  stop_out=False, server_event=False, partial=False,
                  position_side=_model.ProtoOATradeSide.BUY):
    closing_side = (_model.ProtoOATradeSide.SELL
                    if position_side == _model.ProtoOATradeSide.BUY
                    else _model.ProtoOATradeSide.BUY)
    order = _make_order(order_id=order_id, executed=closed_cents,
                        volume=closed_cents, position_id=_PID, closing=True,
                        order_type=order_type, side=closing_side,
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
                                          tradeSide=position_side),
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


def _released(broker) -> list:
    """The events of the close fills an own close's answer released, in order."""
    events = list(broker._released_close_events)
    broker._released_close_events.clear()
    return events


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
    assert seen == [None], "held until the close is answered"
    seen = _released(broker)
    seen.append(broker._translate_exec_event(_closing_fill(
        order_id=889, closed_cents=1000, deal_id=9001)))
    broker._flush_external_interventions()

    assert len(seen) == 2
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


def __test_bracket_removal_the_bot_s_own_add_fill_is_the_first_to_show_is_reported__(
        tmp_path):
    """The bot's bracket is gone and the first view showing it is the bot's own
    add fill. An add fill leaves the protection as it was and no own write
    explains the bare view, so someone else removed the bracket: the view is
    judged, not accepted as the bot's, and the snapshot confirming it
    reports."""
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
    assert len(calls) == 1
    assert calls[0][1]['expected'][:2] == [pytest.approx(1.1), pytest.approx(1.3)]
    assert calls[0][1]['observed'] == [None, None, False, None]
    assert _store_events(broker, 'ctrader_fill_protection_unexplained') == []


def _stale_own_fill_view(broker) -> None:
    """A reconcile pass read the position bare before the bot's last protection
    write and booked an own fill on it."""
    asyncio.run(broker._observe_protection_snapshot(
        {_PID: _protected_position()}, read_started_at=time.time() - 5.0,
        filled_positions={_PID}))


def __test_own_fill_view_read_before_an_own_protection_write_is_not_accepted__(tmp_path):
    """The pass read the position bare, the engine attached the bracket while
    the pass went on, then the pass reached the stale view: it is not the
    bot's, so someone removing the stop later is reported."""
    broker = _guarded_broker(tmp_path, sl=1.1, tp=None)
    calls = _spy_quarantine(broker)
    _stale_own_fill_view(broker)

    _set_venue(broker, sl=1.1)
    for _ in range(2):
        _snapshot_pass(broker)
    assert calls == []

    _set_venue(broker)
    for _ in range(3):
        _snapshot_pass(broker)
    assert len(calls) == 1


@pytest.mark.parametrize('pushes', [0, 2])
def __test_own_fill_view_read_before_an_own_protection_write_is_taken_from_the_next_read__(
        tmp_path, pushes):
    """What the fill did to the protection is the bot's: the first view read
    after the bot's write stands in for the stale one, and pushes showing it
    before that read are not judged."""
    broker = _guarded_broker(tmp_path, sl=1.1, tp=None)
    calls = _spy_quarantine(broker)
    _stale_own_fill_view(broker)

    for _ in range(pushes):
        broker._translate_exec_event(_replaced_push(sl=1.1, tp=1.3))
    _set_venue(broker, sl=1.1, tp=1.3)
    for _ in range(3):
        _snapshot_pass(broker)
        broker._translate_exec_event(_replaced_push(sl=1.1, tp=1.3))

    assert calls == []


def __test_stale_own_fill_view_never_makes_a_possibly_landed_protection_bare__(tmp_path):
    """The bot's stop write got no answer, so the guard knows nothing; a fill
    view read before that write does not prove the position bare, and the
    stop that landed firing later is the bot's."""
    broker = _owned_broker(tmp_path)
    broker._wire = object()
    _set_venue(broker)
    broker._raise_on_dispatch = _unknown_amend()
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker.execute_exit(_envelope(ExitIntent(
            pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
            sl_price=1.08))))
    broker._raise_on_dispatch = None
    calls = _spy_quarantine(broker)
    _stale_own_fill_view(broker)

    _set_venue(broker, sl=1.08)
    _snapshot_pass(broker)
    broker._translate_exec_event(_sltp_fill(stop=1.08))
    broker._flush_external_interventions()

    assert calls == []


@pytest.mark.parametrize('position', ['bare', 'guarded'])
def __test_protection_change_seen_before_the_bot_s_own_add_fill_is_reported__(
        tmp_path, position):
    """Someone adds a stop to the bot's bare position, or moves the bot's stop;
    the bot's own add fills before a second view. The bot sends no protection
    with its orders, so the add does not explain the change the venue showed
    before it: the fill's view is the second observation of an edit."""
    if position == 'bare':
        broker = _opened_bare(tmp_path)
        _snapshot_pass(broker)
        foreign = {'sl': 1.0}
    else:
        broker = _guarded_broker(tmp_path)
        _set_venue(broker, sl=1.1, tp=1.3)
        _snapshot_pass(broker)
        foreign = {'sl': 1.0, 'tp': 1.3}
    calls = _spy_quarantine(broker)
    broker._translate_exec_event(_replaced_push(**foreign))
    assert calls == []

    _own_second_entry_fill(broker, **foreign)

    assert len(calls) == 1
    assert calls[0][1]['observed'][0] == pytest.approx(1.0)
    assert _store_events(broker, 'ctrader_fill_protection_unexplained') == []


def __test_stale_own_fill_view_never_hides_a_later_removal_of_the_bot_s_bracket__(tmp_path):
    """An own add fill's view predates the bot's bracket: the view proves
    nothing, and once the venue was seen carrying the bracket for two
    snapshots the view is dropped, so someone removing the bracket much later
    is reported."""
    broker = _guarded_broker(tmp_path)
    _set_venue(broker, sl=1.1, tp=1.3)
    _snapshot_pass(broker)
    calls = _spy_quarantine(broker)
    _own_second_entry_fill(broker)

    _set_venue(broker, volume=2000, sl=1.1, tp=1.3)
    for _ in range(5):
        _snapshot_pass(broker)
    assert calls == []

    _set_venue(broker, volume=2000)
    for _ in range(3):
        _snapshot_pass(broker)
    assert len(calls) == 1
    assert calls[0][1]['observed'] == [None, None, False, None]


def __test_stale_own_fill_view_of_an_earlier_own_bracket_is_no_edit__(tmp_path):
    """The bot moved its stop; a fill that executed before the move shows the
    old stop. That is the bot's own earlier level: no ERROR, no report, and it
    stops being acceptable once the venue showed the new stop."""
    broker = _guarded_broker(tmp_path, sl=1.1, tp=None)
    _set_venue(broker, sl=1.1)
    _snapshot_pass(broker)
    asyncio.run(broker.execute_exit(_envelope(ExitIntent(
        pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
        sl_price=1.12))))
    _set_venue(broker, sl=1.12)
    _snapshot_pass(broker)
    calls = _spy_quarantine(broker)

    _own_second_entry_fill(broker, sl=1.1)
    _set_venue(broker, volume=2000, sl=1.12)
    for _ in range(3):
        _snapshot_pass(broker)
    assert calls == []
    assert _store_events(broker, 'ctrader_fill_protection_unexplained') == []

    _set_venue(broker, volume=2000, sl=1.1)
    for _ in range(2):
        _snapshot_pass(broker)
    assert len(calls) == 1


def __test_protection_a_reducing_own_fill_changed_is_accepted_with_an_audit__(tmp_path):
    """On a NETTING account the bot's own opposite entry reduced its position,
    and the venue showed the position without its bracket, possibly before
    the fill event arrived. Whether the venue dropped the bracket with the
    bot's fill is not measured: nothing is reported, the ERROR and the audit
    event record it, and the venue's levels are the bot's from then on."""
    broker = _guarded_broker(tmp_path)
    broker.store_ctx.upsert_order('c1', qty=20.0, filled_qty=20.0)
    _set_venue(broker, volume=2000, sl=1.1, tp=1.3)
    _snapshot_pass(broker)
    calls = _spy_quarantine(broker)
    broker._translate_exec_event(_replaced_push(volume=2000))

    _own_second_entry_fill(broker, volume=1000, closed_cents=1000)
    _set_venue(broker, volume=1000)
    for _ in range(3):
        _snapshot_pass(broker)

    assert calls == []
    audits = _store_events(broker, 'ctrader_fill_protection_unexplained')
    assert len(audits) == 1 and audits[0]['may_have_changed'] is True

    _set_venue(broker, volume=1000, sl=1.0)
    for _ in range(2):
        _snapshot_pass(broker)
    assert len(calls) == 1


# === NETTING: only the bot's share of a merged position is the bot's ======

def _store_events(broker, kind) -> list:
    return list(broker.store_ctx.iter_events_by_kind_since(kind, 0))


def _retired(broker, coid='c1') -> float:
    return (broker.store_ctx.get_order(coid).extras or {}).get('journal_exposure_retired', 0.0)


def __test_netting_close_of_only_another_party_s_exposure_is_ignored__(tmp_path):
    """The bot holds 10 of a 15-unit merged position; someone closes 5 and
    leaves 10: the bot's share is intact, so nothing is booked or reported."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=500, remaining_cents=1000))
    broker._flush_external_interventions()

    assert out is None
    assert calls == []
    assert _retired(broker) == 0.0
    assert broker.store_ctx.get_order('c1').closed_ts_ms is None
    assert _ignored_external(broker)[-1]['reason'] == 'foreign_exposure_only'


def __test_netting_close_below_the_share_books_and_reports_the_shortfall__(tmp_path):
    """Someone closes 10 of a 15-unit merged position the bot holds 10 of:
    5 of the bot's share went, booked and reported."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=1000, remaining_cents=500))
    broker._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(5.0)
    assert _retired(broker) == pytest.approx(5.0)
    assert len(calls) == 1
    context = calls[0][1]
    assert context['booked_qty'] == pytest.approx(5.0)
    assert context['share'] == pytest.approx(10.0)
    assert context['venue_after'] == pytest.approx(5.0)


def __test_netting_venue_stop_on_a_merged_position_books_the_bot_s_share__(tmp_path):
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=1500,
        order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT))
    broker._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert broker.store_ctx.get_order('c1').closed_ts_ms is not None
    assert calls == []


def __test_netting_full_share_own_close_leaves_the_remainder_to_its_owner__(tmp_path):
    """The bot closes its whole share of a merged position someone else still
    holds part of; their later close of the remainder is theirs alone."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=888, position_id=_PID, closing=True))
    _close(broker, qty=15.0)

    own = broker._translate_exec_event(_closing_fill(
        order_id=888, closed_cents=1500, remaining_cents=500))
    assert own is not None and own.fill_qty == pytest.approx(10.0)
    assert _retired(broker) == pytest.approx(10.0)
    assert _PID not in broker._close_dispatch_pine_by_position

    foreign = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=500))
    broker._flush_external_interventions()

    assert foreign is None
    assert calls == []
    assert broker.store_ctx.get_order('c1').closed_ts_ms is not None


def __test_netting_opposite_order_of_someone_else_is_a_close_of_the_bot__(tmp_path):
    """An opposite market order that is not a closing order still closes the
    merged position: it is attributed, booked by its closed volume, reported."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    order = _make_order(order_id=70450000, executed=1500, volume=1500,
                        side=_model.ProtoOATradeSide.SELL, position_id=_PID,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    deal = _model.ProtoOADeal(
        dealId=70450001, filledVolume=1500, executionPrice=1.1,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=1000))
    event = _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order, deal=deal)
    event.position.CopyFrom(_model.ProtoOAPosition(
        positionId=_PID, positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=500,
                                          tradeSide=_model.ProtoOATradeSide.SELL)))

    out = broker._translate_exec_event(event)
    broker._flush_external_interventions()

    assert out is not None and out.leg_type is LegType.CLOSE
    assert out.fill_qty == pytest.approx(10.0)
    assert broker.store_ctx.get_order('c1').closed_ts_ms is not None
    assert len(calls) == 1


def __test_netting_share_counts_an_own_fill_only_from_its_execution__(tmp_path):
    """The journal counts the bot's add from its dispatch answer before an
    earlier foreign close is translated; that close is judged against the
    share the bot held when it executed."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=70211180, executed=1000, volume=1000, position_id=_PID,
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
        deal=_model.ProtoOADeal(dealId=70211181, filledVolume=1000, executionPrice=1.1,
                                executionTimestamp=2_000))
    asyncio.run(broker.execute_entry(_envelope(EntryIntent(
        pine_id="L2", symbol="EURUSD", side="buy", qty=10.0,
        order_type=OrderType.MARKET))))
    assert broker._own_share(_PID) == pytest.approx(20.0)

    foreign = _closing_fill(order_id=70400000, closed_cents=1000, remaining_cents=500)
    foreign.deal.executionTimestamp = 1_000
    out = broker._translate_exec_event(foreign)
    broker._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(5.0)
    assert calls[0][1]['booked_qty'] == pytest.approx(5.0)


def __test_netting_share_takes_off_an_own_close_executed_first__(tmp_path):
    """The bot's own close executed before a foreign close but is translated
    after it: the foreign close is judged against the reduced share."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    own_order = _make_order(order_id=888, executed=500, volume=500, position_id=_PID,
                            closing=True,
                            status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    own_deal = _model.ProtoOADeal(
        dealId=8881, filledVolume=500, executionPrice=1.1, executionTimestamp=1_000,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=500))
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED, order=own_order, deal=own_deal)
    broker._canned_event.position.CopyFrom(_protected_position(volume=1500))
    _close(broker, qty=5.0)

    foreign = _closing_fill(order_id=70400000, closed_cents=1000, remaining_cents=500)
    foreign.deal.executionTimestamp = 2_000
    assert broker._translate_exec_event(foreign) is None
    own = broker._translate_exec_event(broker._canned_event)
    broker._flush_external_interventions()

    assert own is not None and own.fill_qty == pytest.approx(5.0)
    assert calls == []
    assert broker._own_share(_PID) == pytest.approx(5.0)


def _entry_answer(*, executed_ms=None, side=_model.ProtoOATradeSide.BUY):
    """The venue's answer to an own MARKET entry of 10 that filled on ``_PID``."""
    deal = _model.ProtoOADeal(dealId=81000001, filledVolume=1000, executionPrice=1.2)
    if executed_ms is not None:
        deal.executionTimestamp = executed_ms
    return _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=81000000, executed=1000, price=1.2, position_id=_PID,
                          side=side, status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
        deal=deal)


def _refused_entry() -> ExchangeOrderRejectedError:
    reject = ExchangeOrderRejectedError("cTrader rejected the order (TRADING_DISABLED)")
    reject.__cause__ = CTraderProtocolError('TRADING_DISABLED', '')
    return reject


def _close_during_entry(tmp_path, *, close, side, answer=None, failure=None,
                        order_type=OrderType.MARKET):
    """Translate ``close`` while an own NETTING entry of 10 is in flight."""
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    calls = _spy_quarantine(broker)
    seen: list = []

    async def close_in_flight(_req):
        seen.append(broker._translate_exec_event(close))

    broker.on_dispatch = close_in_flight
    broker._canned_event = answer
    broker._raise_on_dispatch = failure
    try:
        asyncio.run(broker.execute_entry(_envelope(EntryIntent(
            pine_id="E", symbol="EURUSD", side=side, qty=10.0, order_type=order_type,
            limit=1.3 if order_type is OrderType.LIMIT else None))))
    except (OrderDispositionUnknownError, ExchangeOrderRejectedError):
        pass
    broker._flush_external_interventions()
    return broker, seen[0], calls


def _foreign_close(*, closed_cents, remaining_cents, executed_ms=2_000):
    fill = _closing_fill(order_id=70400000, closed_cents=closed_cents,
                         remaining_cents=remaining_cents)
    fill.deal.executionTimestamp = executed_ms
    return fill


@pytest.mark.parametrize('answer, failure, reported, ambiguous', [
    # The add had not executed when the venue answered.
    (None, None, False, False),
    # The add executed on the position before the close: the bot held 20.
    (_entry_answer(executed_ms=1_000), None, True, False),
    # The add executed after the close.
    (_entry_answer(executed_ms=3_000), None, False, False),
    # The add's fill carries no time: the order against the close is unknown.
    (_entry_answer(), None, False, True),
    # The add may have landed: its effect is unknown.
    (None, OrderDispositionUnknownError("entry timed out", client_order_id="x"), False, True),
    # The add was provably refused.
    (None, _refused_entry(), False, False),
])
def __test_netting_close_during_an_own_add_is_judged_on_the_add_s_answer__(
        tmp_path, answer, failure, reported, ambiguous):
    """The bot's add may already have executed when another party closed 5 of
    the merged position and left 10: the close took the bot's share only if
    the add came first. The verdict waits for the add's answer; while the
    evidence still cannot decide, an ERROR and an audit event, no report."""
    broker, out, calls = _close_during_entry(
        tmp_path, close=_foreign_close(closed_cents=500, remaining_cents=1000),
        side="buy", answer=answer, failure=failure)

    assert out is None, "booked as if the add came later: nothing of the bot's 10 was taken"
    assert len(calls) == (1 if reported else 0)
    audits = _store_events(broker, 'ctrader_close_share_ambiguous')
    assert len(audits) == (1 if ambiguous else 0)
    if ambiguous:
        assert audits[0]['high'] == pytest.approx(5.0)
    assert broker._held_close_verdicts == []


def _own_add_fill(*, executed_cents, deal_id, filled_cents, venue_cents, partial):
    """A fill of the bot's NETTING add of 10 on ``_PID``, executed at 1_000."""
    order = _make_order(order_id=81000000, executed=executed_cents, price=1.2,
                        position_id=_PID)
    deal = _model.ProtoOADeal(dealId=deal_id, filledVolume=filled_cents, executionPrice=1.2,
                              executionTimestamp=1_000)
    event = _exec_event(_model.ProtoOAExecutionType.ORDER_PARTIAL_FILL if partial
                        else _model.ProtoOAExecutionType.ORDER_FILLED, order=order, deal=deal)
    event.position.CopyFrom(_model.ProtoOAPosition(
        positionId=_PID, positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=venue_cents,
                                          tradeSide=_model.ProtoOATradeSide.BUY)))
    return event


@pytest.mark.parametrize('pushed_cents, answer_cents, closed_cents, venue_after, share', [
    # The whole add was pushed before its answer (bot 20, other party 5): the
    # other party closed its own 5, and the answer repeats the pushed deal.
    (1000, 1000, 500, 2000, None),
    # 4 of the add pushed, the rest executed too (bot 20): closing 4 of the
    # other party's 5 left the bot's 20.
    (400, 1000, 400, 2100, None),
    # 4 pushed, the answer shows 7 executed in all (bot 17): closing 6 left
    # 16, one unit of the bot's share.
    (400, 700, 600, 1600, 17.0),
])
def __test_netting_add_fill_pushed_before_its_answer_is_counted_once__(
        tmp_path, pushed_cents, answer_cents, closed_cents, venue_after, share):
    """Bot 10 + other party 5. The bot's add of 10 executes before another
    party's close, and its fill push overtakes the dispatch answer: the journal
    counts the pushed part already, so the close is judged against it once,
    never again from the answer."""
    broker = _merged_broker(tmp_path, venue_cents=1500, share=10.0)
    calls = _spy_quarantine(broker)
    venue_before = 1500 + answer_cents

    async def push_and_close_in_flight(req):
        push = _own_add_fill(executed_cents=pushed_cents, deal_id=81000001,
                             filled_cents=pushed_cents, venue_cents=1500 + pushed_cents,
                             partial=pushed_cents < 1000)
        push.order.clientOrderId = req.clientOrderId
        broker._translate_exec_event(push)
        broker._translate_exec_event(_foreign_close(
            closed_cents=closed_cents, remaining_cents=venue_before - closed_cents))

    broker.on_dispatch = push_and_close_in_flight
    answer_deal = 81000001 if answer_cents == pushed_cents else 81000002
    broker._canned_event = _own_add_fill(
        executed_cents=answer_cents, deal_id=answer_deal,
        filled_cents=answer_cents - (0 if answer_deal == 81000001 else pushed_cents),
        venue_cents=venue_before, partial=answer_cents < 1000)
    assert venue_before - closed_cents == venue_after
    asyncio.run(broker.execute_entry(_envelope(EntryIntent(
        pine_id="E", symbol="EURUSD", side="buy", qty=10.0, order_type=OrderType.MARKET))))
    broker._flush_external_interventions()

    assert broker._held_close_verdicts == []
    assert _store_events(broker, 'ctrader_close_share_ambiguous') == []
    if share is None:
        assert calls == []
    else:
        assert len(calls) == 1
        assert calls[0][1]['share'] == pytest.approx(share)


@pytest.mark.parametrize('order_type', [OrderType.LIMIT, OrderType.MARKET])
def __test_netting_own_sl_during_an_opposite_entry_books_the_whole_share__(
        tmp_path, order_type):
    """The bot's own SL closes its whole position while an opposite own entry
    is in flight. The close is booked as if the entry came later, so the
    engine never ends below the venue; the row is retired."""
    sl = _closing_fill(order_id=70600000, closed_cents=1000, remaining_cents=0,
                       order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT)
    broker, out, calls = _close_during_entry(tmp_path, close=sl, side="sell",
                                             order_type=order_type)

    assert out is not None and out.leg_type is LegType.CLOSE
    assert out.fill_qty == pytest.approx(10.0)
    assert broker.store_ctx.get_order('c1').closed_ts_ms is not None
    assert calls == []


@pytest.mark.parametrize('answer, reported', [
    # The entry had not executed: the close took the bot's 10.
    (None, True),
    # The entry executed first and flattened the bot's share: the close took
    # only another party's 10.
    (_entry_answer(executed_ms=1_000, side=_model.ProtoOATradeSide.SELL), False),
])
def __test_netting_foreign_full_close_during_an_opposite_entry_is_judged_on_its_answer__(
        tmp_path, answer, reported):
    """Another party closes the whole position while an opposite own entry is
    in flight: the event books the bot's share, and the report follows the
    entry's answer, never lost with the retired row."""
    broker, out, calls = _close_during_entry(
        tmp_path, close=_foreign_close(closed_cents=1000, remaining_cents=0),
        side="sell", answer=answer)

    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert len(calls) == (1 if reported else 0)
    assert _store_events(broker, 'ctrader_close_share_ambiguous') == []
    assert 70400001 in broker._judged_close_deals


@pytest.mark.parametrize('order_type, closed, reported', [
    (_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT, 1000, False),
    (_model.ProtoOAOrderType.MARKET, 400, True),
])
def __test_hedged_closes_of_the_bot_s_own_leg_are_booked_whole__(
        tmp_path, order_type, closed, reported):
    """A HEDGED leg the bot alone holds: its share is the whole leg, so every
    close takes the bot's volume, and someone else's is reported."""
    broker = _owned_broker(tmp_path)
    broker._hedging_enabled = True
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=closed, remaining_cents=1000 - closed,
        order_type=order_type))
    broker._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(closed / 100)
    assert len(calls) == (1 if reported else 0)


@pytest.mark.parametrize('closed, remaining, booked', [(500, 1000, None), (1000, 500, 5.0)])
def __test_hedged_close_follows_the_bot_s_share_of_its_leg__(
        tmp_path, closed, remaining, booked):
    """Another client's order added 5 to the bot's 10-unit leg. Its close of
    exactly that add leaves the bot's share and is no intervention; a close
    below the share is booked for the shortfall and reported."""
    broker = _owned_broker(tmp_path)
    broker._hedging_enabled = True
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=closed, remaining_cents=remaining))
    broker._flush_external_interventions()

    if booked is None:
        assert out is None
        assert calls == []
        assert _retired(broker) == 0.0
    else:
        assert out is not None and out.fill_qty == pytest.approx(booked)
        assert len(calls) == 1 and calls[0][1]['booked_qty'] == pytest.approx(booked)


# === Protection that fired: the bot's own levels only =====================

def _sltp_fill(*, stop=None, limit=None, order_id=70600000):
    fill = _closing_fill(order_id=order_id,
                         order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT)
    if stop is not None:
        fill.order.stopPrice = stop
    if limit is not None:
        fill.order.limitPrice = limit
    return fill


def __test_protection_moved_and_fired_before_any_observation_is_reported__(tmp_path):
    """Someone moves the bot's SL into the market: it fires before the bot
    could observe the move. The fill is venue truth; the level was not the
    bot's."""
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_sltp_fill(stop=1.05))
    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert calls == []
    broker._flush_external_interventions()

    assert len(calls) == 1
    assert calls[0][1]['fired_stop'] == pytest.approx(1.05)


@pytest.mark.parametrize('levels', [{'stop': 1.1}, {'limit': 1.3}, {'stop': 1.1, 'limit': 1.3}])
def __test_own_protection_firing_is_not_reported__(tmp_path, levels):
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(**levels))
    broker._flush_external_interventions()

    assert calls == []


def __test_own_protection_moved_by_the_bot_fires_at_an_earlier_own_level__(tmp_path):
    """A level the bot set earlier on the position is still the bot's."""
    broker = _guarded_broker(tmp_path)
    new = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                     side="sell", qty=10.0, tp_price=1.4, sl_price=1.15)
    old = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                     side="sell", qty=10.0, tp_price=1.3, sl_price=1.1)
    asyncio.run(broker.modify_exit(_envelope(old), _envelope(new, run_tag="cd34")))
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(stop=1.1))
    broker._flush_external_interventions()

    assert calls == []


def __test_own_trailing_stop_firing_where_it_trailed_to_is_not_reported__(tmp_path):
    broker = _FakeBroker()
    broker._last_bid = 1.2
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker)
    asyncio.run(broker.execute_exit(_envelope(ExitIntent(
        pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
        trail_offset=0.01))))
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(stop=1.234))
    broker._flush_external_interventions()

    assert calls == []


def __test_protection_firing_on_a_position_the_bot_left_bare_is_reported__(tmp_path):
    broker = _guarded_broker(tmp_path)
    _set_venue(broker, sl=1.1, tp=1.3)
    asyncio.run(broker.execute_cancel(_envelope(CancelIntent(
        pine_id='Exit', symbol='EURUSD', from_entry='L'))))
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(stop=1.0))
    broker._flush_external_interventions()

    assert len(calls) == 1
    assert calls[0][1]['bare'] is True


def __test_protection_firing_on_a_position_the_bot_opened_bare_is_reported__(tmp_path):
    broker = _owned_broker(tmp_path)
    broker.store_ctx.upsert_order('c1', filled_qty=0.0)
    entry = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=70211174, executed=1000, position_id=_PID,
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
        deal=_model.ProtoOADeal(dealId=5151, filledVolume=1000, executionPrice=1.2))
    entry.position.CopyFrom(_protected_position(volume=1000))
    assert broker._translate_exec_event(entry) is not None
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(limit=1.25))
    broker._flush_external_interventions()

    assert len(calls) == 1


def __test_protection_firing_unreported_levels_after_a_foreign_view_is_audited__(tmp_path):
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._translate_exec_event(_replaced_push(sl=1.05, tp=1.3))

    fill = _sltp_fill()
    fill.ClearField('position')
    broker._translate_exec_event(fill)
    broker._flush_external_interventions()

    assert calls == []
    assert len(_store_events(broker, 'ctrader_protection_fired_unconfirmed')) == 1


def _resumed_after_an_operator_move(tmp_path, reassert_failure):
    """The operator moved the bot's SL (1.1 -> 1.05), the bot quarantined and
    the operator resumed it. The restarted bot re-asserts its own exit once;
    ``reassert_failure`` (if any) is how that write failed."""
    broker = _guarded_broker(tmp_path, sl=1.1, tp=1.3)
    rebaseline_after_resume(broker.store_ctx)
    restarted = _FakeBroker()
    restarted.store_ctx = broker.store_ctx
    restarted._wire = object()
    _set_venue(restarted, sl=1.05, tp=1.3)
    calls = _spy_quarantine(restarted)
    restarted._raise_on_dispatch = reassert_failure
    intent = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                        side="sell", qty=10.0, tp_price=1.3, sl_price=1.1)
    if reassert_failure is None:
        asyncio.run(restarted.modify_exit(_envelope(intent), _envelope(intent, run_tag="cd34")))
    else:
        with pytest.raises(type(reassert_failure)):
            asyncio.run(restarted.modify_exit(_envelope(intent),
                                              _envelope(intent, run_tag="cd34")))
    restarted._raise_on_dispatch = None
    return restarted, calls


@pytest.mark.parametrize('failure', ['refused', 'unknown'])
def __test_operator_accepted_stop_firing_after_a_failed_reassert_is_not_reported__(
        tmp_path, failure):
    """The re-assert never landed, and the stop the operator left (accepted
    by the resume) fires before any venue view was taken as the baseline."""
    restarted, calls = _resumed_after_an_operator_move(
        tmp_path, _refused_amend() if failure == 'refused' else _unknown_amend())

    out = restarted._translate_exec_event(_sltp_fill(stop=1.05, limit=1.3))
    restarted._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert calls == []
    assert len(_store_events(restarted, 'ctrader_protection_fired_after_resume')) == 1


def __test_foreign_stop_firing_after_a_confirmed_reassert_is_still_reported__(tmp_path):
    """Once the bot's re-assert is confirmed, the venue carried the bot's own
    levels: a stop it never set firing after that is someone else's."""
    restarted, calls = _resumed_after_an_operator_move(tmp_path, None)

    restarted._translate_exec_event(_sltp_fill(stop=1.05, limit=1.3))
    restarted._flush_external_interventions()

    assert len(calls) == 1
    assert calls[0][1]['fired_stop'] == pytest.approx(1.05)


# === Own closes: claims settled by the venue's answer =====================

def __test_fill_taken_for_an_own_close_but_of_another_order_is_reported__(tmp_path):
    """A foreign close fills while the bot's close is in flight; the answer
    names the bot's close order, so the taken fill was someone else's."""
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    calls = _spy_quarantine(broker)

    async def foreign_fill_in_flight(_req):
        out = broker._translate_exec_event(_closing_fill(
            order_id=70700000, closed_cents=400, remaining_cents=600))
        assert out is None, "held until the close is answered"

    broker.on_dispatch = foreign_fill_in_flight
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=889, position_id=_PID, closing=True))
    _close(broker, qty=4.0)
    released = _released(broker)
    broker._flush_external_interventions()

    assert len(released) == 1 and released[0].leg_type is LegType.CLOSE
    assert released[0].fill_qty == pytest.approx(4.0)
    assert len(calls) == 1
    assert calls[0][1]['order_id'] == 70700000
    assert calls[0][1]['claimed_as_own'] is True


def __test_fill_taken_for_a_refused_own_close_is_reported__(tmp_path):
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    calls = _spy_quarantine(broker)

    async def foreign_fill_in_flight(_req):
        broker._translate_exec_event(_closing_fill(order_id=70700000))
        broker._raise_on_dispatch = _position_locked_reject()

    broker.on_dispatch = foreign_fill_in_flight
    with pytest.raises(OrderSkippedByPlugin):
        _close(broker)
    broker._flush_external_interventions()

    assert len(calls) == 1 and calls[0][1]['order_id'] == 70700000


def __test_fill_taken_for_an_own_close_of_unknown_outcome_is_audited__(tmp_path):
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._raise_on_dispatch = OrderDispositionUnknownError("timed out", client_order_id="x")
    with pytest.raises(OrderDispositionUnknownError):
        _close(broker)
    broker._raise_on_dispatch = None

    out = broker._translate_exec_event(_closing_fill(order_id=890))
    broker._flush_external_interventions()

    assert out is not None and out.leg_type is LegType.CLOSE
    assert calls == []
    audits = _store_events(broker, 'ctrader_close_claim_ambiguous')
    assert len(audits) == 1 and audits[0]['order_id'] == 890
    assert _PID in broker._ambiguous_closes


def __test_own_close_fill_after_the_grace_is_still_the_bot_s__(tmp_path):
    """A close the venue named stays the bot's at any age: a fill pushed after
    a long reconnect is never a manual close."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=888, position_id=_PID, closing=True))
    _close(broker)
    for record in broker._own_closes:
        record.expires_at = time.monotonic() - 1.0

    broker._translate_exec_event(_closing_fill(order_id=888))
    broker._flush_external_interventions()

    assert calls == []


def __test_server_initiated_close_is_booked_and_audited__(tmp_path):
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_closing_fill(order_id=70800000, server_event=True))
    broker._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert calls == []
    assert len(_store_events(broker, 'ctrader_server_close')) == 1


# === Pending entry orders: someone else's amend ===========================

def _replaced_entry(*, limit, volume=1000, order_id=111):
    order = _make_order(order_id=order_id, volume=volume, limit=limit,
                        order_type=_model.ProtoOAOrderType.LIMIT)
    return _exec_event(_model.ProtoOAExecutionType.ORDER_REPLACED, order=order)


@pytest.mark.parametrize('limit, volume', [(1.15, 1000), (1.2, 2000)])
def __test_foreign_amend_of_a_working_entry_is_reported_at_once__(tmp_path, limit, volume):
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    calls = _spy_quarantine(broker)

    assert broker._translate_exec_event(_replaced_entry(limit=limit, volume=volume)) is None

    assert len(calls) == 1
    assert calls[0][1]['order_id'] == 111
    broker._translate_exec_event(_replaced_entry(limit=limit, volume=volume))
    assert len(calls) == 1


def __test_own_amend_of_a_working_entry_and_its_echoes_are_not_reported__(tmp_path):
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    calls = _spy_quarantine(broker)

    async def echo_in_flight(_req):
        broker._translate_exec_event(_replaced_entry(limit=1.18))

    broker.on_dispatch = echo_in_flight
    old = EntryIntent(pine_id='Long', symbol='EURUSD', side='buy', qty=10.0,
                      order_type=OrderType.LIMIT, limit=1.20)
    new = EntryIntent(pine_id='Long', symbol='EURUSD', side='buy', qty=10.0,
                      order_type=OrderType.LIMIT, limit=1.18)
    asyncio.run(broker.modify_entry(_envelope(old), _envelope(new, run_tag='cd34')))
    broker.on_dispatch = None
    broker._translate_exec_event(_replaced_entry(limit=1.18))
    broker._translate_exec_event(_replaced_entry(limit=1.2))

    assert calls == []


def __test_no_venue_write_leaves_while_a_queued_report_waits__(tmp_path):
    """The report of a manual close follows its fill event; until the policy
    ran, the intervention may require the quarantine, so no write leaves."""
    broker = _GatedBroker()
    broker.venue_writes_blocked = lambda: False
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    calls = _spy_quarantine(broker)
    assert broker._translate_exec_event(_closing_fill(order_id=70400000)) is not None

    req = _oa.ProtoOANewOrderReq(ctidTraderAccountId=999, symbolId=1, volume=1000)
    with pytest.raises(VenueWritesQuarantinedError):
        asyncio.run(broker._dispatch_order(req, coid='coid-q', context='entry'))
    assert _sent_writes(broker) == [] and calls == []

    broker._flush_external_interventions()
    assert len(calls) == 1
    assert broker._unhandled_close_reports == set()


# === NETTING: a position whose share the bot gave up is not the bot's ====

def __test_foreign_protection_firing_after_the_bot_closed_its_whole_share_is_not_reported__(
        tmp_path):
    """The bot opened bare, someone else added 5 and the bot closed its own 10:
    the other party's own SL closing its 5 touches nothing of the bot's."""
    broker = _owned_broker(tmp_path)
    broker.store_ctx.upsert_order('c1', filled_qty=0.0)
    entry = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=70211174, executed=1000, position_id=_PID,
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
        deal=_model.ProtoOADeal(dealId=5151, filledVolume=1000, executionPrice=1.2))
    entry.position.CopyFrom(_protected_position(volume=1000))
    assert broker._translate_exec_event(entry) is not None
    assert _PID in broker._bare_positions
    calls = _spy_quarantine(broker)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=888, position_id=_PID, closing=True))
    _close(broker, qty=10.0)
    own = broker._translate_exec_event(_closing_fill(
        order_id=888, closed_cents=1000, remaining_cents=500))
    assert own is not None and own.fill_qty == pytest.approx(10.0)

    foreign_stop = _closing_fill(
        order_id=70600000, closed_cents=500,
        order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT)
    foreign_stop.order.stopPrice = 1.0
    out = broker._translate_exec_event(foreign_stop)
    broker._flush_external_interventions()

    assert out is None
    assert calls == []


def __test_foreign_protection_edit_after_the_bot_closed_its_whole_share_is_not_reported__(
        tmp_path):
    """The bot's guarded bracket sat on a merged position; once the bot closed
    its whole share, moving the SL protects only the other party's remainder."""
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=888, position_id=_PID, closing=True))
    _close(broker, qty=10.0)
    own = broker._translate_exec_event(_closing_fill(
        order_id=888, closed_cents=1000, remaining_cents=500))
    assert own is not None and own.fill_qty == pytest.approx(10.0)

    for _ in range(2):
        broker._translate_exec_event(_replaced_push(sl=1.05, tp=1.3, volume=500))
    _set_venue(broker, sl=1.05, tp=1.3, volume=500)
    for _ in range(3):
        _snapshot_pass(broker)
    broker._flush_external_interventions()

    assert calls == []


# === Protection the bot wrote outside the guard is never "bare" ===========

@pytest.mark.parametrize('writer', ['native_failsafe', 'anchorless_trailing'])
def __test_a_stale_own_fill_view_never_makes_unguarded_protection_bare__(tmp_path, writer):
    """An own fill executed before the bot's unguarded protection landed shows
    the position without it; that protection firing later is the bot's own."""
    broker = _owned_broker(tmp_path)
    broker._wire = object()
    _set_venue(broker)
    if writer == 'native_failsafe':
        asyncio.run(broker.publish_native_failsafe_sl(NativeBracketSnapshot(
            parent_entry_dispatch_ref='c1', symbol='EURUSD', parent_side='long',
            stop_level=1.08, profit_level=None, trailing_stop=None, generation=1)))
    else:
        asyncio.run(broker.execute_exit(_envelope(ExitIntent(
            pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
            trail_offset=0.01))))
        assert not broker.sent[-1].HasField('stopLoss')
    calls = _spy_quarantine(broker)
    broker.store_ctx.upsert_order('c1', filled_qty=5.0)
    stale = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=70211174, executed=1000, position_id=_PID,
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
        deal=_model.ProtoOADeal(dealId=5152, filledVolume=500, executionPrice=1.2))
    stale.position.CopyFrom(_protected_position(volume=1000))
    assert broker._translate_exec_event(stale) is not None

    broker._translate_exec_event(_sltp_fill(stop=1.08))
    broker._flush_external_interventions()

    assert calls == []


def _failsafe(broker, *, stop=1.08, profit=None, trailing=None, generation=1):
    asyncio.run(broker.publish_native_failsafe_sl(NativeBracketSnapshot(
        parent_entry_dispatch_ref='c1', symbol='EURUSD', parent_side='long',
        stop_level=stop, profit_level=profit, trailing_stop=trailing,
        generation=generation)))


def _failsafe_broker(tmp_path, *, hedged=False, broker=None):
    """A broker owning position ``_PID`` that the native fail-safe protects."""
    broker = broker if broker is not None else _owned_broker(tmp_path)
    if broker.store_ctx is None:
        _open_store(tmp_path, broker)
        _seed_owned_entry(broker)
    broker._wire = object()
    broker._hedging_enabled = hedged
    _set_venue(broker)
    return broker


@pytest.mark.parametrize('hedged', [False, True])
def __test_moved_native_failsafe_stop_that_fires_is_reported__(tmp_path, hedged):
    """Someone moves the fail-safe stop into the market and it fires before
    the core fail-safe manager observes the edit: the fill is venue truth,
    the level was not the bot's."""
    broker = _failsafe_broker(tmp_path, hedged=hedged)
    _failsafe(broker)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_sltp_fill(stop=1.19))
    broker._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert len(calls) == 1
    assert calls[0][1]['fired_stop'] == pytest.approx(1.19)
    assert calls[0][1]['own_levels'] == [[1.08, None, False, None]]


def __test_take_profit_someone_added_to_a_native_failsafe_leg_is_reported__(tmp_path):
    broker = _failsafe_broker(tmp_path, hedged=True)
    _failsafe(broker)
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(stop=1.08, limit=1.12))
    broker._flush_external_interventions()

    assert len(calls) == 1


@pytest.mark.parametrize('writes, fired', [
    # The fail-safe's own stop.
    ([{}], {'stop': 1.08}),
    # Its own take-profit, as either field.
    ([{'profit': 1.3}], {'limit': 1.3}),
    ([{'profit': 1.3}], {'stop': 1.3}),
    # A trailing fail-safe stop fires where it trailed to.
    ([{'trailing': 0.01}], {'stop': 1.12}),
    # An earlier fail-safe level of the bot, before a republish landed.
    ([{}, {'stop': 1.09, 'generation': 2}], {'stop': 1.08}),
])
def __test_native_failsafe_own_level_firing_is_not_reported__(tmp_path, writes, fired):
    broker = _failsafe_broker(tmp_path, hedged=True)
    for write in writes:
        _failsafe(broker, **write)
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(**fired))
    broker._flush_external_interventions()

    assert calls == []


def __test_bracket_level_still_on_the_venue_after_a_failsafe_write_is_the_bot_s__(tmp_path):
    """The bot's own bracket was on the position when the fail-safe took over;
    a TP of that bracket firing is the bot's, whichever write the venue held."""
    broker = _guarded_broker(tmp_path)
    _failsafe(broker)
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(limit=1.3))
    broker._flush_external_interventions()

    assert calls == []


def __test_native_failsafe_leg_is_not_judged_before_a_write_landed__(tmp_path):
    """Before any fail-safe write of this process landed, the venue may carry
    protection this process never wrote: nothing firing is judged; once one
    landed, a level the bot never set is."""
    broker = _failsafe_broker(tmp_path, hedged=True)
    reject = ExchangeOrderRejectedError("cTrader rejected the order (TRADING_BAD_STOPS)")
    reject.__cause__ = CTraderProtocolError('TRADING_BAD_STOPS', '')
    broker._raise_on_dispatch = reject
    with pytest.raises(ExchangeOrderRejectedError):
        _failsafe(broker)
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(stop=1.19))
    broker._flush_external_interventions()
    assert calls == []


def __test_native_failsafe_leg_is_not_judged_while_a_write_is_in_flight__(tmp_path):
    broker = _failsafe_broker(tmp_path, hedged=True,
                              broker=_HookedBroker(reconcile=_open_eurusd_position(_PID)))
    _failsafe(broker)
    calls = _spy_quarantine(broker)
    seen: list = []

    async def fires_in_flight(_req):
        seen.append(broker._translate_exec_event(_sltp_fill(stop=1.19)))

    broker.on_dispatch = fires_in_flight
    _failsafe(broker, stop=1.09, generation=2)
    broker._flush_external_interventions()

    assert seen[0] is not None and calls == []


# === Native fail-safe write over the bot's own bracket ====================

def _failsafe_failed_over_bracket(broker, failure: Exception):
    """The native fail-safe write of SL 1.08 over the bot's SL 1.1 / TP 1.3
    bracket fails with ``failure``."""
    broker._raise_on_dispatch = failure
    with pytest.raises(type(failure)):
        _failsafe(broker)
    broker._raise_on_dispatch = None
    return broker


@pytest.mark.parametrize('restart', [False, True], ids=['same-process', 'restarted'])
def __test_stop_moved_after_a_refused_failsafe_over_the_bot_s_bracket_is_reported__(
        tmp_path, restart):
    """The venue refused the fail-safe write, so the bot's bracket stayed on
    the position and stays guarded, across a restart too."""
    broker = _failsafe_failed_over_bracket(_guarded_broker(tmp_path), _bad_stops_reject())
    if restart:
        broker = _restart(broker)
    calls = _spy_quarantine(broker)

    _set_venue(broker, sl=1.05, tp=1.3)
    _snapshot_pass(broker)
    _snapshot_pass(broker)

    assert len(calls) == 1
    assert calls[0][1]['observed'][0] == pytest.approx(1.05)


@pytest.mark.parametrize('restart', [False, True], ids=['same-process', 'restarted'])
def __test_moved_stop_firing_after_a_refused_failsafe_over_the_bot_s_bracket_is_reported__(
        tmp_path, restart):
    broker = _failsafe_failed_over_bracket(_guarded_broker(tmp_path), _bad_stops_reject())
    if restart:
        broker = _restart(broker)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_sltp_fill(stop=1.05))
    broker._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert len(calls) == 1


@pytest.mark.parametrize('restart_first', [False, True], ids=['same-process', 'restarted'])
@pytest.mark.parametrize(('fired', 'reported'), [
    ({'stop': 1.1}, 0),
    ({'limit': 1.3}, 0),
    ({'stop': 1.08}, 0),
    ({'stop': 1.05}, 1),
], ids=['bracket-stop', 'bracket-take-profit', 'failsafe-stop', 'foreign-stop'])
def __test_protection_firing_after_an_unknown_failsafe_over_the_bot_s_bracket_is_judged__(
        tmp_path, restart_first, fired, reported):
    """The fail-safe write's outcome is unknown, so the venue carries either
    the bot's bracket (known only from the journal after a restart) or the
    fail-safe stop: a level of either firing is the bot's, any other is not."""
    broker = _guarded_broker(tmp_path)
    if restart_first:
        broker = _restart(broker)
    _failsafe_failed_over_bracket(broker, _unknown_amend())
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(**fired))
    broker._flush_external_interventions()

    assert len(calls) == reported


def __test_failsafe_levels_seen_while_its_write_is_in_flight_are_no_edit__(tmp_path):
    """The fail-safe write lands before its answer arrives: snapshots and a
    push showing its levels meanwhile are not judged against the bracket."""
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker)
    asyncio.run(broker.execute_exit(_envelope(ExitIntent(
        pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
        tp_price=1.3, sl_price=1.1))))
    calls = _spy_quarantine(broker)

    async def landed_before_the_answer(_req):
        _set_venue(broker, sl=1.08)
        for _ in range(3):
            _ = [e async for e in broker._reconcile_snapshot()]
        broker._translate_exec_event(_replaced_push(sl=1.08))

    broker.on_dispatch = landed_before_the_answer
    _failsafe(broker)
    broker.on_dispatch = None
    for _ in range(3):
        _snapshot_pass(broker)

    assert calls == []


# === NETTING: the bot's share follows the side of its own net =============

_SELL = _model.ProtoOATradeSide.SELL


def _own_opposite_fill(broker, *, order_id, cents, closed_cents, venue_cents, venue_side,
                       pine_id='S', side='sell'):
    """Dispatch the bot's own opposite MARKET entry; its fill PUSH follows the answer."""
    trade_side = _SELL if side == 'sell' else _model.ProtoOATradeSide.BUY
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=order_id, volume=cents, side=trade_side, position_id=_PID))
    asyncio.run(broker.execute_entry(_envelope(EntryIntent(
        pine_id=pine_id, symbol="EURUSD", side=side, qty=cents / 100,
        order_type=OrderType.MARKET))))
    order = _make_order(order_id=order_id, executed=cents, volume=cents, side=trade_side,
                        position_id=_PID,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    deal = _model.ProtoOADeal(
        dealId=order_id + 1, filledVolume=cents, executionPrice=1.1,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=closed_cents))
    event = _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order, deal=deal)
    event.position.CopyFrom(_model.ProtoOAPosition(
        positionId=_PID, positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=venue_cents,
                                          tradeSide=venue_side)))
    out = broker._translate_exec_event(event)
    assert out is not None and out.leg_type is LegType.ENTRY and out.pine_id == pine_id
    return out


def __test_netting_share_after_an_own_flip_is_the_net_on_the_new_side__(tmp_path):
    """The bot flips its long 10 with a sell 20 that keeps the positionId; someone
    else adds a short 5 and later closes only that 5: nothing of the bot's."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _own_opposite_fill(broker, order_id=70211190, cents=2000, closed_cents=1000,
                       venue_cents=1000, venue_side=_SELL)
    assert broker._own_share(_PID) == pytest.approx(10.0)
    assert broker._share_side(_PID) == 'sell'

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=500, remaining_cents=1000, position_side=_SELL))
    broker._flush_external_interventions()

    assert out is None
    assert calls == []
    assert broker._own_share(_PID) == pytest.approx(10.0)
    assert broker.store_ctx.get_order('c1').closed_ts_ms is None


def __test_netting_shrink_watch_reads_an_own_flip_as_no_shortfall__(tmp_path):
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _own_opposite_fill(broker, order_id=70211190, cents=2000, closed_cents=1000,
                       venue_cents=1000, venue_side=_SELL)
    broker._share_effects = []
    res = _oa.ProtoOAReconcileRes()
    res.position.append(_model.ProtoOAPosition(
        positionId=_PID, positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=1000, tradeSide=_SELL)))
    broker._reconcile_res = res

    for _ in range(3):
        _snapshot_pass(broker)
    broker._flush_external_interventions()

    assert calls == []
    assert _retired(broker) == 0.0
    assert broker._own_share(_PID) == pytest.approx(10.0)


def __test_netting_close_of_foreign_exposure_after_an_own_reduction_is_ignored__(tmp_path):
    """The bot reduced its long 30 by an opposite order of 10 (net 20) while
    someone else held 10; their close of exactly their 10 leaves the bot's 20."""
    broker = _owned_broker(tmp_path)
    broker.store_ctx.upsert_order('c1', qty=30.0, filled_qty=30.0)
    calls = _spy_quarantine(broker)
    _own_opposite_fill(broker, order_id=70211192, cents=1000, closed_cents=1000,
                       venue_cents=3000, venue_side=_model.ProtoOATradeSide.BUY,
                       pine_id='R')
    assert broker._own_share(_PID) == pytest.approx(20.0)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=1000, remaining_cents=2000))
    broker._flush_external_interventions()

    assert out is None
    assert calls == []


def __test_netting_partial_close_retires_only_the_net_side__(tmp_path):
    """A close of the bot's net long retires the long rows, never the bot's
    own opposite row: that would grow the net instead of shrinking it."""
    broker = _owned_broker(tmp_path)
    broker.store_ctx.upsert_order('c1', qty=30.0, filled_qty=30.0)
    _own_opposite_fill(broker, order_id=70211192, cents=1000, closed_cents=1000,
                       venue_cents=2000, venue_side=_model.ProtoOATradeSide.BUY,
                       pine_id='R')
    reduce_row = next(row for row in broker.store_ctx.iter_live_orders()
                      if row.pine_entry_id == 'R')

    broker._retire_partial_close_exposure(_PID, 5.0)

    assert _retired(broker) == pytest.approx(5.0)
    assert _retired(broker, reduce_row.client_order_id) == 0.0
    assert broker._own_share(_PID) == pytest.approx(15.0)


def __test_own_flip_fill_pushed_before_its_dispatch_answer_is_the_entry__(tmp_path):
    """The bot's own opposite entry closes the old side, but its fill is the
    entry the bot sent, never a close of the old entry."""
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    calls = _spy_quarantine(broker)
    order = _make_order(order_id=70211190, executed=2000, volume=2000, side=_SELL,
                        position_id=_PID,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    deal = _model.ProtoOADeal(
        dealId=70211191, filledVolume=2000, executionPrice=1.1,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=1000))
    fill = _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order, deal=deal)
    fill.position.CopyFrom(_model.ProtoOAPosition(
        positionId=_PID, positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=1000, tradeSide=_SELL)))
    seen: list = []

    async def push_overtakes_the_answer(req):
        fill.order.clientOrderId = req.clientOrderId
        seen.append(broker._translate_exec_event(fill))

    broker.on_dispatch = push_overtakes_the_answer
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=70211190, volume=2000, side=_SELL, position_id=_PID))
    asyncio.run(broker.execute_entry(_envelope(EntryIntent(
        pine_id="S", symbol="EURUSD", side="sell", qty=20.0,
        order_type=OrderType.MARKET))))
    broker._flush_external_interventions()

    assert len(seen) == 1 and seen[0] is not None
    assert seen[0].leg_type is LegType.ENTRY and seen[0].pine_id == 'S'
    assert seen[0].fill_qty == pytest.approx(20.0)
    assert broker.store_ctx.get_order('c1').closed_ts_ms is None
    assert broker._own_share(_PID) == pytest.approx(10.0)
    assert broker._share_side(_PID) == 'sell'
    assert calls == []


# === NETTING: a fill an own close took that was another order's ==========

def __test_netting_foreign_close_taken_for_an_own_close_is_not_booked_as_the_bot_s__(
        tmp_path):
    """The bot holds 20 of 40 and closes 10; meanwhile the other party closes
    exactly its own 20. The engine must end with 10 closed, the journal with 10."""
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID, volume=4000))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker.store_ctx.upsert_order('c1', qty=20.0, filled_qty=20.0)
    calls = _spy_quarantine(broker)
    seen: list = []

    async def foreign_fill_in_flight(_req):
        seen.append(broker._translate_exec_event(_closing_fill(
            order_id=70700000, closed_cents=2000, remaining_cents=2000)))

    broker.on_dispatch = foreign_fill_in_flight
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=889, position_id=_PID, closing=True))
    _close(broker, qty=10.0)
    broker.on_dispatch = None
    seen.append(broker._translate_exec_event(_closing_fill(
        order_id=889, closed_cents=1000, remaining_cents=1000, deal_id=8891)))
    broker._flush_external_interventions()

    booked = sum(event.fill_qty for event in seen if event is not None)
    assert booked == pytest.approx(10.0)
    assert broker._own_share(_PID) == pytest.approx(10.0)
    assert calls == []
    assert _store_events(broker, 'ctrader_close_claim_misbooked') == []


@pytest.mark.parametrize('sink', ['working', 'missing', 'failing'])
def __test_netting_foreign_close_taken_for_a_refused_own_close_is_rebooked__(tmp_path, sink):
    """The bot holds 20 of 40 and closes 10; meanwhile the other party closes
    exactly its own 20, and the bot's close is refused. That fill was the other
    party's own business: the engine never books it, the journal keeps the
    bot's 20, a WARNING and an audit event — no quarantine and no halt, with or
    without a working sink."""
    broker = _merged_broker(tmp_path, venue_cents=4000, share=20.0)
    calls = _spy_quarantine(broker)
    if sink == 'missing':
        broker.quarantine_sink = None
    elif sink == 'failing':
        def failing_sink(_reason, _context, *, kind):
            raise RuntimeError(f"sink down ({kind})")

        broker.quarantine_sink = failing_sink
    seen: list = []

    async def foreign_fill_in_flight(_req):
        seen.append(broker._translate_exec_event(_closing_fill(
            order_id=70700000, closed_cents=2000, remaining_cents=2000)))
        broker._raise_on_dispatch = _position_locked_reject()

    broker.on_dispatch = foreign_fill_in_flight
    broker.on_unexpected_cancel = 'ignore'
    with pytest.raises(OrderSkippedByPlugin):
        _close(broker, qty=10.0)
    seen.extend(_released(broker))
    broker._flush_external_interventions()

    assert seen == [None]
    assert calls == []
    assert broker._own_share(_PID) == pytest.approx(20.0)
    audits = _store_events(broker, 'ctrader_close_claim_rebooked')
    assert len(audits) == 1 and audits[0]['volume'] == pytest.approx(10.0)
    assert audits[0]['restored'] == pytest.approx(10.0)


def __test_netting_foreign_close_taken_for_a_cancelled_own_close_is_rebooked__(tmp_path):
    """The bot holds 20 of 40 and closes 10; meanwhile the other party closes
    exactly its own 20, the venue accepts the bot's close and later cancels
    it. Nothing of the bot's was closed: the engine never books the taken
    fill, the journal keeps the bot's 20, and nothing is reported."""
    broker = _merged_broker(tmp_path, venue_cents=4000, share=20.0)
    calls = _spy_quarantine(broker)
    seen: list = []

    async def foreign_fill_in_flight(_req):
        seen.append(broker._translate_exec_event(_closing_fill(
            order_id=70700000, closed_cents=2000, remaining_cents=2000)))

    broker.on_dispatch = foreign_fill_in_flight
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=889, position_id=_PID, closing=True))
    _close(broker, qty=10.0)
    broker.on_dispatch = None
    seen.extend(_released(broker))
    seen.append(broker._translate_exec_event(_exec_event(
        _model.ProtoOAExecutionType.ORDER_CANCELLED,
        order=_make_order(order_id=889, position_id=_PID, closing=True))))
    broker._flush_external_interventions()

    assert sum(event.fill_qty or 0.0 for event in seen if event is not None) == 0.0
    assert calls == []
    assert broker._own_share(_PID) == pytest.approx(20.0)
    audits = _store_events(broker, 'ctrader_close_claim_rebooked')
    assert len(audits) == 1 and audits[0]['volume'] == pytest.approx(10.0)


def __test_netting_refused_close_of_the_whole_share_keeps_the_position_guarded__(tmp_path):
    """The bot holds 10 of 20 under its own bracket and closes its 10; the other
    party closes its own 10 meanwhile and the bot's close is refused. The bot
    still holds its 10: the fill taken for the close must not have vacated the
    position, so its protection stays guarded and a foreign stop move is
    reported."""
    broker = _merged_broker(tmp_path, venue_cents=2000, share=10.0)
    broker._wire = object()
    asyncio.run(broker.execute_exit(_envelope(ExitIntent(
        pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
        tp_price=1.3, sl_price=1.1))))
    broker._reconcile_res = _open_eurusd_position(position_id=_PID, volume=2000)
    calls = _spy_quarantine(broker)

    async def foreign_fill_in_flight(_req):
        broker._translate_exec_event(_closing_fill(
            order_id=70700000, closed_cents=1000, remaining_cents=1000))
        broker._raise_on_dispatch = _position_locked_reject()

    broker.on_dispatch = foreign_fill_in_flight
    with pytest.raises(OrderSkippedByPlugin):
        _close(broker, qty=10.0)
    broker.on_dispatch = None
    broker._raise_on_dispatch = None
    assert _released(broker) == []
    broker._flush_external_interventions()

    assert calls == []
    assert broker._own_share(_PID) == pytest.approx(10.0)
    assert broker._protection_guard().known(f"position:{_PID}")
    broker._translate_exec_event(_replaced_push(sl=1.05, tp=1.3))
    broker._translate_exec_event(_replaced_push(sl=1.05, tp=1.3))
    assert len(calls) == 1 and 'protection_key' in calls[0][1]


def __test_released_close_fill_is_yielded_before_the_next_message_and_its_report__(tmp_path):
    """A foreign close takes 4 of the bot's 10 while the bot's close is in
    flight. Once the answer names the bot's order, the held fill is yielded
    first, ahead of the close's own fill already queued, and only then is the
    report handled."""
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker.on_unexpected_cancel = 'halt'
    broker._exec_events = asyncio.Queue()
    own_fill = _closing_fill(order_id=889, closed_cents=400, remaining_cents=200)

    async def foreign_fill_in_flight(_req):
        assert broker._translate_exec_event(_closing_fill(
            order_id=70700000, closed_cents=400, remaining_cents=600)) is None
        broker._exec_events.put_nowait(own_fill)

    broker.on_dispatch = foreign_fill_in_flight
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=889, position_id=_PID, closing=True))
    _close(broker, qty=4.0)

    async def run():
        agen = broker.watch_orders()
        try:
            first = await asyncio.wait_for(agen.__anext__(), timeout=5.0)
            with pytest.raises(UnexpectedCancelError):
                await asyncio.wait_for(agen.__anext__(), timeout=5.0)
            return first
        finally:
            await agen.aclose()

    first = asyncio.run(run())
    assert first.order.id == '70700000' and first.fill_qty == pytest.approx(4.0)


def __test_own_close_failing_on_an_unexpected_response_has_an_unknown_outcome__(tmp_path):
    """An unexpected response is no proof the close was refused: it may have
    executed, so a closing fill that follows is claimed and audited, never
    reported as a manual close."""
    broker = _owned_broker(tmp_path)
    calls = _spy_quarantine(broker)
    broker._raise_on_dispatch = ExchangeOrderRejectedError(
        "cTrader close: unexpected response ProtoOAErrorRes")
    with pytest.raises(ExchangeOrderRejectedError):
        _close(broker)
    broker._raise_on_dispatch = None

    out = broker._translate_exec_event(_closing_fill(order_id=892))
    broker._flush_external_interventions()

    assert out is not None and out.leg_type is LegType.CLOSE
    assert calls == []
    assert len(_store_events(broker, 'ctrader_close_claim_ambiguous')) == 1


@pytest.mark.parametrize('own_fill_before_answer', [False, True])
def __test_netting_own_full_close_with_a_foreign_close_taken_for_it_books_the_share__(
        tmp_path, own_fill_before_answer):
    """The bot holds 10 of a 15-unit merged position and closes its 10; while
    that close is in flight the other party closes only its own 5. The fill
    taken for the close lowered the journal share, which caps the bot's own
    fill: its credit must not come off that fill a second time."""
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID, volume=1500))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    calls = _spy_quarantine(broker)
    seen: list = []

    def own_fill():
        seen.append(broker._translate_exec_event(_closing_fill(
            order_id=888, closed_cents=1000, remaining_cents=0, deal_id=8881)))

    async def foreign_fill_in_flight(_req):
        seen.append(broker._translate_exec_event(_closing_fill(
            order_id=70700000, closed_cents=500, remaining_cents=1000)))
        if own_fill_before_answer:
            own_fill()

    broker.on_dispatch = foreign_fill_in_flight
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=888, position_id=_PID, closing=True))
    _close(broker, qty=10.0)
    broker.on_dispatch = None
    seen.extend(_released(broker))
    if not own_fill_before_answer:
        own_fill()
    broker._flush_external_interventions()

    assert sum(event.fill_qty for event in seen if event is not None) == pytest.approx(10.0)
    assert calls == []
    assert _store_events(broker, 'ctrader_close_claim_misbooked') == []
    assert broker.store_ctx.get_order('c1').closed_ts_ms is not None


def _own_close_answer(*, cents, executed_ms, remaining_cents, order_id=888):
    """The bot's close answered by its own fill (a correlated ``ORDER_FILLED``)."""
    order = _make_order(order_id=order_id, executed=cents, volume=cents, position_id=_PID,
                        closing=True, status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED)
    deal = _model.ProtoOADeal(
        dealId=order_id * 10 + 1, filledVolume=cents, executionPrice=1.1,
        executionTimestamp=executed_ms,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=cents))
    event = _exec_event(_model.ProtoOAExecutionType.ORDER_FILLED, order=order, deal=deal)
    event.position.CopyFrom(_protected_position(volume=remaining_cents))
    return event


def _close_with_a_foreign_push_first(broker, *, own_cents, own_ms, own_remaining,
                                     foreign_cents, foreign_ms, foreign_remaining) -> list:
    """The bot closes ``own_cents``; its answer is its own fill. Another party's
    close push is translated while that answer is still being processed, and
    the answer's fill is translated after it, as its re-injected copy is."""
    seen: list = []

    async def foreign_push_first(_req):
        foreign = _closing_fill(order_id=70700000, closed_cents=foreign_cents,
                                remaining_cents=foreign_remaining)
        foreign.deal.executionTimestamp = foreign_ms
        seen.append(broker._translate_exec_event(foreign))

    broker.on_dispatch = foreign_push_first
    broker._canned_event = _own_close_answer(
        cents=own_cents, executed_ms=own_ms, remaining_cents=own_remaining)
    _close(broker, qty=own_cents / 100)
    broker.on_dispatch = None
    seen.extend(_released(broker))
    seen.append(broker._translate_exec_event(broker._canned_event))
    broker._flush_external_interventions()
    return seen


def _merged_broker(tmp_path, *, venue_cents, share):
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID,
                                                           volume=venue_cents))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker.store_ctx.upsert_order('c1', qty=share, filled_qty=share)
    return broker


def __test_netting_foreign_close_taken_after_an_own_partial_close_executed_is_not_reported__(
        tmp_path):
    """Bot 20 + other party 10. The bot's close of 10 executes first
    (30 -> 20), then the other party closes its own 10 (20 -> 10); that push is
    taken for the bot's close before the answer is processed. Judged against
    the share the bot's close had already reduced, it took nothing of the
    bot's."""
    broker = _merged_broker(tmp_path, venue_cents=3000, share=20.0)
    calls = _spy_quarantine(broker)

    seen = _close_with_a_foreign_push_first(
        broker, own_cents=1000, own_ms=1_000, own_remaining=2000,
        foreign_cents=1000, foreign_ms=2_000, foreign_remaining=1000)

    assert calls == []
    assert sum(event.fill_qty for event in seen if event is not None) == pytest.approx(10.0)
    assert broker._own_share(_PID) == pytest.approx(10.0)
    assert _store_events(broker, 'ctrader_close_claim_misbooked') == []


def __test_netting_foreign_close_taken_after_an_own_full_close_executed_is_not_reported__(
        tmp_path):
    """Bot 10 + other party 5. The bot's close of its 10 executes first
    (15 -> 5), then the other party closes its own 5 (5 -> 0) and that push,
    taken for the bot's close, flattens the journal. The answer's own fill is
    still the bot's close: the engine ends with exactly the bot's 10 closed."""
    broker = _merged_broker(tmp_path, venue_cents=1500, share=10.0)
    calls = _spy_quarantine(broker)

    seen = _close_with_a_foreign_push_first(
        broker, own_cents=1000, own_ms=1_000, own_remaining=500,
        foreign_cents=500, foreign_ms=2_000, foreign_remaining=0)

    assert calls == []
    assert len(seen) == 2 and seen[1] is not None and seen[1].leg_type is LegType.CLOSE
    assert sum(event.fill_qty for event in seen if event is not None) == pytest.approx(10.0)
    assert broker.store_ctx.get_order('c1').closed_ts_ms is not None
    assert _store_events(broker, 'ctrader_close_claim_misbooked') == []


def __test_netting_venue_stop_after_an_own_full_close_executed_books_only_the_share__(
        tmp_path):
    """Bot 10 + other party 5. The bot's close of its 10 executes first
    (15 -> 5), then the position's stop closes the remaining 5 (5 -> 0) and
    its push is translated before the answer's own fill. The stop took nothing
    of the bot's: the engine ends with exactly the bot's 10 closed."""
    broker = _merged_broker(tmp_path, venue_cents=1500, share=10.0)
    calls = _spy_quarantine(broker)
    seen: list = []

    async def stop_push_first(_req):
        stop = _closing_fill(order_id=70600000, closed_cents=500, remaining_cents=0,
                             order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT)
        stop.deal.executionTimestamp = 2_000
        seen.append(broker._translate_exec_event(stop))

    broker.on_dispatch = stop_push_first
    broker._canned_event = _own_close_answer(
        cents=1000, executed_ms=1_000, remaining_cents=500)
    _close(broker, qty=10.0)
    broker.on_dispatch = None
    seen.extend(_released(broker))
    seen.append(broker._translate_exec_event(broker._canned_event))
    broker._flush_external_interventions()

    assert calls == []
    assert sum(event.fill_qty for event in seen if event is not None) == pytest.approx(10.0)
    assert _store_events(broker, 'ctrader_close_claim_misbooked') == []


def __test_netting_close_before_the_own_answer_fill_is_translated_sees_the_true_share__(
        tmp_path):
    """Bot 20 + other party 10. The bot's close of 10 executes first
    (30 -> 20), the other party closes its own 10 (20 -> 10) and that push is
    taken for the bot's close. Before the answer's own fill is translated,
    someone closes 5 more (10 -> 5): that took the bot's share, which the
    claim's credit and the pending own fill must not reduce twice."""
    broker = _merged_broker(tmp_path, venue_cents=3000, share=20.0)
    calls = _spy_quarantine(broker)
    seen: list = []

    async def foreign_push_first(_req):
        foreign = _closing_fill(order_id=70700000, closed_cents=1000, remaining_cents=1000)
        foreign.deal.executionTimestamp = 2_000
        seen.append(broker._translate_exec_event(foreign))

    broker.on_dispatch = foreign_push_first
    broker._canned_event = _own_close_answer(
        cents=1000, executed_ms=1_000, remaining_cents=2000)
    _close(broker, qty=10.0)
    broker.on_dispatch = None
    seen.extend(_released(broker))
    later = _closing_fill(order_id=70800000, closed_cents=500, remaining_cents=500)
    later.deal.executionTimestamp = 3_000
    seen.append(broker._translate_exec_event(later))
    seen.append(broker._translate_exec_event(broker._canned_event))
    broker._flush_external_interventions()

    assert len(calls) == 1 and calls[0][1]['order_id'] == 70800000
    assert calls[0][1]['booked_qty'] == pytest.approx(5.0)
    assert sum(event.fill_qty for event in seen if event is not None) == pytest.approx(15.0)
    assert broker._own_share(_PID) == pytest.approx(5.0)


def __test_netting_close_taken_before_an_own_close_executed_is_still_reported__(tmp_path):
    """The bot holds the whole 20. Someone closes 5 (20 -> 15) before the bot's
    close of 10 executes (15 -> 5): that close took the bot's share."""
    broker = _merged_broker(tmp_path, venue_cents=2000, share=20.0)
    calls = _spy_quarantine(broker)

    seen = _close_with_a_foreign_push_first(
        broker, own_cents=1000, own_ms=2_000, own_remaining=500,
        foreign_cents=500, foreign_ms=1_000, foreign_remaining=1500)

    assert len(calls) == 1
    assert calls[0][1]['order_id'] == 70700000
    assert calls[0][1]['booked_qty'] == pytest.approx(5.0)
    assert sum(event.fill_qty for event in seen if event is not None) == pytest.approx(15.0)
    assert broker._own_share(_PID) == pytest.approx(5.0)


def __test_netting_close_taken_in_the_same_ms_as_an_own_close_is_audited__(tmp_path):
    """The bot's close and another party's close executed in the same
    millisecond: whether that close took any of the bot's share depends on
    their order, which nothing tells. ERROR and an audit event, no report."""
    broker = _merged_broker(tmp_path, venue_cents=3000, share=20.0)
    calls = _spy_quarantine(broker)

    _close_with_a_foreign_push_first(
        broker, own_cents=1000, own_ms=1_000, own_remaining=2000,
        foreign_cents=1000, foreign_ms=1_000, foreign_remaining=1000)

    assert calls == []
    audits = _store_events(broker, 'ctrader_close_share_ambiguous')
    assert len(audits) == 1
    assert audits[0]['low'] == pytest.approx(0.0) and audits[0]['high'] == pytest.approx(10.0)


def _own_close_fill_lost(tmp_path, *, answer_lost):
    """Bot 20 + other party 12. The bot's close of 10 executes (32 -> 22), but
    its fill never reaches the bot: the answer is lost (and the close's claim
    grace ran out), or the close was accepted and its fill push lost."""
    broker = _merged_broker(tmp_path, venue_cents=3200, share=20.0)
    calls = _spy_quarantine(broker)
    if answer_lost:
        broker._raise_on_dispatch = OrderDispositionUnknownError(
            "close timed out", client_order_id="x")
        with pytest.raises(OrderDispositionUnknownError):
            _close(broker, qty=10.0)
        broker._raise_on_dispatch = None
        for record in broker._own_closes:
            record.expires_at = time.monotonic() - 1.0
    else:
        broker._canned_event = _exec_event(
            _model.ProtoOAExecutionType.ORDER_ACCEPTED,
            order=_make_order(order_id=888, position_id=_PID, closing=True))
        _close(broker, qty=10.0)
        broker._canned_event = None
    return broker, calls


@pytest.mark.parametrize('answer_lost', [True, False])
def __test_netting_foreign_close_after_an_own_close_s_fill_was_lost_is_audited__(
        tmp_path, answer_lost):
    """The journal still counts the bot's 20, while the bot holds only 10 if
    its close executed. The other party then closes its own 12 (22 -> 10):
    the evidence cannot tell that it took any of the bot's share. ERROR and
    an audit event, no report."""
    broker, calls = _own_close_fill_lost(tmp_path, answer_lost=answer_lost)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=1200, remaining_cents=1000))
    broker._flush_external_interventions()

    assert calls == []
    assert out is None, "booked as if the bot's close executed: none of its 10 was taken"
    audits = _store_events(broker, 'ctrader_close_share_ambiguous')
    assert len(audits) == 1
    assert audits[0]['low'] == pytest.approx(0.0) and audits[0]['high'] == pytest.approx(10.0)


@pytest.mark.parametrize('answer_lost', [True, False])
def __test_netting_foreign_close_beyond_an_own_close_s_lost_fill_is_reported__(
        tmp_path, answer_lost):
    """Same, but the other party closes 16 (22 -> 6): even if the bot's close
    executed, 4 of the bot's 10 were taken."""
    broker, calls = _own_close_fill_lost(tmp_path, answer_lost=answer_lost)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=1600, remaining_cents=600))
    broker._flush_external_interventions()

    assert len(calls) == 1 and calls[0][1]['order_id'] == 70400000
    assert out is not None and out.fill_qty == pytest.approx(4.0)


def __test_netting_own_close_fill_delivered_late_ends_its_uncertainty__(tmp_path):
    """The accepted close's fill arrives after all: the journal holds the bot's
    10, and a later foreign close of 14 (22 -> 8) took 2 of them."""
    broker, calls = _own_close_fill_lost(tmp_path, answer_lost=False)
    own = broker._translate_exec_event(_closing_fill(
        order_id=888, closed_cents=1000, remaining_cents=2200))
    assert own is not None and own.fill_qty == pytest.approx(10.0)
    assert broker._unresolved_own_close(_PID) == pytest.approx(0.0)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=1400, remaining_cents=800))
    broker._flush_external_interventions()

    assert len(calls) == 1 and calls[0][1]['booked_qty'] == pytest.approx(2.0)
    assert out is not None and out.fill_qty == pytest.approx(2.0)


@pytest.mark.parametrize('exec_type', [_model.ProtoOAExecutionType.ORDER_CANCELLED,
                                       _model.ProtoOAExecutionType.ORDER_REJECTED])
def __test_netting_own_close_the_venue_ended_unfilled_runs_unseen_no_more__(
        tmp_path, exec_type):
    broker, _calls = _own_close_fill_lost(tmp_path, answer_lost=False)
    assert broker._unresolved_own_close(_PID) == pytest.approx(10.0)

    broker._translate_exec_event(_exec_event(
        exec_type, order=_make_order(order_id=888, position_id=_PID, closing=True)))

    assert broker._unresolved_own_close(_PID) == pytest.approx(0.0)


# === Report delivery: a second report on the same position ===============

def __test_second_report_on_a_position_lost_with_its_process_is_replayed__(tmp_path):
    """A part close was reported and handled; after a restart a full close of
    the same position is queued, but the process dies before delivering it."""
    broker = _owned_broker(tmp_path)
    broker._hedging_enabled = True
    calls = _spy_quarantine(broker)
    assert broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=400, remaining_cents=600)) is not None
    broker._flush_external_interventions()
    assert len(calls) == 1
    broker.store_ctx.close()

    restarted = _FakeBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, restarted)
    restarted._hedging_enabled = True
    restarted_calls = _spy_quarantine(restarted)
    restarted._flush_external_interventions()
    assert restarted_calls == []
    assert restarted._translate_exec_event(_closing_fill(
        order_id=70400001, closed_cents=600, deal_id=70400002)) is not None
    restarted.store_ctx.close()

    third = _FakeBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, third)
    third_calls = _spy_quarantine(third)
    third._flush_external_interventions()

    assert len(third_calls) == 1
    assert third_calls[0][1]['order_id'] == 70400001


# === NETTING: a position whose bot share is not known ======================

def _unknown_share_broker(tmp_path, broker_cls=_FakeBroker, *, hedged=False):
    """The bot closes a startup-adopted 15-unit position no entry row of the
    run links: only its dispatch record attributes closing fills to it."""
    broker = broker_cls(reconcile=_open_eurusd_position(position_id=_PID, volume=1500))
    _open_store(tmp_path, broker)
    broker._hedging_enabled = hedged
    broker._close_dispatch_pine_by_position[_PID] = 'L'
    return broker


@pytest.mark.parametrize('hedged', [False, True])
def __test_close_of_a_position_whose_bot_share_is_not_known__(tmp_path, hedged):
    """NETTING: nothing proves someone's close of 5 took the bot's part, so it
    is booked as the venue shows it with an ERROR and an audit, never
    reported. A HEDGED position has one owner: reported."""
    broker = _unknown_share_broker(tmp_path, hedged=hedged)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_closing_fill(
        order_id=70400000, closed_cents=500, remaining_cents=1000))
    broker._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(5.0)
    audits = _store_events(broker, 'ctrader_close_share_unknown')
    if hedged:
        assert len(calls) == 1 and audits == []
    else:
        assert calls == []
        assert len(audits) == 1
        assert audits[0]['order_id'] == 70400000
        assert audits[0]['booked_qty'] == pytest.approx(5.0)


def __test_netting_close_taken_for_an_own_close_of_an_unknown_share_is_audited__(tmp_path):
    """A foreign close fills while the bot's close of the adopted position is
    in flight; the answer names the bot's order, so the taken fill was someone
    else's — but with the share unknown it is audited, not reported."""
    broker = _unknown_share_broker(tmp_path, _HookedBroker)
    calls = _spy_quarantine(broker)

    async def foreign_fill_in_flight(_req):
        out = broker._translate_exec_event(_closing_fill(
            order_id=70700000, closed_cents=400, remaining_cents=1100))
        assert out is None, "held until the close is answered"

    broker.on_dispatch = foreign_fill_in_flight
    broker._canned_event = _exec_event(
        _model.ProtoOAExecutionType.ORDER_ACCEPTED,
        order=_make_order(order_id=889, position_id=_PID, closing=True))
    _close(broker, qty=4.0)
    released = _released(broker)
    broker._flush_external_interventions()

    assert len(released) == 1 and released[0].leg_type is LegType.CLOSE
    assert calls == []
    audits = _store_events(broker, 'ctrader_close_share_unknown')
    assert len(audits) == 1 and audits[0]['order_id'] == 70700000


# === Fired protection: the field carrying a level is not trusted ==========

@pytest.mark.parametrize('stop, limit', [(1.3, None), (None, 1.1), (1.3, 1.1)])
def __test_own_protection_firing_is_not_reported_whichever_field_carries_it__(
        tmp_path, stop, limit):
    """The bot's own SL 1.1 / TP 1.3 fire; the fill push reports them in the
    other order field. Which field carries which level is not measured."""
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)

    out = broker._translate_exec_event(_sltp_fill(stop=stop, limit=limit))
    broker._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert calls == []


@pytest.mark.parametrize('stop, limit', [(1.3, 1.05), (1.05, 1.3)])
def __test_foreign_level_firing_beside_an_own_one_is_reported_in_either_field__(
        tmp_path, stop, limit):
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(stop=stop, limit=limit))
    broker._flush_external_interventions()

    assert len(calls) == 1


def __test_foreign_take_profit_firing_beside_an_own_trailing_stop_is_reported__(tmp_path):
    """Under the bot's own trailing stop one fired price may be where the stop
    trailed to, never two."""
    broker = _FakeBroker()
    broker._last_bid = 1.2
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker)
    asyncio.run(broker.execute_exit(_envelope(ExitIntent(
        pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
        trail_offset=0.01))))
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(stop=1.234, limit=1.35))
    broker._flush_external_interventions()

    assert len(calls) == 1


@pytest.mark.parametrize('static_confirmed, reported', [
    # The venue confirmed the static stop: no trailing stop of the bot's is
    # on the position any more, so the moved stop cannot be one that trailed.
    (True, True),
    # The static write's outcome is unknown: the trailing stop may still be
    # the position's, so one fired price may be where it trailed to.
    (False, False),
])
def __test_stop_moved_and_fired_after_the_bot_left_its_trailing_stop_is_judged_static__(
        tmp_path, static_confirmed, reported):
    """The bot trailed its stop, then switched to a static SL 1.1 / TP 1.3.
    Someone moves the SL to 1.05 and it fires before any observation."""
    broker = _FakeBroker()
    broker._last_bid = 1.2
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker)
    trailing = ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD", side="sell",
                          qty=10.0, tp_price=1.3, trail_offset=0.01)
    asyncio.run(broker.execute_exit(_envelope(trailing)))
    if not static_confirmed:
        broker._raise_on_dispatch = _unknown_amend()
    try:
        asyncio.run(broker.modify_exit(
            _envelope(trailing),
            _envelope(ExitIntent(pine_id="Exit", from_entry="L", symbol="EURUSD",
                                 side="sell", qty=10.0, tp_price=1.3, sl_price=1.1),
                      run_tag='cd34')))
    except OrderDispositionUnknownError:
        pass
    broker._raise_on_dispatch = None
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(stop=1.05, limit=1.3))
    broker._flush_external_interventions()

    assert len(calls) == (1 if reported else 0)


# === Share bounds: only entries that can add to the position ==============

def __test_netting_close_below_the_share_during_an_entry_on_another_symbol_is_reported__(
        tmp_path):
    """An own entry in flight on another symbol cannot change the share of
    this position: a close that provably took the bot's volume is reported."""
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    calls = _spy_quarantine(broker)
    seen: list = []

    async def close_in_flight(_req):
        seen.append(broker._translate_exec_event(_closing_fill(
            order_id=70400000, closed_cents=500, remaining_cents=500)))

    broker.on_dispatch = close_in_flight
    asyncio.run(broker.execute_entry(_envelope(EntryIntent(
        pine_id="G", symbol="GBPUSD", side="sell", qty=10.0,
        order_type=OrderType.MARKET))))
    broker._flush_external_interventions()

    assert seen[0] is not None and seen[0].fill_qty == pytest.approx(5.0)
    assert len(calls) == 1
    assert _store_events(broker, 'ctrader_close_share_ambiguous') == []


def __test_hedged_close_below_the_share_during_an_own_entry_is_reported__(tmp_path):
    """A HEDGED entry always opens a position of its own: it never widens the
    share of the leg someone closes meanwhile."""
    broker = _HookedBroker(reconcile=_open_eurusd_position(position_id=_PID))
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._hedging_enabled = True
    calls = _spy_quarantine(broker)
    seen: list = []

    async def close_in_flight(_req):
        seen.append(broker._translate_exec_event(_closing_fill(
            order_id=70400000, closed_cents=500, remaining_cents=500)))

    broker.on_dispatch = close_in_flight
    asyncio.run(broker.execute_entry(_envelope(EntryIntent(
        pine_id="S", symbol="EURUSD", side="sell", qty=10.0,
        order_type=OrderType.MARKET))))
    broker._flush_external_interventions()

    assert seen[0] is not None and seen[0].fill_qty == pytest.approx(5.0)
    assert len(calls) == 1


# === Bare positions: the mark survives a restart ==========================

def _bare_after_clear(tmp_path):
    """The bot attached SL 1.1 / TP 1.3, then cleared the bracket."""
    broker = _guarded_broker(tmp_path)
    _set_venue(broker, sl=1.1, tp=1.3)
    asyncio.run(broker.execute_cancel(_envelope(CancelIntent(
        pine_id='Exit', symbol='EURUSD', from_entry='L'))))
    return broker


def _restart(broker) -> _FakeBroker:
    restarted = _FakeBroker(reconcile=broker._reconcile_res)
    restarted.store_ctx = broker.store_ctx
    restarted._wire = object()
    return restarted


def _apply_resume(broker, resume_ts_ms: int) -> None:
    """Apply an operator resume to a fresh process the way the engine does:
    the persisted protection baselines are voided before the plugin first
    uses its guard."""
    rebaseline_after_resume(broker.store_ctx)
    broker.operator_resume_ts_ms = resume_ts_ms


def __test_foreign_edit_pushed_before_the_first_pass_after_a_resume_is_reported__(tmp_path):
    """After an operator resume the venue as it stands is the new baseline.
    Someone moves the stop right after the stream starts: the pushes of that
    edit, queued before any reconcile pass, are judged against the snapshot
    the stream took first, never taken as the baseline themselves."""
    broker = _guarded_broker(tmp_path, sl=1.1, tp=1.3)
    restarted = _restart(broker)
    _set_venue(restarted, sl=1.1, tp=1.3)
    _apply_resume(restarted, int(time.time() * 1000) - 1)
    calls = _spy_quarantine(restarted)

    async def start_stream():
        restarted._exec_events = asyncio.Queue()
        restarted._exec_events.put_nowait(_replaced_push(sl=1.05, tp=1.3))
        restarted._exec_events.put_nowait(_replaced_push(sl=1.05, tp=1.3))
        agen = restarted.watch_orders()
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(agen.__anext__(), timeout=0.05)
        finally:
            await agen.aclose()

    asyncio.run(start_stream())

    assert len(calls) == 1
    assert calls[0][1]['expected'][:2] == pytest.approx([1.1, 1.3])
    assert calls[0][1]['observed'][:2] == pytest.approx([1.05, 1.3])


def __test_protection_firing_on_a_bare_position_after_a_restart_is_reported__(tmp_path):
    restarted = _restart(_bare_after_clear(tmp_path))
    calls = _spy_quarantine(restarted)

    out = restarted._translate_exec_event(_sltp_fill(stop=1.0))
    restarted._flush_external_interventions()

    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert len(calls) == 1 and calls[0][1]['bare'] is True


def _nothing_to_amend() -> ExchangeOrderRejectedError:
    reject = ExchangeOrderRejectedError("cTrader rejected the order: Nothing to amend.")
    reject.__cause__ = CTraderProtocolError("INVALID_REQUEST", "Nothing to amend.")
    return reject


def __test_foreign_removal_the_bot_s_own_clear_finds_bare_is_reported__(tmp_path):
    """Someone removes the bot's bracket and the bot then clears it itself:
    the venue answers ``Nothing to amend``. No own write removed the bracket,
    so the clear does not make the bot's protection empty: the guard keeps
    the bracket and the next snapshots report the removal."""
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _set_venue(broker)
    _snapshot_pass(broker)
    assert calls == []

    broker._raise_on_dispatch = _nothing_to_amend()
    _clear_bracket(broker)
    broker._raise_on_dispatch = None
    for _ in range(3):
        _snapshot_pass(broker)

    assert len(calls) == 1
    assert calls[0][1]['expected'][:2] == pytest.approx([1.1, 1.3])
    assert calls[0][1]['observed'][:2] == [None, None]
    assert _PID not in broker._bare_positions


def __test_own_clear_repeated_on_a_position_its_first_clear_left_bare_is_no_edit__(
        tmp_path):
    """Live incident: the bot's first clear already stripped the bracket and a
    retry clears again; ``Nothing to amend`` is the clear's goal state."""
    broker = _guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _clear_bracket(broker)
    _set_venue(broker)

    broker._raise_on_dispatch = _nothing_to_amend()
    _clear_bracket(broker)
    broker._raise_on_dispatch = None
    for _ in range(3):
        _snapshot_pass(broker)

    assert calls == []
    assert _PID in broker._bare_positions
    _set_venue(broker, sl=1.0)
    for _ in range(2):
        _snapshot_pass(broker)
    assert len(calls) == 1


def __test_bare_mark_from_before_an_operator_resume_is_accepted__(tmp_path):
    """The operator resumed the run: whatever protection the bare position
    carried then is accepted, so it firing later is not reported."""
    restarted = _restart(_bare_after_clear(tmp_path))
    _apply_resume(restarted, int(time.time() * 1000) + 1)
    calls = _spy_quarantine(restarted)

    restarted._translate_exec_event(_sltp_fill(stop=1.0))
    restarted._flush_external_interventions()

    assert calls == []


def _bare_mark(broker, coid='c1') -> dict:
    return (broker.store_ctx.get_order(coid).extras or {})['protection_bare']


def _resumed(broker) -> _FakeBroker:
    """A restart that applied an operator resume right after the bare mark."""
    resumed = _restart(broker)
    _apply_resume(resumed, _bare_mark(broker)['at_ms'])
    resumed._apply_adoption_baseline(resumed._reconcile_res)
    return resumed


def __test_bare_mark_an_operator_resume_voided_stays_void_after_a_later_restart__(tmp_path):
    """Someone protected the bare position while the bot was quarantined and
    the operator resumed: the process that applied the resume drops the mark,
    so the accepted protection firing after a later plain restart is the
    venue's."""
    broker = _bare_after_clear(tmp_path)
    _set_venue(broker, sl=1.0)
    resumed = _resumed(broker)
    assert 'protection_bare' not in (resumed.store_ctx.get_order('c1').extras or {})
    later = _restart(resumed)
    calls = _spy_quarantine(later)

    out = later._translate_exec_event(_sltp_fill(stop=1.0))
    later._flush_external_interventions()

    assert out is not None and calls == []


@pytest.mark.parametrize('later_restart', [False, True])
def __test_position_still_bare_at_an_operator_resume_is_marked_again__(
        tmp_path, later_restart):
    """The venue the operator accepted shows the position without protection:
    it stays bare, so a protection someone adds later and that fires is
    reported, in the resumed process and after a later restart alike."""
    broker = _bare_after_clear(tmp_path)
    _set_venue(broker)
    process = _resumed(broker)
    if later_restart:
        process = _restart(process)
    calls = _spy_quarantine(process)

    process._translate_exec_event(_sltp_fill(stop=1.0))
    process._flush_external_interventions()

    assert len(calls) == 1 and calls[0][1]['bare'] is True


@pytest.mark.parametrize('later_restart', [False, True])
def __test_protection_added_to_a_position_still_bare_at_an_operator_resume_is_reported__(
        tmp_path, later_restart):
    """The operator accepted the position without protection: a stop someone
    adds after the resume changes the bot's protection."""
    broker = _bare_after_clear(tmp_path)
    _set_venue(broker)
    process = _resumed(broker)
    if later_restart:
        process = _restart(process)
    calls = _spy_quarantine(process)
    _snapshot_pass(process)
    assert calls == []

    _set_venue(process, sl=1.0)
    _snapshot_pass(process)
    _snapshot_pass(process)

    assert len(calls) == 1


def __test_bare_mark_of_a_working_entry_fill_survives_a_restart__(tmp_path):
    """The bot's LIMIT entry filled into a position without protection: the
    mark lands on the entry row the fill links, so a restarted process still
    reports a protection someone else adds and that fires."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker, order_id=111)
    fill = _exec_event(
        _model.ProtoOAExecutionType.ORDER_FILLED,
        order=_make_order(order_id=111, executed=1000, position_id=_PID,
                          order_type=_model.ProtoOAOrderType.LIMIT,
                          status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED),
        deal=_model.ProtoOADeal(dealId=5151, filledVolume=1000, executionPrice=1.2))
    fill.position.CopyFrom(_protected_position(volume=1000))
    assert broker._translate_exec_event(fill) is not None
    restarted = _restart(broker)
    calls = _spy_quarantine(restarted)

    restarted._translate_exec_event(_sltp_fill(limit=1.25))
    restarted._flush_external_interventions()

    assert len(calls) == 1 and calls[0][1]['bare'] is True


def __test_native_failsafe_on_a_bare_position_is_never_foreign_after_a_restart__(tmp_path):
    broker = _bare_after_clear(tmp_path)
    broker.store_ctx.upsert_order('c1', exchange_order_id=str(_PID))
    asyncio.run(broker.publish_native_failsafe_sl(NativeBracketSnapshot(
        parent_entry_dispatch_ref='c1', symbol='EURUSD', parent_side='long',
        stop_level=1.08, profit_level=None, trailing_stop=None, generation=1)))
    restarted = _restart(broker)
    calls = _spy_quarantine(restarted)

    restarted._translate_exec_event(_sltp_fill(stop=1.08))
    restarted._flush_external_interventions()

    assert calls == []


def __test_own_bracket_on_a_bare_position_ends_the_mark_across_a_restart__(tmp_path):
    broker = _bare_after_clear(tmp_path)
    asyncio.run(broker.execute_exit(_envelope(ExitIntent(
        pine_id="Exit2", from_entry="L", symbol="EURUSD", side="sell", qty=10.0,
        tp_price=1.4, sl_price=1.05))))
    restarted = _restart(broker)
    calls = _spy_quarantine(restarted)

    restarted._translate_exec_event(_sltp_fill(stop=1.05))
    restarted._flush_external_interventions()

    assert calls == []


def __test_protection_firing_on_a_position_a_recovered_fill_opened_bare_is_reported__(
        tmp_path):
    """The stream missed the bot's working entry's fill; the reconcile pass
    recovered it on a position that carries no protection."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker, order_id=70211174)
    broker._wire = object()
    res = _oa.ProtoOAReconcileRes()
    res.order.append(_make_order(order_id=70211174, volume=1000, executed=500,
                                 position_id=_PID, limit=1.2,
                                 order_type=_model.ProtoOAOrderType.LIMIT))
    res.position.append(_protected_position(volume=500))
    broker._reconcile_res = res
    assert len(_snapshot_pass(broker)) == 1
    calls = _spy_quarantine(broker)

    broker._translate_exec_event(_sltp_fill(stop=1.0))
    broker._flush_external_interventions()

    assert len(calls) == 1 and calls[0][1]['bare'] is True


# === Pending entries: overlapping amends, missed edits, other attributes ==

def _modify_long(broker, *, limit, on_dispatch=None):
    broker.on_dispatch = on_dispatch
    old = EntryIntent(pine_id='Long', symbol='EURUSD', side='buy', qty=10.0,
                      order_type=OrderType.LIMIT, limit=1.20)
    new = EntryIntent(pine_id='Long', symbol='EURUSD', side='buy', qty=10.0,
                      order_type=OrderType.LIMIT, limit=limit)
    asyncio.run(broker.modify_entry(_envelope(old), _envelope(new, run_tag='cd34')))
    broker.on_dispatch = None


def __test_foreign_amend_pushed_while_an_own_amend_is_in_flight_is_reported_after_it__(
        tmp_path):
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    calls = _spy_quarantine(broker)

    async def foreign_in_flight(_req):
        broker._translate_exec_event(_replaced_entry(limit=1.15))

    _modify_long(broker, limit=1.18, on_dispatch=foreign_in_flight)
    assert calls == []
    broker._flush_external_interventions()

    assert len(calls) == 1
    assert calls[0][1]['observed'][0] == pytest.approx(1.15)


@pytest.mark.parametrize('refused', [True, False])
def __test_terms_of_a_refused_own_amend_are_not_the_bot_s__(tmp_path, refused):
    """The venue refused the amend: the order never carried its terms by the
    bot's hand, so someone else's edit to exactly them is reported. An amend
    of unknown outcome may have applied: its terms stay the bot's."""
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    calls = _spy_quarantine(broker)
    broker._raise_on_dispatch = (_position_locked_reject() if refused else
                                 OrderDispositionUnknownError("timed out", client_order_id="x"))
    with pytest.raises((ExchangeOrderRejectedError, OrderDispositionUnknownError)):
        _modify_long(broker, limit=1.18)
    broker._raise_on_dispatch = None

    broker._translate_exec_event(_replaced_entry(limit=1.18))
    broker._flush_external_interventions()

    assert len(calls) == (1 if refused else 0)
    broker._translate_exec_event(_replaced_entry(limit=1.2))
    assert len(calls) == (1 if refused else 0)


def __test_own_amend_echo_pushed_while_it_is_in_flight_is_never_reported__(tmp_path):
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    calls = _spy_quarantine(broker)

    async def echo_in_flight(_req):
        broker._translate_exec_event(_replaced_entry(limit=1.18))

    _modify_long(broker, limit=1.18, on_dispatch=echo_in_flight)
    broker._flush_external_interventions()

    assert calls == []
    assert broker._replaced_during_amend == {}


def _amend_answer(**fields):
    """The venue's answer to the bot's amend of its LIMIT entry to 1.18."""
    answer = _make_order(order_id=111, order_type=_model.ProtoOAOrderType.LIMIT, limit=1.18)
    for name, value in fields.items():
        setattr(answer, name, value)
    return _exec_event(_model.ProtoOAExecutionType.ORDER_REPLACED, order=answer)


def __test_foreign_attribute_pushed_during_an_own_amend_is_not_taken_from_its_answer__(
        tmp_path):
    """Someone adds a stop-loss to the bot's working entry; its push arrives
    while the bot's own price amend is in flight, and the amend's answer shows
    the stop-loss the venue kept. It was there before the amend: someone
    else's, reported once the amend is answered."""
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    calls = _spy_quarantine(broker)
    broker._canned_event = _amend_answer(stopLoss=1.1)

    async def foreign_in_flight(_req):
        push = _replaced_entry(limit=1.2)
        push.order.stopLoss = 1.1
        broker._translate_exec_event(push)

    _modify_long(broker, limit=1.18, on_dispatch=foreign_in_flight)
    broker._flush_external_interventions()

    assert len(calls) == 1
    assert calls[0][1]['observed_attrs'][0] == pytest.approx(1.1)


def __test_foreign_attribute_a_snapshot_saw_before_an_own_amend_is_still_reported__(
        tmp_path):
    """The stream is down: someone adds an expiry to the bot's working entry,
    one snapshot sees it, then the bot amends the price and the answer shows
    the expiry. Later snapshots confirm someone else's edit."""
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    broker._wire = object()
    calls = _spy_quarantine(broker)
    expiry = 1_900_000_000_000
    _working_entry_snapshot(broker, expirationTimestamp=expiry)
    _snapshot_pass(broker)
    broker._canned_event = _amend_answer(expirationTimestamp=expiry)
    _modify_long(broker, limit=1.18)
    broker._own_amend_settled_at[111] -= 1.0

    _working_entry_snapshot(broker, limitPrice=1.18, expirationTimestamp=expiry)
    for _ in range(3):
        _snapshot_pass(broker)

    assert len(calls) == 1
    assert _store_events(broker, 'ctrader_order_attrs_unexplained') == []


def __test_attribute_first_shown_by_an_own_amend_s_answer_is_audited_not_reported__(
        tmp_path):
    """No view before the amend showed the expiry its answer shows: the venue
    may have set it, or someone else just before. ERROR and an audit event;
    later views carrying it are not reported."""
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    calls = _spy_quarantine(broker)
    expiry = 1_900_000_000_000
    broker._canned_event = _amend_answer(expirationTimestamp=expiry)

    _modify_long(broker, limit=1.18)
    echo = _replaced_entry(limit=1.18)
    echo.order.expirationTimestamp = expiry
    broker._translate_exec_event(echo)
    broker._flush_external_interventions()

    assert calls == []
    audits = _store_events(broker, 'ctrader_order_attrs_unexplained')
    assert len(audits) == 1 and audits[0]['order_id'] == 111


def _working_entry_snapshot(broker, **fields):
    res = _oa.ProtoOAReconcileRes()
    order = _make_order(order_id=111, volume=1000, limit=1.20,
                        order_type=_model.ProtoOAOrderType.LIMIT)
    for name, value in fields.items():
        setattr(order, name, value)
    res.order.append(order)
    broker._reconcile_res = res


def __test_foreign_amend_of_a_working_entry_missed_by_the_stream_is_reported__(tmp_path):
    """An edit made while the stream was down shows only in the snapshot:
    two consecutive passes must agree, then it is reported once."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    broker._wire = object()
    calls = _spy_quarantine(broker)
    _working_entry_snapshot(broker)
    for _ in range(2):
        _snapshot_pass(broker)
    assert calls == []

    _working_entry_snapshot(broker, limitPrice=1.15)
    _snapshot_pass(broker)
    assert calls == []
    _snapshot_pass(broker)
    assert len(calls) == 1
    _snapshot_pass(broker)
    assert len(calls) == 1


def __test_snapshots_around_an_own_amend_are_never_reported__(tmp_path):
    broker = _HookedBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    broker._wire = object()
    calls = _spy_quarantine(broker)
    stale_read_started_at = time.time() - 1.0

    async def snapshots_in_flight(_req):
        _working_entry_snapshot(broker, limitPrice=1.18)
        _ = [e async for e in broker._reconcile_snapshot()]
        _ = [e async for e in broker._reconcile_snapshot()]

    _modify_long(broker, limit=1.18, on_dispatch=snapshots_in_flight)
    stale = _make_order(order_id=111, volume=1000, limit=1.25,
                        order_type=_model.ProtoOAOrderType.LIMIT)
    for _ in range(2):
        broker._observe_working_order(stale, read_started_at=stale_read_started_at)
    for limit in (1.18, 1.20):
        _working_entry_snapshot(broker, limitPrice=limit)
        for _ in range(2):
            _snapshot_pass(broker)

    assert calls == []


def __test_partly_filled_working_entry_reporting_its_remainder_is_not_reported__(tmp_path):
    """Whether the venue reports a partly filled order's whole volume or its
    remainder is not measured: both are the bot's own volume."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    calls = _spy_quarantine(broker)

    for volume in (600, 1000):
        push = _replaced_entry(limit=1.2, volume=volume)
        push.order.executedVolume = 400
        broker._translate_exec_event(push)

    assert calls == []


def _entry_partial_fill(*, volume, executed):
    """A partial fill push of the bot's LIMIT entry 111 (10 lots at 1.2)."""
    order = _make_order(order_id=111, volume=volume, executed=executed, limit=1.2,
                        order_type=_model.ProtoOAOrderType.LIMIT, position_id=_PID,
                        status=_model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED)
    deal = _model.ProtoOADeal(dealId=91000001, filledVolume=executed, executionPrice=1.2)
    return _exec_event(_model.ProtoOAExecutionType.ORDER_PARTIAL_FILL, order=order, deal=deal)


@pytest.mark.parametrize('fill_volume, replaced_volume, reported', [
    # The venue reports the whole volume: the fill left it at 10 lots, so a
    # view of 7 lots is someone cutting the order by exactly what executed.
    (1000, 700, True),
    (1000, 1000, False),
    # The venue reports the remainder: the fill left 7 lots, so a view of
    # 10 lots is someone raising the remainder by exactly what executed.
    (700, 1000, True),
    (700, 700, False),
])
def __test_volume_edit_of_a_partly_filled_entry_by_its_executed_volume_is_reported__(
        tmp_path, fill_volume, replaced_volume, reported):
    """The bot's LIMIT of 10 lots filled 3; its fill push teaches what the
    venue's volume of a partly filled order means, and a later view is judged
    by that alone."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    calls = _spy_quarantine(broker)
    fill = broker._translate_exec_event(_entry_partial_fill(volume=fill_volume, executed=300))
    assert fill is not None and fill.fill_qty == pytest.approx(3.0)

    push = _replaced_entry(limit=1.2, volume=replaced_volume)
    push.order.executedVolume = 300
    broker._translate_exec_event(push)

    assert len(calls) == (1 if reported else 0)


def __test_partial_fill_contradicting_the_learned_volume_meaning_accepts_both_again__(
        tmp_path):
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    broker._translate_exec_event(_entry_partial_fill(volume=1000, executed=300))
    assert broker._order_volume_semantics == 'whole'
    later = _entry_partial_fill(volume=500, executed=500)
    later.deal.dealId = 91000002
    later.deal.filledVolume = 200

    broker._translate_exec_event(later)

    assert broker._order_volume_semantics is None


@pytest.mark.parametrize('replaced_volume, reported', [(1300, False), (1000, True)])
def __test_partly_filled_entry_amended_by_the_bot_is_judged_by_the_amend_s_answer__(
        tmp_path, replaced_volume, reported):
    """The venue reports whole volumes and took the bot's amend of the partly
    filled order to 10 lots as its remainder: the answer shows 13 lots in
    all. A later view of 10 lots is someone cutting it."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    broker._translate_exec_event(_entry_partial_fill(volume=1000, executed=300))
    calls = _spy_quarantine(broker)
    broker._canned_event = _amend_answer()
    broker._canned_event.order.tradeData.volume = 1300
    broker._canned_event.order.executedVolume = 300
    _modify_long(broker, limit=1.18)

    push = _replaced_entry(limit=1.18, volume=replaced_volume)
    push.order.executedVolume = 300
    broker._translate_exec_event(push)

    assert len(calls) == (1 if reported else 0)


@pytest.mark.parametrize('field, value', [
    ('stopLoss', 1.1), ('takeProfit', 1.3), ('relativeStopLoss', 500),
    ('expirationTimestamp', 1_900_000_000_000),
    ('timeInForce', _model.ProtoOATimeInForce.GOOD_TILL_DATE),
])
def __test_foreign_attribute_on_a_working_entry_is_reported__(tmp_path, field, value):
    """Price and volume are the bot's, but someone added a stop-loss, a
    take-profit or an expiry the bot never sets."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    calls = _spy_quarantine(broker)
    push = _replaced_entry(limit=1.2)
    setattr(push.order, field, value)

    broker._translate_exec_event(push)

    assert len(calls) == 1
    assert calls[0][1]['observed_attrs'] != calls[0][1]['own_attrs'][0]


def __test_attributes_the_venue_answered_for_the_bot_s_order_are_the_bot_s__(tmp_path):
    """Whatever the venue fills in on the bot's order by itself is not an edit;
    a later change of it is."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    answer = _make_order(order_id=111, order_type=_model.ProtoOAOrderType.LIMIT,
                         limit=1.2)
    answer.timeInForce = _model.ProtoOATimeInForce.GOOD_TILL_CANCEL
    answer.expirationTimestamp = 1_800_000_000_000
    broker._canned_event = _exec_event(_model.ProtoOAExecutionType.ORDER_ACCEPTED, order=answer)
    asyncio.run(broker.execute_entry(_envelope(EntryIntent(
        pine_id='Long', symbol='EURUSD', side='buy', qty=10.0,
        order_type=OrderType.LIMIT, limit=1.20))))
    calls = _spy_quarantine(broker)

    echo = _replaced_entry(limit=1.2)
    echo.order.expirationTimestamp = 1_800_000_000_000
    broker._translate_exec_event(echo)
    assert calls == []

    changed = _replaced_entry(limit=1.2)
    changed.order.expirationTimestamp = 1_700_000_000_000
    broker._translate_exec_event(changed)
    assert len(calls) == 1


# === HEDGED: the leg's protection written through amend_bracket ===========

def _hedged_guarded_broker(tmp_path, *, sl=1.1, tp=1.3, trail_offset=None):
    """A HEDGED broker whose one-way emulator replicated the exit's bracket
    onto the bot's leg ``_PID``."""
    broker = _FakeBroker()
    broker._hedging_enabled = True
    broker._last_bid = 1.2
    _open_store(tmp_path, broker)
    _seed_owned_entry(broker)
    broker._wire = object()
    _set_venue(broker)
    asyncio.run(broker.amend_bracket(
        'EURUSD', str(_PID), side='sell', tp_price=tp, sl_price=sl,
        trail_offset=trail_offset, coid='bo-attach'))
    return broker


@pytest.mark.parametrize('venue', [
    {'tp': 1.3}, {'sl': 1.1}, {'sl': 1.1, 'tp': 1.3, 'trailing': True}, {'sl': 1.05, 'tp': 1.3},
])
def __test_hedged_leg_protection_changed_by_someone_else_is_reported_once__(tmp_path, venue):
    """Stop removed, take-profit removed, trailing switched on, stop moved."""
    broker = _hedged_guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _set_venue(broker, sl=1.1, tp=1.3)
    _snapshot_pass(broker)
    assert calls == []

    _set_venue(broker, **venue)
    for _ in range(3):
        _snapshot_pass(broker)

    assert len(calls) == 1
    assert calls[0][2] is QuarantineKind.EXTERNAL_INTERVENTION


def __test_hedged_leg_trailing_switched_off_by_someone_else_is_reported__(tmp_path):
    broker = _hedged_guarded_broker(tmp_path, sl=None, tp=None, trail_offset=0.01)
    calls = _spy_quarantine(broker)

    _set_venue(broker, sl=1.195, trailing=False)
    for _ in range(3):
        _snapshot_pass(broker)

    assert len(calls) == 1


def __test_hedged_leg_own_amend_and_clear_and_their_echoes_are_never_reported__(tmp_path):
    broker = _hedged_guarded_broker(tmp_path)
    calls = _spy_quarantine(broker)
    _set_venue(broker, sl=1.1, tp=1.3)
    asyncio.run(broker.amend_bracket(
        'EURUSD', str(_PID), side='sell', tp_price=1.4, sl_price=1.15,
        trail_offset=None, coid='bo-amend'))
    for levels in ({'sl': 1.1, 'tp': 1.3}, {'sl': 1.15, 'tp': 1.4}):
        broker._translate_exec_event(_replaced_push(**levels))
    _set_venue(broker, sl=1.15, tp=1.4)
    for _ in range(3):
        _snapshot_pass(broker)

    asyncio.run(broker.amend_bracket(
        'EURUSD', str(_PID), side='sell', tp_price=None, sl_price=None,
        trail_offset=None, coid='bo-clear'))
    broker._translate_exec_event(_replaced_push(sl=1.15, tp=1.4))
    _set_venue(broker)
    for _ in range(3):
        _snapshot_pass(broker)
    broker._flush_external_interventions()

    assert calls == []


def __test_working_entry_edited_before_an_operator_resume_is_accepted__(tmp_path):
    """The operator amended the quarantined bot's pending entry, then resumed
    it: the first view is the venue the operator accepted; a later edit is
    reported."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    broker._wire = object()
    broker.operator_resume_ts_ms = int(time.time() * 1000)
    calls = _spy_quarantine(broker)
    _working_entry_snapshot(broker, limitPrice=1.15)

    for _ in range(3):
        _snapshot_pass(broker)
    assert calls == []
    assert len(_store_events(broker, 'ctrader_order_accepted_by_resume')) == 1

    _working_entry_snapshot(broker, limitPrice=1.10)
    for _ in range(2):
        _snapshot_pass(broker)
    assert len(calls) == 1


@pytest.mark.parametrize('baseline_limit', [1.15, 1.20], ids=['operator-edit', 'own-terms'])
def __test_working_entry_edited_before_a_reapplied_operator_resume_is_reported__(
        tmp_path, baseline_limit):
    """The process that applied the resume took the first view of the order as
    the baseline and stopped before completing the resume. Someone edited the
    order while no process ran: the process applying the same resume again
    judges the edit against that baseline instead of accepting it."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    _dispatch_limit_entry(broker)
    broker._wire = object()
    broker.operator_resume_ts_ms = int(time.time() * 1000)
    _working_entry_snapshot(broker, limitPrice=baseline_limit)
    _snapshot_pass(broker)
    accepted = _store_events(broker, 'ctrader_order_accepted_by_resume')

    again = _restart(broker)
    again.operator_resume_ts_ms = broker.operator_resume_ts_ms
    calls = _spy_quarantine(again)
    _working_entry_snapshot(again, limitPrice=1.10)
    for _ in range(2):
        _snapshot_pass(again)

    assert len(calls) == 1
    assert _store_events(again, 'ctrader_order_accepted_by_resume') == accepted


def __test_working_entry_placed_after_an_operator_resume_is_judged__(tmp_path):
    """An order the bot placed after the resume was never part of what the
    operator accepted: someone else's edit of it is reported, even when the
    first snapshot view already shows it."""
    broker = _FakeBroker()
    _open_store(tmp_path, broker)
    broker.operator_resume_ts_ms = int(time.time() * 1000) - 1
    _dispatch_limit_entry(broker)
    broker._wire = object()
    calls = _spy_quarantine(broker)
    _working_entry_snapshot(broker, limitPrice=1.15)

    for _ in range(2):
        _snapshot_pass(broker)

    assert len(calls) == 1
    assert _store_events(broker, 'ctrader_order_accepted_by_resume') == []
