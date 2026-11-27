"""
@pyne
"""
import asyncio
import logging
from dataclasses import replace
from time import time as epoch_time
from types import SimpleNamespace

import pytest

from pynecore import lib
from pynecore.core.broker.exceptions import BrokerManualInterventionError, UnexpectedCancelError
from pynecore.core.broker.disappearance import (
    MISSING_ACCEPTED_BY_RESUME_EXTRA,
    accept_missing_before_resume,
)
from pynecore.core.broker.models import (
    CANCEL_REASON_VENUE_REJECTED,
    FEE_EVENT_TYPE,
    VENUE_DRIVEN_CANCEL_REASONS,
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
    QuarantineKind,
)
from pynecore.core.broker.position import BrokerPosition
from pynecore.core.broker.protection_guard import ProtectionLevels
from pynecore.core.broker.sync_engine import OrderSyncEngine
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.types.strategy import ADOPTED_STARTUP_ENTRY_ID, ADOPTED_STARTUP_EXTRA_KEY

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader import events as ctrader_events
from pynecore_ctrader import reconcile as ctrader_reconcile
from pynecore_ctrader._base import _UNKNOWN_DISPATCH_TTL_S
from pynecore_ctrader.helpers import volume_to_units
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model
from pynecore_ctrader.models import _SymbolRules
from pynecore_ctrader.protection import (
    _FEE_DELIVERY_EVENT,
    _FILL_DELIVERY_EVENT,
    CloseOrigin,
    _AnswerFill,
    _ClaimedFill,
    _PositionDeal,
    protection_key,
)
from pynecore_ctrader.reconcile import _DealWalk
from pynecore_ctrader.wire import CTraderProtocolError, CTraderTimeoutError


# === Fakes ================================================================

class _FakeWire:
    """Stubbed wire serving the deal-history bridge: every request returns ``deal_res``.

    With ``honour_window`` set, a ``ProtoOADealListReq`` returns only the deals
    of ``deal_res`` executed inside its ``fromTimestamp`` .. ``toTimestamp``
    window, as the venue does, so a test can prove which window a read asks for.

    A closing order looked up by ``ProtoOAOrderDetailsReq`` is the position's
    own SL/TP order: the venue closed it.
    """

    def __init__(self, *, deal_res=None, fail=False):
        self._deal_res = deal_res
        self._fail = fail
        self.honour_window = False
        self.requests: list = []
        self.timeouts: list = []

    async def send_request(self, req, *, timeout=None, **_kwargs):
        self.requests.append(req)
        self.timeouts.append(timeout)
        if self._fail:
            raise RuntimeError("wire down")
        if isinstance(req, _oa.ProtoOAOrderDetailsReq):
            return _oa.ProtoOAOrderDetailsRes(order=_model.ProtoOAOrder(
                orderId=req.orderId,
                orderType=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT,
                orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
                closingOrder=True,
                tradeData=_model.ProtoOATradeData(symbolId=1, volume=0,
                                                  tradeSide=_model.ProtoOATradeSide.SELL)))
        if (self.honour_window and self._deal_res is not None
                and isinstance(req, _oa.ProtoOADealListReq)):
            return _deal_res(*(
                deal for deal in self._deal_res.deal
                if req.fromTimestamp <= (deal.executionTimestamp or deal.createTimestamp)
                <= req.toTimestamp), has_more=self._deal_res.hasMore)
        return self._deal_res


class _ReconcileBroker(CTrader):
    """cTrader broker with ``_reconcile`` canned and the wire stubbed."""

    def __init__(self, recon, *, deal_res=None, wire_fails=False):
        super().__init__(symbol=None, config=_make_config())
        self._live_account_id = 999
        self._symbols_by_name = {'EURUSD': 1}
        self._symbols_by_id = {1: 'EURUSD'}
        self._recon = recon
        self._wire = _FakeWire(deal_res=deal_res, fail=wire_fails)

    async def _reconcile(self, *, return_protection_orders=False):
        return self._recon


def _make_config(**overrides) -> CTraderConfig:
    defaults = dict(demo=True, client_id="cid", client_secret="sec", account_id="999")
    defaults.update(overrides)
    return CTraderConfig(**defaults)


def _open(tmp_path, broker) -> None:
    store = BrokerStore(tmp_path / "broker.sqlite", plugin_name=broker.plugin_name)
    identity = RunIdentity(
        strategy_id="recon", symbol="EURUSD", timeframe="60",
        account_id="recon-account",
    )
    broker.store_ctx = store.open_run(identity, script_source="// recon")


def _recon(*, orders=(), positions=()) -> _oa.ProtoOAReconcileRes:
    return _oa.ProtoOAReconcileRes(order=list(orders), position=list(positions))


def _order(*, order_id, volume, executed=0, position_id=0, price=0.0,
           side=_model.ProtoOATradeSide.BUY):
    return _model.ProtoOAOrder(
        orderId=order_id, orderType=_model.ProtoOAOrderType.LIMIT,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED,
        executedVolume=executed, executionPrice=price, positionId=position_id,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=volume, tradeSide=side),
    )


def _position(*, position_id, volume, price=1.1, side=_model.ProtoOATradeSide.BUY,
              stop_loss=None, take_profit=None, trailing=False):
    pos = _model.ProtoOAPosition(
        positionId=position_id,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        price=price,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=volume, tradeSide=side),
    )
    if stop_loss is not None:
        pos.stopLoss = stop_loss
    if take_profit is not None:
        pos.takeProfit = take_profit
    if trailing:
        pos.trailingStopLoss = True
    return pos


def _protection_order(*, position_id, stop_loss=None, take_profit=None,
                      trailing=False):
    # A position's STOP_LOSS_TAKE_PROFIT protection order as the live venue
    # actually shapes it (probe114, Pepperstone demo): the SL rides in
    # ``stopPrice`` and the TP in ``limitPrice``; the order's ``stopLoss`` /
    # ``takeProfit`` fields stay unset.
    order = _model.ProtoOAOrder(
        orderId=900_000 + position_id,
        orderType=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED,
        positionId=position_id,
        tradeData=_model.ProtoOATradeData(
            symbolId=1, volume=0,
            tradeSide=_model.ProtoOATradeSide.BUY),
    )
    if stop_loss is not None:
        order.stopPrice = stop_loss
    if take_profit is not None:
        order.limitPrice = take_profit
    if trailing:
        order.trailingStopLoss = True
    return order


def _deal(*, deal_id, order_id, position_id, filled_volume, price=1.1,
          status=_model.ProtoOADealStatus.FILLED):
    return _model.ProtoOADeal(
        dealId=deal_id, orderId=order_id, positionId=position_id,
        filledVolume=filled_volume, executionPrice=price, dealStatus=status,
        moneyDigits=2, commission=0, executionTimestamp=1_700_000_000_000,
    )


def _close_deal(*, deal_id, order_id, position_id, closed_volume, price=1.1,
                side=_model.ProtoOATradeSide.SELL, commission=0):
    """A FILLED closing deal carrying ``closePositionDetail.closedVolume``; by
    default the SELL that closes a long."""
    return _model.ProtoOADeal(
        dealId=deal_id, orderId=order_id, positionId=position_id,
        filledVolume=closed_volume, executionPrice=price, tradeSide=side,
        dealStatus=_model.ProtoOADealStatus.FILLED, moneyDigits=2, commission=commission,
        executionTimestamp=1_700_000_100_000,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=closed_volume),
    )


def _deal_res(*deals, has_more=False) -> _oa.ProtoOADealListRes:
    return _oa.ProtoOADealListRes(deal=list(deals), hasMore=has_more)


def _seed_working(broker, coid, *, order_id, qty, extras=None):
    base = {'order_id': str(order_id), 'position_id': None}
    base.update(extras or {})
    broker.store_ctx.upsert_order(
        coid, symbol='EURUSD', side='buy', qty=qty, filled_qty=0.0,
        state='confirmed', pine_entry_id='long', exchange_order_id=str(order_id),
        extras=base,
    )
    broker.store_ctx.add_ref(coid, 'order_id', str(order_id))


def _seed_partial_working(broker, coid, *, order_id, qty, filled, position_id):
    """A working row that already partial-filled: ``position_id`` linked, residual open."""
    broker.store_ctx.upsert_order(
        coid, symbol='EURUSD', side='buy', qty=qty, filled_qty=filled,
        state='confirmed', pine_entry_id='long', exchange_order_id=str(order_id),
        extras={'order_id': str(order_id), 'position_id': position_id},
    )
    broker.store_ctx.add_ref(coid, 'order_id', str(order_id))


def _seed_position(broker, coid, *, position_id, qty, extras=None):
    base = {'order_id': '111', 'position_id': position_id}
    base.update(extras or {})
    broker.store_ctx.upsert_order(
        coid, symbol='EURUSD', side='buy', qty=qty, filled_qty=qty,
        state='confirmed', pine_entry_id='long',
        exchange_order_id=str(position_id), extras=base,
    )


def _run(broker) -> list:
    async def collect():
        out = []
        async for e in broker._reconcile_snapshot():
            out.append(e)
        return out
    return asyncio.run(collect())


# === Working-order partial-fill progress ==================================

def __test_reconcile_partial_fill_emits_event_and_links_position__(tmp_path):
    qty = volume_to_units(2000)
    partial = volume_to_units(1000)
    broker = _ReconcileBroker(
        _recon(orders=[_order(order_id=111, volume=2000, executed=1000,
                              position_id=222, price=1.1)])
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c1', order_id=111, qty=qty)

    events = _run(broker)

    assert len(events) == 1
    ev = events[0]
    assert ev.event_type == 'partial'
    assert ev.fill_qty == partial
    assert ev.pine_id == 'long'
    row = broker.store_ctx.get_order('c1')
    assert row is not None
    assert row.filled_qty == partial
    assert (row.extras or {}).get('position_id') == 222
    # The order_id alias is preserved across the extras merge.
    assert (row.extras or {}).get('order_id') == '111'


def __test_reconcile_working_present_no_progress_is_noop__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(orders=[_order(order_id=111, volume=2000, executed=0)])
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c1', order_id=111, qty=qty)

    assert _run(broker) == []
    row = broker.store_ctx.get_order('c1')
    assert row is not None and row.filled_qty == 0.0


# === Position-row disappearance tracking ==================================

def __test_reconcile_position_gone_stamps_missing_pending__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(positions=[]))  # position 222 vanished
    _open(tmp_path, broker)
    _seed_position(broker, 'c2', position_id=222, qty=qty)

    assert _run(broker) == []  # 2.1 only stamps; the grace raise is later
    row = broker.store_ctx.get_order('c2')
    assert row is not None
    assert 'missing_pending_since' in (row.extras or {})


def __test_reconcile_position_back_clears_missing_pending__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=222, volume=2000)])
    )
    _open(tmp_path, broker)
    _seed_position(broker, 'c2', position_id=222, qty=qty,
                   extras={'missing_pending_since': 123.0})

    _run(broker)
    row = broker.store_ctx.get_order('c2')
    assert row is not None
    assert 'missing_pending_since' not in (row.extras or {})


# === Working → position promotion via the deal-history bridge =============

def __test_reconcile_vanished_working_promoted_from_deal__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=333, volume=2000)]),
        deal_res=_deal_res(_deal(deal_id=900, order_id=111, position_id=333,
                                 filled_volume=2000, price=1.1)),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c4', order_id=111, qty=qty)

    events = _run(broker)

    assert len(events) == 1
    ev = events[0]
    assert ev.event_type == 'filled'
    assert ev.fill_qty == qty
    assert ev.pine_id == 'long'
    row = broker.store_ctx.get_order('c4')
    assert row is not None
    assert row.state == 'confirmed'
    assert (row.extras or {}).get('position_id') == 333
    assert 900 in broker._seen_deal_ids


def __test_reconcile_promotion_idempotent_when_deal_already_seen__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=333, volume=2000)]),
        deal_res=_deal_res(_deal(deal_id=900, order_id=111, position_id=333,
                                 filled_volume=2000)),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c4', order_id=111, qty=qty)
    broker._seen_deal_ids.add(900)  # PUSH already applied this fill

    events = _run(broker)

    assert events == []  # no duplicate emit
    row = broker.store_ctx.get_order('c4')
    assert row is not None
    # position_id is still linked (set-once), the dedupe only suppresses the emit.
    assert (row.extras or {}).get('position_id') == 333


def __test_reconcile_vanished_working_no_fill_stamps_missing_pending__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[]),
        deal_res=_deal_res(),  # conclusive: history fully read, no FILLED deal
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c5', order_id=111, qty=qty)

    assert _run(broker) == []
    row = broker.store_ctx.get_order('c5')
    assert row is not None
    assert 'missing_pending_since' in (row.extras or {})


def __test_reconcile_vanished_working_inconclusive_history_no_stamp__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(positions=[]), wire_fails=True)
    _open(tmp_path, broker)
    _seed_working(broker, 'c6', order_id=111, qty=qty)

    assert _run(broker) == []
    row = broker.store_ctx.get_order('c6')
    assert row is not None
    # A failed history read must NOT conclude a cancel — leave it for next pass.
    assert 'missing_pending_since' not in (row.extras or {})


def __test_reconcile_partial_then_vanished_recovers_final_fill__(tmp_path):
    # A LIMIT order that earlier partial-filled (position_id linked, residual
    # open) then vanished from order[] because the rest filled while the stream
    # was down. It must be bridged through the deal history — never treated as a
    # settled position — so the final fill is emitted and persisted.
    qty = volume_to_units(2000)
    partial = volume_to_units(1000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]),
        deal_res=_deal_res(_deal(deal_id=901, order_id=111, position_id=222,
                                 filled_volume=2000, price=1.1)),
    )
    _open(tmp_path, broker)
    _seed_partial_working(broker, 'c7', order_id=111, qty=qty,
                          filled=partial, position_id=222)

    events = _run(broker)

    assert len(events) == 1
    ev = events[0]
    assert ev.event_type == 'filled'
    assert ev.order.status.name == 'FILLED'
    assert ev.fill_qty == qty - partial
    row = broker.store_ctx.get_order('c7')
    assert row is not None
    assert row.filled_qty == qty  # the residual fill is persisted, not lost
    assert 901 in broker._seen_deal_ids


def __test_reconcile_vanished_working_uses_deal_volume_not_net_position__(tmp_path):
    # NETTING / pyramiding: the open position's tradeData.volume is the net size
    # shared across entries (5000), far larger than THIS order's own fill (1000).
    # The recovered quantity must come from the order's deal filledVolume, so a
    # partial that vanished is recovered as PARTIALLY_FILLED at its own size — not
    # overstated to fully filled using unrelated position volume.
    qty = volume_to_units(2000)
    own_fill = volume_to_units(1000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=222, volume=5000)]),
        deal_res=_deal_res(_deal(deal_id=902, order_id=111, position_id=222,
                                 filled_volume=1000, price=1.1)),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c8', order_id=111, qty=qty)

    events = _run(broker)

    assert len(events) == 1
    ev = events[0]
    assert ev.event_type == 'filled'
    assert ev.order.status.name == 'PARTIALLY_FILLED'
    assert ev.fill_qty == own_fill
    row = broker.store_ctx.get_order('c8')
    assert row is not None
    assert row.filled_qty == own_fill


def __test_reconcile_vanished_working_sums_multiple_fill_deals__(tmp_path):
    # A single working order can fill across several partial deals. The bridge
    # must sum every FILLED deal of the order, not stop at the first one, so the
    # recovered quantity reflects the order's full cumulative fill.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]),
        deal_res=_deal_res(
            _deal(deal_id=903, order_id=111, position_id=222, filled_volume=1200),
            _deal(deal_id=904, order_id=111, position_id=222, filled_volume=800),
        ),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c9', order_id=111, qty=qty)

    events = _run(broker)

    # One event per deal, as the push stream delivers them: the receipts and
    # the duplicate gate key on the deal ids.
    assert [(e.event_type, e.order.status.name, e.fill_id, e.fill_qty) for e in events] == [
        ('partial', 'PARTIALLY_FILLED', '903', pytest.approx(volume_to_units(1200))),
        ('filled', 'FILLED', '904', pytest.approx(volume_to_units(800)))]
    assert sum(e.fill_qty for e in events) == pytest.approx(qty)
    assert [e.journal_fill_qty for e in events] == [e.fill_qty for e in events]
    row = broker.store_ctx.get_order('c9')
    assert row is not None
    assert row.filled_qty == qty


def __test_reconcile_partial_then_vanished_prices_only_the_new_fill__(tmp_path):
    # The cursor already booked the first half through the PUSH path (deal 910
    # at 1.0, its commission included). The recovered residual is the later
    # deal at 2.0: it is booked at 2.0 with only its own commission — never at
    # the order's 1.5 average with the booked commission charged again.
    qty = volume_to_units(2000)
    partial = volume_to_units(1000)
    first = _deal(deal_id=910, order_id=111, position_id=222,
                  filled_volume=1000, price=1.0)
    first.commission = 300
    second = _deal(deal_id=911, order_id=111, position_id=222,
                   filled_volume=1000, price=2.0)
    second.commission = 500
    second.executionTimestamp = first.executionTimestamp + 60_000
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]),
        deal_res=_deal_res(second, first),
    )
    _open(tmp_path, broker)
    _seed_partial_working(broker, 'c10', order_id=111, qty=qty,
                          filled=partial, position_id=222)
    broker._seen_deal_ids.add(910)

    events = _run(broker)

    assert len(events) == 1
    ev = events[0]
    assert ev.fill_qty == qty - partial
    assert ev.fill_price == 2.0
    assert ev.fee == 5.0


def _restarted_process(tmp_path, broker, recon, deal_res):
    """A new process of the same run: the journal survives, the memory does not."""
    broker.store_ctx.close()
    restarted = _ReconcileBroker(recon, deal_res=deal_res)
    _open(tmp_path, restarted)
    return restarted


@pytest.mark.parametrize('restart', [False, True], ids=['same-process', 'restart'])
def __test_reconcile_snapshot_booked_partial_commission_recovered_once__(
        tmp_path, monkeypatch, restart):
    # The first half was booked by the snapshot partial-fill path while the
    # deal history did not list it yet, so it carried no commission. The
    # recovered residual is still priced from its own (later) deal, and the
    # commissions of BOTH deals are booked now, since neither was booked
    # before — in the same process, and in a later one that has no memory of
    # which deals were applied.
    clock = [1_700_000_100.0]
    monkeypatch.setattr(ctrader_reconcile, 'epoch_time', lambda: clock[0])
    qty = volume_to_units(2000)
    partial = volume_to_units(1000)
    first = _deal(deal_id=920, order_id=111, position_id=222,
                  filled_volume=1000, price=1.0)
    first.commission = 300
    second = _deal(deal_id=921, order_id=111, position_id=222,
                   filled_volume=1000, price=2.0)
    second.commission = 500
    second.executionTimestamp = first.executionTimestamp + 60_000
    broker = _ReconcileBroker(
        _recon(orders=[_order(order_id=111, volume=2000, executed=1000,
                              position_id=222, price=1.0)]),
        deal_res=_deal_res(),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c11', order_id=111, qty=qty)
    (booked,) = _run(broker)
    assert booked.fill_qty == partial and booked.fee == 0.0
    clock[0] += 5.0
    recon = _recon(positions=[_position(position_id=222, volume=2000)])
    if restart:
        broker = _restarted_process(tmp_path, broker, recon, _deal_res(second, first))
    else:
        broker._recon = recon
        broker._wire._deal_res = _deal_res(second, first)

    events = _run(broker)

    # The residual's own deal is its fill, with its own commission; the first
    # deal, which the cursor counts, delivers its commission as a fee event
    # of the snapshot booking that counted it.
    assert booked.fill_id == 'c11:snapshot:1000'
    assert [(e.event_type, e.fill_id, e.fill_qty, e.fee) for e in events] == [
        (FEE_EVENT_TYPE, 'c11:snapshot:1000', None, pytest.approx(3.0)),
        ('filled', '921', pytest.approx(partial), pytest.approx(5.0))]
    assert events[1].fill_price == 2.0
    assert sum(e.fee for e in events) == pytest.approx(8.0)
    row = broker.store_ctx.get_order('c11')
    assert row is not None and not (row.extras or {}).get('fee_unbooked')


def __test_reconcile_unpriced_snapshot_progress_waits_for_a_price__(tmp_path):
    """Neither the history nor the snapshot prices the progress: nothing is
    booked and the cursor stays, so the push at the same cumulative books it."""
    broker = _ReconcileBroker(
        _recon(orders=[_order(order_id=111, volume=2000, executed=1000,
                              position_id=222, price=0.0)]),
        deal_res=_deal_res(),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c11', order_id=111, qty=volume_to_units(2000))

    assert _run(broker) == []
    row = broker.store_ctx.get_order('c11')
    assert row is not None and row.filled_qty == 0.0
    assert not (row.extras or {}).get('fee_unbooked')

    push = __test_helper_entry_push(
        deal_id=930, executed=1000, filled=1000, price=1.0, commission=300)
    event = broker._translate_exec_event(push)
    assert event is not None and event.event_type == 'partial'
    assert event.fill_qty == pytest.approx(volume_to_units(1000))
    assert event.fill_price == 1.0 and event.fill_id == '930'
    assert broker.store_ctx.get_order('c11').filled_qty == pytest.approx(10.0)


def __test_helper_entry_push(*, deal_id, executed, filled, price, commission, volume=2000):
    """A push of entry order 111 (``volume`` cents, position 222) executing ``filled`` cents."""
    order = _order(order_id=111, volume=volume, executed=executed, position_id=222, price=price)
    final = executed >= volume
    if final:
        order.orderStatus = _model.ProtoOAOrderStatus.ORDER_STATUS_FILLED
    deal = _deal(deal_id=deal_id, order_id=111, position_id=222, filled_volume=filled,
                 price=price)
    deal.commission = commission
    event = _oa.ProtoOAExecutionEvent(
        executionType=(_model.ProtoOAExecutionType.ORDER_FILLED if final
                       else _model.ProtoOAExecutionType.ORDER_PARTIAL_FILL))
    event.order.CopyFrom(order)
    event.deal.CopyFrom(deal)
    return event


#: The two deals of entry order 111: 10 units at 1.0 (commission 3.0), then
#: 10 at 2.0 (commission 5.0).
__test_helper_DEAL_A = dict(deal_id=920, executed=1000, filled=1000, price=1.0, commission=300)
__test_helper_DEAL_B = dict(deal_id=921, executed=2000, filled=1000, price=2.0, commission=500)


def __test_helper_close_long(qty: float) -> OrderEvent:
    """The fill of a close of ``qty`` units of the ``long`` entry at 2.5."""
    return OrderEvent(
        order=ExchangeOrder(
            id='close-1', symbol='EURUSD', side='sell', order_type=OrderType.MARKET, qty=qty,
            filled_qty=qty, remaining_qty=0.0, price=None, stop_price=None,
            average_fill_price=2.5, status=OrderStatus.FILLED, timestamp=1.0, fee=0.0,
            fee_currency='', reduce_only=True),
        event_type='filled', fill_price=2.5, fill_qty=qty, timestamp=1.0, from_entry='long',
        leg_type=LegType.CLOSE, fill_id='close-1', journal_fill_qty=qty)


def __test_helper_commission_run(tmp_path, *, snapshot_first: bool) -> dict:
    """Entry order 111 fills in two deals and the position is closed.

    With ``snapshot_first`` the reconcile pass books the first deal from the
    snapshot while the deal history is empty (without its commission), before
    either push arrives; otherwise both pushes book their deals with their own
    commissions. Every event is booked by a sync engine of the run, and the
    commission event of the first deal is delivered twice.
    """
    broker = _ReconcileBroker(_recon(orders=[_order(order_id=111, volume=2000)]),
                              deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_working(broker, 'c11', order_id=111, qty=volume_to_units(2000))
    engine = __test_helper_engine(broker)
    engine.reconcile()
    broker._recon = _recon(orders=[_order(order_id=111, volume=2000, executed=1000,
                                          position_id=222, price=1.0)])
    if snapshot_first:
        (booked,) = _run(broker)
        assert booked.fee == 0.0
        __test_helper_book(engine, [booked])
    broker._recon = _recon(positions=[_position(position_id=222, volume=1000)])
    first = broker._translate_exec_event(__test_helper_entry_push(**__test_helper_DEAL_A))
    assert first is not None
    __test_helper_book(engine, [first, first])
    broker._recon = _recon(positions=[_position(position_id=222, volume=2000)])
    second = broker._translate_exec_event(__test_helper_entry_push(**__test_helper_DEAL_B))
    assert second is not None
    __test_helper_book(engine, [second])
    open_commission = engine._position.open_commission
    broker._recon = _recon()
    __test_helper_book(engine, [__test_helper_close_long(20.0)])
    return {'first': first, 'open_commission': open_commission,
            'netprofit': engine._position.netprofit,
            'commissions': [t.commission for t in engine._position.closed_trades],
            'broker': broker}


def __test_commission_of_a_snapshot_booked_fill_reaches_the_books_like_the_fill_s_own__(
        tmp_path, monkeypatch):
    """The first deal of an entry was booked from the snapshot without its
    commission; its push is not booked again but delivers the deal's
    commission as a fee event, twice. The books end where they end when both
    pushes carry their commissions on their fills: the same commission and the
    same net profit. The commission is taken off the row's unbooked ranges, so
    no recovery charges it again, and its record goes once the receipt
    accounts it."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    baseline = __test_helper_commission_run(tmp_path / 'baseline', snapshot_first=False)
    run = __test_helper_commission_run(tmp_path / 'snapshot', snapshot_first=True)

    first = run['first']
    assert first.event_type == FEE_EVENT_TYPE and first.fill_id == 'c11:snapshot:1000'
    assert first.fee == pytest.approx(3.0) and first.fill_qty is None
    assert baseline['open_commission'] == pytest.approx(8.0)
    assert run['open_commission'] == pytest.approx(baseline['open_commission'])
    assert run['netprofit'] == pytest.approx(baseline['netprofit'])
    assert sum(run['commissions']) == pytest.approx(sum(baseline['commissions']))
    broker = run['broker']
    row = broker.store_ctx.get_order('c11')
    assert row is not None and not (row.extras or {}).get('fee_unbooked')
    receipt = broker.store_ctx.fill_receipt('c11:snapshot:1000')
    assert receipt is not None and receipt.fee == pytest.approx(3.0)
    assert broker._settle_deliveries() == []
    assert broker.store_ctx.keyed_events_for_run_id(_FEE_DELIVERY_EVENT) == {}


def __test_commission_of_a_snapshot_booked_fill_its_order_s_recovery_finds_is_booked_once__(
        tmp_path, monkeypatch):
    """The first deal was booked from the snapshot without its commission and
    the residual was cancelled while its push never came: the vanished order's
    recovery finds the deal, which the cursor already counts. No fill is
    delivered, its commission is, as a fee event of the deal, and only once."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    broker = _ReconcileBroker(_recon(orders=[_order(order_id=111, volume=2000)]),
                              deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_working(broker, 'c11', order_id=111, qty=volume_to_units(2000))
    engine = __test_helper_engine(broker)
    engine.reconcile()
    broker._recon = _recon(orders=[_order(order_id=111, volume=2000, executed=1000,
                                          position_id=222, price=1.0)])
    __test_helper_book(engine, _run(broker))
    assert engine._position.open_commission == 0.0
    first = _deal(deal_id=920, order_id=111, position_id=222, filled_volume=1000, price=1.0)
    first.commission = 300
    broker._recon = _recon(positions=[_position(position_id=222, volume=1000)])
    broker._wire._deal_res = _deal_res(first)

    events = _run(broker)
    __test_helper_book(engine, [*events, *events])

    assert [(e.event_type, e.fill_id, e.fee) for e in events] == [
        (FEE_EVENT_TYPE, 'c11:snapshot:1000', pytest.approx(3.0))]
    assert engine._position.size == pytest.approx(10.0)
    assert engine._position.open_commission == pytest.approx(3.0)
    row = broker.store_ctx.get_order('c11')
    assert row is not None and not (row.extras or {}).get('fee_unbooked')
    assert broker._settle_deliveries() == []
    assert broker.store_ctx.keyed_events_for_run_id(_FEE_DELIVERY_EVENT) == {}


@pytest.mark.parametrize('engine_booked', [False, True], ids=['before-engine', 'after-engine'])
def __test_commission_event_a_process_died_on_is_booked_once_after_a_restart__(
        tmp_path, monkeypatch, engine_booked):
    """The process delivered the first deal's commission as a fee event and
    died: before its engine booked it, or after (the receipt accounts it).
    The next process delivers the commission again only in the first case; the
    engine books it onto the entry's adopted trade once, however often it is
    delivered, and the record goes once the receipt accounts it."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    broker = _ReconcileBroker(_recon(orders=[_order(order_id=111, volume=2000)]),
                              deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_working(broker, 'c11', order_id=111, qty=volume_to_units(2000))
    engine = __test_helper_engine(broker)
    engine.reconcile()
    broker._recon = _recon(orders=[_order(order_id=111, volume=2000, executed=1000,
                                          position_id=222, price=1.0)])
    (booked,) = _run(broker)
    __test_helper_book(engine, [booked])
    broker._recon = _recon(positions=[_position(position_id=222, volume=1000)])
    fee = broker._translate_exec_event(__test_helper_entry_push(**__test_helper_DEAL_A))
    assert fee is not None and fee.event_type == FEE_EVENT_TYPE
    if engine_booked:
        __test_helper_book(engine, [fee])
        assert engine._position.open_commission == pytest.approx(3.0)
    # The process dies here.
    process = _next_process(broker)
    restarted = __test_helper_engine(process)
    restarted.reconcile()
    assert restarted._position.size == pytest.approx(10.0)

    redelivered = __test_helper_stream_start(process)
    __test_helper_book(restarted, [*redelivered, *redelivered])

    assert [(e.event_type, e.fill_id) for e in redelivered] == (
        [] if engine_booked else [(FEE_EVENT_TYPE, 'c11:snapshot:1000')])
    # The adopted trade carries the commission once either way: from the
    # receipt the earlier process wrote, or from the fee event delivered again.
    assert restarted._position.open_commission == pytest.approx(3.0)
    receipt = process.store_ctx.fill_receipt('c11:snapshot:1000')
    assert receipt is not None and receipt.fee == pytest.approx(3.0)
    assert process._settle_deliveries() == []
    assert process.store_ctx.keyed_events_for_run_id(_FEE_DELIVERY_EVENT) == {}
    assert __test_helper_stream_start(_next_process(process)) == []


def __test_helper_recovered_entry_run(tmp_path, *, straddle: bool):
    """A working entry of order 111 filled while the stream was down; a pass recovers it.

    Without ``straddle`` the order (20 units) filled in two deals, 920 (10 at
    1.0, commission 3.0) then 921 (10 at 2.0, commission 5.0), both unseen.
    With ``straddle`` the order (10 units) filled in one deal, 930 (commission
    4.0), whose first 5 units a snapshot-only pass booked without commission
    before the history listed the deal: the deal straddles the cursor.
    Every event before the recovery is booked by a sync engine of the run.

    :return: ``(broker, engine, the recovery pass's events)``.
    """
    volume = 1000 if straddle else 2000
    broker = _ReconcileBroker(_recon(orders=[_order(order_id=111, volume=volume)]),
                              deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_working(broker, 'c11', order_id=111, qty=volume_to_units(volume))
    engine = __test_helper_engine(broker)
    engine.reconcile()
    if straddle:
        broker._recon = _recon(orders=[_order(order_id=111, volume=volume, executed=500,
                                              position_id=222, price=1.0)])
        (booked,) = _run(broker)
        assert booked.fill_id == 'c11:snapshot:500' and booked.fee == 0.0
        __test_helper_book(engine, [booked])
        only = _deal(deal_id=930, order_id=111, position_id=222, filled_volume=1000, price=1.0)
        only.commission = 400
        deals = [only]
    else:
        first = _deal(deal_id=920, order_id=111, position_id=222, filled_volume=1000,
                      price=1.0)
        first.commission = 300
        second = _deal(deal_id=921, order_id=111, position_id=222, filled_volume=1000,
                       price=2.0)
        second.commission = 500
        second.executionTimestamp = first.executionTimestamp + 60_000
        deals = [second, first]
    broker._recon = _recon(positions=[_position(position_id=222, volume=volume)])
    broker._wire._deal_res = _deal_res(*deals)
    return broker, engine, _run(broker)


#: scenario -> (the recovery's events as ``(fill id, units, price,
#: commission)``, a fee event without units and price; the position; the
#: commission the books end with; the late push of the last deal). The
#: straddling deal's commission is split by its parts: the half the snapshot
#: booked goes to that booking's fill id, the half above the cursor rides on
#: the deal's own fill.
__test_helper_RECOVERED_ENTRIES = {
    'two-deals': ([('920', 10.0, 1.0, 3.0), ('921', 10.0, 2.0, 5.0)], 20.0, 8.0,
                  dict(deal_id=921, executed=2000, filled=1000, price=2.0, commission=500)),
    'straddling-deal': ([('c11:snapshot:500', None, None, 2.0), ('930', 5.0, 1.0, 2.0)],
                        10.0, 4.0,
                        dict(deal_id=930, executed=1000, filled=1000, price=1.0,
                             commission=400)),
}


def __test_helper_event_terms(event) -> tuple:
    """``(fill id, units, price, commission)`` of an event, approximated."""
    return (event.fill_id,
            None if event.fill_qty is None else pytest.approx(event.fill_qty),
            None if event.fill_price is None else pytest.approx(event.fill_price),
            pytest.approx(event.fee))


@pytest.mark.parametrize('scenario', list(__test_helper_RECOVERED_ENTRIES))
def __test_recovered_entry_fill_is_one_event_per_deal_booked_once__(
        tmp_path, monkeypatch, scenario):
    """The recovered entry fill is delivered as the push stream delivers it:
    one event per deal, with the deal id as the fill id, its own price and
    commission (a deal straddling the cursor books its part and the commission
    the snapshot booking left), and a delivery record per fill written with
    the cursor. Delivered twice, it is booked once, and a late push of a
    recovered deal books nothing."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    expected, size, commission, late_push = __test_helper_RECOVERED_ENTRIES[scenario]
    broker, engine, events = __test_helper_recovered_entry_run(
        tmp_path, straddle=scenario == 'straddling-deal')

    assert [__test_helper_event_terms(e) for e in events] == expected
    assert [e.journal_fill_qty for e in events] == [e.fill_qty for e in events]
    fills = [terms for terms in expected if terms[1] is not None]
    records = broker.store_ctx.keyed_events_for_run_id(_FILL_DELIVERY_EVENT)
    # Beside the record of a snapshot booking the engine took and no pass
    # settled yet.
    assert {key for key in records if ':snapshot:' not in key} == {
        f'fill:{fill_id}' for fill_id, *_rest in fills}

    __test_helper_book(engine, [*events, *events])
    late = broker._translate_exec_event(__test_helper_entry_push(**late_push))
    __test_helper_book(engine, [late] if late is not None else [])

    assert engine._position.size == pytest.approx(size)
    assert engine._position.open_commission == pytest.approx(commission)
    for fill_id, *_rest in expected:
        assert broker.store_ctx.fill_delivered(fill_id)
    assert broker._settle_deliveries() == []
    assert broker.store_ctx.keyed_events_for_run_id(_FILL_DELIVERY_EVENT) == {}


@pytest.mark.parametrize('engine_booked', [False, True], ids=['before-engine', 'after-engine'])
@pytest.mark.parametrize('scenario', list(__test_helper_RECOVERED_ENTRIES))
def __test_recovered_entry_fill_commission_survives_a_restart__(
        tmp_path, monkeypatch, scenario, engine_booked):
    """The process recovered the entry fill and died: before its engine booked
    the events, or after. The next process starts from the journal, which
    holds the fill; it delivers again only the fills without a receipt, marked
    journaled before the restart, and the adopted trade carries the recovered
    commission once however often they are delivered. A late push of a
    recovered deal books nothing."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    expected, size, commission, late_push = __test_helper_RECOVERED_ENTRIES[scenario]
    broker, engine, events = __test_helper_recovered_entry_run(
        tmp_path, straddle=scenario == 'straddling-deal')
    if engine_booked:
        __test_helper_book(engine, events)
        assert engine._position.open_commission == pytest.approx(commission)
    # The process dies here.
    process = _next_process(broker)
    restarted = __test_helper_engine(process)
    restarted.reconcile()
    assert restarted._position.size == pytest.approx(size)

    redelivered = __test_helper_stream_start(process)
    __test_helper_book(restarted, [*redelivered, *redelivered])

    # Fills first, marked as in the restart's position, then commissions.
    assert [(e.fill_id, e.journaled_before_restart) for e in redelivered] == (
        [] if engine_booked else [
            *((fill_id, True) for fill_id, qty, *_rest in expected if qty is not None),
            *((fill_id, False) for fill_id, qty, *_rest in expected if qty is None)])
    assert restarted._position.size == pytest.approx(size)
    assert restarted._position.open_commission == pytest.approx(commission)
    late = process._translate_exec_event(__test_helper_entry_push(**late_push))
    __test_helper_book(restarted, [late] if late is not None else [])
    assert restarted._position.size == pytest.approx(size)
    assert restarted._position.open_commission == pytest.approx(commission)
    assert process._settle_deliveries() == []
    assert process.store_ctx.keyed_events_for_run_id(_FILL_DELIVERY_EVENT) == {}
    assert __test_helper_stream_start(_next_process(process)) == []


def __test_recovered_straddling_deal_charges_each_trade_its_own_commission__(
        tmp_path, monkeypatch):
    """The straddling deal's recovery charges the half the snapshot booked to
    the trade that booking opened and the half above the cursor to the trade
    of the deal's own slice: a FIFO close of the older 5 units realizes 2.0 of
    entry commission, and the remaining 5 keep the other 2.0."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    broker, engine, events = __test_helper_recovered_entry_run(tmp_path, straddle=True)
    __test_helper_book(engine, events)
    assert engine._position.open_commission == pytest.approx(4.0)
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])

    __test_helper_book(engine, [__test_helper_close_long(5.0)])

    assert engine._position.size == pytest.approx(5.0)
    (closed,) = engine._position.closed_trades
    assert closed.commission == pytest.approx(2.0)
    assert engine._position.open_commission == pytest.approx(2.0)


@pytest.mark.parametrize('restart', [False, True], ids=['same-process', 'restart'])
def __test_push_of_a_deal_straddling_the_cursor_books_only_its_new_part__(
        tmp_path, monkeypatch, restart):
    """A snapshot-only pass booked the first 5 units of entry order 111 (10
    units) before the history listed its only deal, 930 (commission 4.0);
    the deal's push then arrives first. It books only the 5 units above the
    cursor with their half of the commission, and the other half goes to the
    snapshot booking's trade as a fee event the stream delivers ahead of the
    fill: the books hold the venue's 10 units and 4.0, a FIFO close of the
    older 5 realizes 2.0 of it, and a later recovery or copy of the deal books
    nothing more, in this process or after a restart (where the engine no
    longer knows which trade the snapshot booking opened)."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    broker = _ReconcileBroker(_recon(orders=[_order(order_id=111, volume=1000)]),
                              deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_working(broker, 'c11', order_id=111, qty=volume_to_units(1000))
    engine = __test_helper_engine(broker)
    engine.reconcile()
    broker._recon = _recon(orders=[_order(order_id=111, volume=1000, executed=500,
                                          position_id=222, price=1.0)])
    (booked,) = _run(broker)
    assert booked.fill_id == 'c11:snapshot:500'
    __test_helper_book(engine, [booked])
    if restart:
        broker._recon = _recon(
            orders=[_order(order_id=111, volume=1000, executed=500, position_id=222,
                           price=1.0)],
            positions=[_position(position_id=222, volume=500)])
        broker = _next_process(broker)
        engine = __test_helper_engine(broker)
        engine.reconcile()
        assert __test_helper_stream_start(broker) == []
    broker._recon = _recon(positions=[_position(position_id=222, volume=1000)])
    push = dict(deal_id=930, executed=1000, filled=1000, price=1.0, commission=400,
                volume=1000)

    delivered = __test_helper_stream_of(broker, [__test_helper_entry_push(**push)])

    assert [(e.event_type, e.fill_id, e.fill_qty, e.fee) for e in delivered] == [
        (FEE_EVENT_TYPE, 'c11:snapshot:500', None, pytest.approx(2.0)),
        ('filled', '930', pytest.approx(5.0), pytest.approx(2.0))]
    fill = delivered[1]
    assert fill.journal_fill_qty == pytest.approx(5.0)
    row = broker.store_ctx.get_order('c11')
    assert row is not None and row.filled_qty == pytest.approx(10.0)
    assert not (row.extras or {}).get('fee_unbooked')
    __test_helper_book(engine, [*delivered, *delivered])
    assert engine._position.size == pytest.approx(10.0)
    assert engine._position.open_commission == pytest.approx(4.0)

    only = _deal(deal_id=930, order_id=111, position_id=222, filled_volume=1000, price=1.0)
    only.commission = 400
    broker._wire._deal_res = _deal_res(only)
    later = [event for _ in range(2) for event in _run(broker)]
    copy = broker._translate_exec_event(__test_helper_entry_push(**push))
    __test_helper_book(engine, [*later, *([copy] if copy is not None else [])])
    assert later == [] and copy is None and not broker._released_close_events
    assert engine._position.size == pytest.approx(10.0)
    assert engine._position.open_commission == pytest.approx(4.0)

    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])
    __test_helper_book(engine, [__test_helper_close_long(5.0)])
    (closed,) = engine._position.closed_trades
    assert closed.commission == pytest.approx(2.0)
    assert engine._position.open_commission == pytest.approx(2.0)
    assert broker._settle_deliveries() == []
    assert broker.store_ctx.keyed_events_for_run_id(_FEE_DELIVERY_EVENT) == {}


def __test_commission_of_a_fill_held_for_the_venue_is_delivered_again_once_it_is_taken__(
        tmp_path, monkeypatch):
    """A fee event of a fill the engine holds for the venue's verdict is not
    booked. Once the engine took the fill, the commission is delivered again,
    once, and the record goes when the receipt accounts it."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    _open(tmp_path, broker)
    engine = __test_helper_engine(broker)
    monkeypatch.setattr(engine, '_fill_report_grace_s', lambda: 0.0)
    engine.reconcile()
    entry = replace(__test_helper_close_long(1.0), order=replace(
        __test_helper_close_long(1.0).order, id='entry-1', side='buy', reduce_only=False),
        fill_id='entry-1', pine_id='long', from_entry=None, leg_type=LegType.ENTRY)
    broker._recon = _recon(positions=[_position(position_id=222, volume=100)])
    __test_helper_book(engine, [entry])
    broker._recon = _recon()
    __test_helper_book(engine, [__test_helper_close_long(1.0)])
    assert engine._position.size == 0.0 and len(engine._position.closed_trades) == 1
    netprofit = engine._position.netprofit
    held = replace(__test_helper_close_long(1.0), fill_id='late-1',
                   order=replace(__test_helper_close_long(1.0).order, id='late-1'))
    fee = replace(held, event_type=FEE_EVENT_TYPE, fill_qty=None, fill_price=None,
                  journal_fill_qty=None, fee=0.7)
    broker._journal_fee_delivery(fee)
    __test_helper_book(engine, [held, fee])
    assert not broker.store_ctx.fill_delivered('late-1')
    assert broker._settle_deliveries() == []
    for _ in range(2):
        engine.reconcile()
    assert broker.store_ctx.fill_delivered('late-1')

    again = broker._settle_deliveries()
    assert [(e.event_type, e.fill_id, e.fee) for e in again] == [(FEE_EVENT_TYPE, 'late-1', 0.7)]
    assert broker._settle_deliveries() == []
    __test_helper_book(engine, again)

    assert engine._position.netprofit == pytest.approx(netprofit - 0.7)
    assert broker._settle_deliveries() == []
    assert broker.store_ctx.keyed_events_for_run_id(_FEE_DELIVERY_EVENT) == {}


def __test_recovered_residual_after_a_restart_charges_only_its_own_commission__(tmp_path):
    # The push stream booked the first deal, with its commission, in an
    # earlier process of the run; the residual filled while no process ran.
    # The restarted process has no memory of the applied deal, yet the
    # journalled cursor counts it: only the residual's commission is booked.
    qty = volume_to_units(2000)
    partial = volume_to_units(1000)
    first = _deal(deal_id=930, order_id=111, position_id=222,
                  filled_volume=1000, price=1.0)
    first.commission = 300
    second = _deal(deal_id=931, order_id=111, position_id=222,
                   filled_volume=1000, price=2.0)
    second.commission = 500
    second.executionTimestamp = first.executionTimestamp + 60_000
    recon = _recon(positions=[_position(position_id=222, volume=2000)])
    earlier = _ReconcileBroker(recon, deal_res=_deal_res())
    _open(tmp_path, earlier)
    _seed_partial_working(earlier, 'c12', order_id=111, qty=qty,
                          filled=partial, position_id=222)
    broker = _restarted_process(tmp_path, earlier, recon, _deal_res(second, first))

    events = _run(broker)

    assert len(events) == 1
    ev = events[0]
    assert ev.fill_qty == qty - partial
    assert ev.fill_price == 2.0
    assert ev.fee == 5.0


def __test_partial_progress_after_a_restart_charges_only_its_own_commission__(tmp_path):
    # The working order's first deal was booked by the push stream in an
    # earlier process; a second partial deal executed while no process ran.
    # The snapshot partial-fill path charges only the new deal's commission.
    qty = volume_to_units(2000)
    first = _deal(deal_id=940, order_id=111, position_id=222,
                  filled_volume=1000, price=1.0)
    first.commission = 300
    second = _deal(deal_id=941, order_id=111, position_id=222,
                   filled_volume=500, price=2.0)
    second.commission = 200
    second.executionTimestamp = first.executionTimestamp + 60_000
    recon = _recon(orders=[_order(order_id=111, volume=2000, executed=1500,
                                  position_id=222, price=1.33)])
    earlier = _ReconcileBroker(recon, deal_res=_deal_res())
    _open(tmp_path, earlier)
    _seed_partial_working(earlier, 'c13', order_id=111, qty=qty,
                          filled=volume_to_units(1000), position_id=222)
    broker = _restarted_process(tmp_path, earlier, recon, _deal_res(second, first))

    events = _run(broker)

    assert len(events) == 1
    ev = events[0]
    assert ev.fill_qty == volume_to_units(500)
    assert ev.fill_price == 2.0
    assert ev.fee == pytest.approx(2.0)


def __test_filled_then_closed_after_a_restart_charges_only_the_entry_residual__(tmp_path):
    # The entry's first deal was booked by the push stream in an earlier
    # process; the residual filled and the SL closed the position while no
    # process ran. The recovered entry fill carries only the residual's
    # commission.
    qty = volume_to_units(2000)
    first = _deal(deal_id=950, order_id=111, position_id=222,
                  filled_volume=1000, price=1.0)
    first.commission = 300
    second = _deal(deal_id=951, order_id=111, position_id=222,
                   filled_volume=1000, price=2.0)
    second.commission = 500
    second.executionTimestamp = first.executionTimestamp + 60_000
    close = _close_deal(deal_id=952, order_id=999, position_id=222, closed_volume=2000,
                        price=1.5, commission=0)
    close.executionTimestamp = first.executionTimestamp + 120_000
    recon = _recon(positions=[])
    earlier = _ReconcileBroker(recon, deal_res=_deal_res())
    _open(tmp_path, earlier)
    _seed_partial_working(earlier, 'c15', order_id=111, qty=qty,
                          filled=volume_to_units(1000), position_id=222)
    broker = _restarted_process(tmp_path, earlier, recon, _deal_res(close, second, first))

    events = _run(broker)

    entries = [event for event in events if event.leg_type is LegType.ENTRY]
    assert len(entries) == 1
    assert entries[0].fill_qty == volume_to_units(1000)
    assert entries[0].fill_price == 2.0
    assert entries[0].fee == 5.0


# === Reconcile-pass I/O budget ============================================

def __test_deal_bridge_defers_read_once_pass_budget_spent__(tmp_path):
    # watch_orders delivers no PUSH message while the reconcile pass runs, so a
    # deal-history read past the pass deadline is deferred as inconclusive
    # without a request, and a read inside it waits at most what is left.
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    _open(tmp_path, broker)

    async def probe():
        loop = asyncio.get_running_loop()
        spent = await broker._find_fill_deal(111, 0, deadline=loop.time() - 1.0)
        live = await broker._find_fill_deal(111, 0, deadline=loop.time() + 5.0)
        return spent, live

    spent, live = asyncio.run(probe())

    assert not spent.conclusive
    assert live.conclusive
    assert len(broker._wire.requests) == 1
    assert 0.0 < broker._wire.timeouts[0] <= 5.0


def __test_reconcile_pass_bridge_reads_share_the_pass_budget__(tmp_path, monkeypatch):
    # With the pass budget already spent the vanished row's bridge read is
    # skipped (the next pass retries it): no deal request, no promotion, no
    # stamp — and the deadline is cleared once the pass ends.
    monkeypatch.setattr(ctrader_events, '_RECONCILE_PASS_BUDGET_S', 0.0)
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=333, volume=2000)]),
        deal_res=_deal_res(_deal(deal_id=930, order_id=111, position_id=333,
                                 filled_volume=2000)),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c12', order_id=111, qty=qty)

    async def collect():
        return [e async for e in broker._run_reconcile_pass()]

    events = asyncio.run(collect())

    assert events == []
    assert broker._wire.requests == []
    assert broker._reconcile_pass_deadline is None
    row = broker.store_ctx.get_order('c12')
    assert row is not None
    assert row.filled_qty == 0.0
    assert 'missing_pending_since' not in (row.extras or {})


def __test_reconcile_pass_bounds_a_stalled_snapshot_read__(tmp_path, monkeypatch):
    # A snapshot read stuck in a rate-limit backoff or an account re-auth spends
    # the same pass budget: the pass gives up as a transient failure instead of
    # holding the PUSH stream until the read returns.
    monkeypatch.setattr(ctrader_events, '_RECONCILE_PASS_BUDGET_S', 0.01)

    class _StalledBroker(_ReconcileBroker):
        async def _reconcile(self, *, return_protection_orders=False):
            await asyncio.Event().wait()

    broker = _StalledBroker(_recon())
    _open(tmp_path, broker)

    async def collect():
        return [e async for e in broker._run_reconcile_pass()]

    events = asyncio.run(collect())

    assert events == []
    assert broker._reconcile_fail_streak == 1
    assert broker._reconcile_pass_deadline is None


class _ScriptedWire(_FakeWire):
    """Wire answering each request with the next scripted response or error."""

    def __init__(self, *responses):
        super().__init__()
        self._responses = list(responses)

    async def send_request(self, req, *, timeout=None):
        self.requests.append(req)
        self.timeouts.append(timeout)
        resp = self._responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp


def __test_deal_bridge_resumes_a_cut_walk_then_reads_only_new_deals__(tmp_path, monkeypatch):
    # A walk cut after its first page keeps that page: the next read resumes
    # below it (plus the deals since), and a finished walk later reads only the
    # window since it ended — never the whole order history again.
    clock = [1_700_000_100.0]
    monkeypatch.setattr(ctrader_reconcile, 'epoch_time', lambda: clock[0])
    newer = _deal(deal_id=940, order_id=111, position_id=222,
                  filled_volume=1000, price=2.0)
    newer.executionTimestamp = 1_700_000_060_000
    older = _deal(deal_id=941, order_id=111, position_id=222,
                  filled_volume=1000, price=1.0)
    broker = _ReconcileBroker(_recon())
    wire = _ScriptedWire(_deal_res(newer, has_more=True), RuntimeError("cut"),
                         _deal_res(older), _deal_res(), _deal_res())
    broker._wire = wire
    _open(tmp_path, broker)
    from_ms = 1_699_999_000_000

    async def probe():
        first = await broker._find_fill_deal(111, from_ms)
        clock[0] = 1_700_000_150.0
        second = await broker._find_fill_deal(111, from_ms)
        clock[0] = 1_700_000_200.0
        third = await broker._find_fill_deal(111, from_ms)
        return first, second, third

    first, second, third = asyncio.run(probe())

    assert not first.conclusive
    assert second.conclusive
    assert second.filled_cents == 2000
    assert second.avg_price == 1.5
    assert (wire.requests[2].fromTimestamp, wire.requests[2].toTimestamp) == (
        from_ms, newer.executionTimestamp)
    assert (wire.requests[3].fromTimestamp, wire.requests[3].toTimestamp) == (
        1_700_000_040_000, 1_700_000_150_000)
    assert third.conclusive
    assert third.filled_cents == 2000
    assert len(wire.requests) == 5
    assert (wire.requests[4].fromTimestamp, wire.requests[4].toTimestamp) == (
        1_700_000_090_000, 1_700_000_200_000)


def __test_deal_bridge_widens_an_unread_tail_instead_of_chasing_the_present__(
        tmp_path, monkeypatch):
    # Each call fits only one page before its budget runs out. A resumed walk
    # first finishes its older sub-window and is cut on the tail it appended;
    # the next call widens that never-answered tail up to now instead of
    # queueing yet another one, so a single page completes the walk.
    clock = [1_700_000_100.0]
    monkeypatch.setattr(ctrader_reconcile, 'epoch_time', lambda: clock[0])
    newer = _deal(deal_id=960, order_id=111, position_id=222,
                  filled_volume=1000, price=2.0)
    newer.executionTimestamp = 1_700_000_060_000
    older = _deal(deal_id=961, order_id=111, position_id=222,
                  filled_volume=1000, price=1.0)
    broker = _ReconcileBroker(_recon())
    wire = _ScriptedWire(_deal_res(newer, has_more=True), RuntimeError("cut"),
                         _deal_res(older), RuntimeError("cut"), _deal_res())
    broker._wire = wire
    _open(tmp_path, broker)
    from_ms = 1_699_999_000_000

    async def probe():
        first = await broker._find_fill_deal(111, from_ms)
        clock[0] = 1_700_000_150.0
        second = await broker._find_fill_deal(111, from_ms)
        clock[0] = 1_700_000_200.0
        third = await broker._find_fill_deal(111, from_ms)
        return first, second, third

    first, second, third = asyncio.run(probe())

    assert not first.conclusive
    assert not second.conclusive
    assert third.conclusive
    assert third.filled_cents == 2000
    assert len(wire.requests) == 5
    assert (wire.requests[4].fromTimestamp, wire.requests[4].toTimestamp) == (
        1_700_000_040_000, 1_700_000_200_000)


def __test_deal_bridge_reads_the_deals_a_page_cut_at_its_oldest_timestamp__(
        tmp_path, monkeypatch):
    # Two fills of the order share one timestamp and the page cut between them:
    # the next page keeps that timestamp in its window, so the cut-off fill is
    # read, and the fill both pages carry is counted once.
    monkeypatch.setattr(ctrader_reconcile, 'epoch_time', lambda: 1_700_000_100.0)
    first = _deal(deal_id=970, order_id=111, position_id=222, filled_volume=1000)
    second = _deal(deal_id=971, order_id=111, position_id=222, filled_volume=1000)
    broker = _ReconcileBroker(_recon())
    wire = _ScriptedWire(_deal_res(first, has_more=True), _deal_res(first, second))
    broker._wire = wire
    _open(tmp_path, broker)

    bridge = asyncio.run(broker._find_fill_deal(111, 1_699_999_000_000))

    assert bridge.conclusive
    assert bridge.filled_cents == 2000
    assert wire.requests[1].toTimestamp == first.executionTimestamp


def __test_deal_bridge_page_that_cannot_narrow_is_inconclusive__(tmp_path, monkeypatch):
    # A page announcing more deals that is empty, or whose deals all sit at the
    # window's lower bound, leaves the rest of the window unread: no evidence of
    # a complete read, and the window is read again by the next call.
    monkeypatch.setattr(ctrader_reconcile, 'epoch_time', lambda: 1_700_000_100.0)
    at_lower = _deal(deal_id=980, order_id=111, position_id=222, filled_volume=1000)
    broker = _ReconcileBroker(_recon())
    wire = _ScriptedWire(_deal_res(has_more=True),
                         _deal_res(at_lower, has_more=True),
                         _deal_res(at_lower))
    broker._wire = wire
    _open(tmp_path, broker)
    from_ms = at_lower.executionTimestamp

    async def probe():
        return (await broker._find_fill_deal(111, from_ms),
                await broker._find_fill_deal(111, from_ms),
                await broker._find_fill_deal(111, from_ms))

    empty, at_bound, complete = asyncio.run(probe())

    assert not empty.conclusive
    assert not at_bound.conclusive
    assert complete.conclusive
    assert complete.filled_cents == 1000
    assert [r.fromTimestamp for r in wire.requests] == [from_ms] * 3


def __test_reconcile_reads_an_unfinished_walk_first_and_prunes_dead_ones__(tmp_path):
    # A row whose walk an earlier pass left unfinished is read before the rows
    # that got their turn, so one persistently expensive lookup cannot starve
    # it; the walk of an order with no live row is dropped.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_working(broker, 'c30', order_id=111, qty=qty,
                  extras={'submitted_at_ms': 1_700_000_000_000})
    _seed_working(broker, 'c31', order_id=112, qty=qty,
                  extras={'submitted_at_ms': 1_700_000_500_000})
    broker._deal_walks[112] = _DealWalk(
        from_ms=1_700_000_440_000, high_ms=1_700_000_600_000,
        segments=[[1_700_000_440_000, 1_700_000_550_000]],
    )
    broker._deal_walks[999] = _DealWalk(from_ms=0, high_ms=0)

    _run(broker)

    first = broker._wire.requests[0]
    assert (first.fromTimestamp, first.toTimestamp) == (
        1_700_000_440_000, 1_700_000_550_000)
    assert 999 not in broker._deal_walks


class _StallingWire(_FakeWire):
    """Fake wire whose deal-history reads from one window never answer.

    A request whose ``fromTimestamp`` is in ``stalled_from`` waits out its
    timeout, like a venue that stopped answering that lookup.
    """

    def __init__(self, *, stalled_from):
        super().__init__(deal_res=_deal_res())
        self.stalled_from = set(stalled_from)

    async def send_request(self, req, *, timeout=None):
        if (isinstance(req, _oa.ProtoOADealListReq)
                and req.fromTimestamp in self.stalled_from):
            self.requests.append(req)
            self.timeouts.append(timeout)
            await asyncio.wait_for(asyncio.Event().wait(), timeout)
        return await super().send_request(req, timeout=timeout)


def __test_reconcile_a_stalled_walk_cannot_starve_another_unfinished_one__(
        tmp_path, monkeypatch):
    # The first row's lookup stalls past the pass budget every time, so the
    # second row's walk is created but left unread. Unfinished walks take turns
    # by their last request: the next pass reads the second walk first, and
    # its no-fill evidence lands even though the first lookup stalls again.
    monkeypatch.setattr(ctrader_events, '_RECONCILE_PASS_BUDGET_S', 0.05)
    qty = volume_to_units(2000)
    anchor_a, anchor_b = 1_700_000_000_000, 1_700_000_500_000
    skew = ctrader_reconcile._SINCE_ANCHOR_SKEW_MS
    broker = _ReconcileBroker(_recon())
    broker._wire = _StallingWire(stalled_from={anchor_a - skew})
    _open(tmp_path, broker)
    _seed_working(broker, 'c32', order_id=111, qty=qty,
                  extras={'submitted_at_ms': anchor_a})
    _seed_working(broker, 'c33', order_id=112, qty=qty,
                  extras={'submitted_at_ms': anchor_b})

    async def one_pass():
        return [e async for e in broker._run_reconcile_pass()]

    asyncio.run(one_pass())
    assert [r.fromTimestamp for r in broker._wire.requests] == [anchor_a - skew]
    assert broker._deal_walks[112].segments
    assert 'missing_pending_since' not in (broker.store_ctx.get_order('c33').extras or {})

    asyncio.run(one_pass())
    assert [r.fromTimestamp for r in broker._wire.requests][1] == anchor_b - skew
    assert not broker._deal_walks[112].segments
    assert 'missing_pending_since' in (broker.store_ctx.get_order('c33').extras or {})
    assert broker._deal_walks[111].segments


def __test_reconcile_snapshot_partial_takes_terms_and_commission_from_deals__(tmp_path):
    # The snapshot's executionPrice is the order's running average and carries
    # no commission: the partial is booked at its own deal's price with that
    # deal's commission, and the deal is marked seen — so when the residual is
    # later cancelled, the recovery neither re-books nor loses the commission.
    qty = volume_to_units(2000)
    partial = volume_to_units(1000)
    deal = _deal(deal_id=950, order_id=111, position_id=222,
                 filled_volume=1000, price=1.2)
    deal.commission = 300
    broker = _ReconcileBroker(
        _recon(orders=[_order(order_id=111, volume=2000, executed=1000,
                              position_id=222, price=1.5)]),
        deal_res=_deal_res(deal),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c40', order_id=111, qty=qty)

    events = _run(broker)

    assert len(events) == 1
    ev = events[0]
    assert ev.fill_qty == partial
    assert ev.fill_price == 1.2
    assert ev.fee == 3.0
    assert 950 in broker._seen_deal_ids

    broker._recon = _recon(positions=[_position(position_id=222, volume=1000)])
    assert _run(broker) == []


# === M3 2.1.5 — cumulative-fill accounting (crash boundaries) =============

def __test_push_advances_cursor_so_same_cycle_reconcile_no_double_count__(tmp_path):
    # Surface (a): a PUSH partial advances the durable cursor BEFORE its
    # OrderEvent is enqueued, so a reconcile pass running in the same
    # watch_orders cycle (the engine has not drained the deferred record_fill
    # yet) sees ``executedVolume == filled_qty`` and emits nothing — no
    # double-count of the same slice.
    qty = volume_to_units(2000)
    partial = volume_to_units(1000)
    broker = _ReconcileBroker(
        _recon(orders=[_order(order_id=111, volume=2000, executed=1000,
                              position_id=222, price=1.1)])
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c10', order_id=111, qty=qty)

    # The PUSH path advances the cursor on the live partial fill.
    broker._advance_fill_cursor(_order(order_id=111, volume=2000, executed=1000))
    assert broker.store_ctx.get_order('c10').filled_qty == partial

    # The same-cycle reconcile pass now finds no progress to re-emit.
    assert _run(broker) == []
    assert broker.store_ctx.get_order('c10').filled_qty == partial


def __test_adoption_baseline_silently_advances_working_cursor__(tmp_path):
    # Restart: a fill landed while the stream was DOWN (PUSH never saw it), so
    # the durable cursor is stale (0) but the broker order already shows the
    # executed volume the engine's net-position adoption folded in. The first
    # get_position-driven baseline silently raises the cursor — no emit — so the
    # first reconcile pass does not re-apply the adopted slice.
    qty = volume_to_units(2000)
    executed = volume_to_units(1000)
    recon = _recon(orders=[_order(order_id=111, volume=2000, executed=1000)])
    broker = _ReconcileBroker(recon)
    _open(tmp_path, broker)
    _seed_working(broker, 'c11', order_id=111, qty=qty)

    broker._apply_adoption_baseline(recon)

    assert broker._adoption_baselined is True
    assert broker.store_ctx.get_order('c11').filled_qty == executed
    # Same snapshot through the reconcile pass: no double-emit past the barrier.
    assert _run(broker) == []


def __test_adoption_baseline_vanished_order_open_position_marks_full__(tmp_path):
    # Restart: a MARKET entry filled before the crash, so its order has already
    # left order[] and only the open position remains (positions carry no
    # order/COID link). The adoption counted it into the net, so the baseline
    # marks the row fully filled — conservatively at row.qty — and emits nothing.
    qty = volume_to_units(2000)
    recon = _recon(positions=[_position(position_id=222, volume=2000)])
    broker = _ReconcileBroker(recon)
    _open(tmp_path, broker)
    _seed_partial_working(broker, 'c12', order_id=111, qty=qty,
                          filled=0.0, position_id=222)

    broker._apply_adoption_baseline(recon)

    assert broker.store_ctx.get_order('c12').filled_qty == qty


def __test_adoption_baseline_retires_a_shrunk_position_exposure__(tmp_path):
    # Restart over a position a partial close shrank while the process was
    # down: the entry row's cumulative filled_qty (2000 centi = full entry) is
    # still the truth about EXECUTION, but the venue position now holds only
    # 1200. The baseline must not lower the monotone cursor; it books the
    # difference into the journal_exposure_retired extras counter so ownership
    # reconstruction sees the venue-remaining 12.0, not 20.0.
    qty = volume_to_units(2000)
    recon = _recon(positions=[_position(position_id=222, volume=1200)])
    broker = _ReconcileBroker(recon)
    _open(tmp_path, broker)
    _seed_position(broker, 'c30', position_id=222, qty=qty)

    broker._apply_adoption_baseline(recon)

    row = broker.store_ctx.get_order('c30')
    assert row.filled_qty == qty, "the execution watermark must not move"
    assert abs((row.extras or {})['journal_exposure_retired']
               - volume_to_units(800)) < 1e-9

    # Idempotent against a repeat over the same store state: the counter is
    # baselined TO the venue difference, not blindly incremented.
    broker._adoption_baselined = False
    broker._apply_adoption_baseline(recon)
    row = broker.store_ctx.get_order('c30')
    assert abs((row.extras or {})['journal_exposure_retired']
               - volume_to_units(800)) < 1e-9


def __test_adoption_baseline_leaves_shared_pyramid_positions_alone__(tmp_path):
    # Two live rows share one netted positionId: the venue snapshot cannot
    # attribute the shrink to either row, so the conservative baseline must
    # not guess — neither row gains a retired counter.
    recon = _recon(positions=[_position(position_id=222, volume=1000)])
    broker = _ReconcileBroker(recon)
    _open(tmp_path, broker)
    _seed_position(broker, 'c31', position_id=222, qty=volume_to_units(2000))
    _seed_position(broker, 'c32', position_id=222, qty=volume_to_units(1000))

    broker._apply_adoption_baseline(recon)

    for coid in ('c31', 'c32'):
        row = broker.store_ctx.get_order(coid)
        assert 'journal_exposure_retired' not in (row.extras or {})


def __test_adoption_baseline_is_one_shot__(tmp_path):
    # The baseline must run exactly once (the startup adoption call). A later
    # call with a higher executedVolume must NOT silently absorb a post-adoption
    # fill — that would lose a fill the engine never adopted.
    qty = volume_to_units(2000)
    first = volume_to_units(500)
    broker = _ReconcileBroker(_recon())
    _open(tmp_path, broker)
    _seed_working(broker, 'c13', order_id=111, qty=qty)

    broker._apply_adoption_baseline(
        _recon(orders=[_order(order_id=111, volume=2000, executed=500)]))
    assert broker.store_ctx.get_order('c13').filled_qty == first

    # A second snapshot at a higher executed volume is ignored (one-shot guard).
    broker._apply_adoption_baseline(
        _recon(orders=[_order(order_id=111, volume=2000, executed=1500)]))
    assert broker.store_ctx.get_order('c13').filled_qty == first


def __test_reconcile_filled_then_closed_retires_instead_of_phantom__(tmp_path):
    # Surface (d): the entry filled then the position closed while the stream was
    # down. The OPEN-filtered snapshot no longer carries the position, but the
    # deal history does — both the opening deal (orderId match) and a closing
    # deal carrying closePositionDetail.closedVolume on the same position. The
    # row is retired through the terminal-close path, NOT promoted to an open
    # position; the whole trade is delivered: the entry fill, then its close.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[]),  # position 222 fully closed -> gone
        deal_res=_deal_res(
            _deal(deal_id=905, order_id=111, position_id=222, filled_volume=2000),
            _close_deal(deal_id=906, order_id=999, position_id=222,
                        closed_volume=2000),
        ),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c14', order_id=111, qty=qty)

    events = _run(broker)

    assert [(e.leg_type, e.event_type, e.fill_qty, e.fill_price) for e in events] == [
        (LegType.ENTRY, 'filled', pytest.approx(qty), pytest.approx(1.1)),
        (LegType.CLOSE, 'filled', pytest.approx(qty), pytest.approx(1.1)),
    ]
    assert events[0].pine_id == 'long' and events[0].order.side == 'buy'
    assert events[1].from_entry == 'long' and events[1].order.side == 'sell'
    assert events[1].fill_id == '906'
    row = broker.store_ctx.get_order('c14')
    assert row is not None
    assert row.state == 'closed'
    # Retired out of the live set so the disappearance tracker never stamps it.
    assert all(r.client_order_id != 'c14'
               for r in broker.store_ctx.iter_live_orders())
    assert 'missing_pending_since' not in (row.extras or {})
    # BOTH the entry fill (905) and the closing deal (906 — a DIFFERENT
    # order, 999) are on the de-dup channel: the retire was concluded from
    # them, so a late PUSH replay of either must not re-book.
    assert {905, 906} <= broker._seen_deal_ids


def _filled_then_closed_broker(tmp_path, *, entry_commission=0):
    """Working order 111 filled (deal 905) and its position 222 was closed by its
    SL (deal 906, order 999) while the stream was down."""
    entry = _deal(deal_id=905, order_id=111, position_id=222, filled_volume=2000, price=1.2)
    entry.commission = entry_commission
    broker = _ReconcileBroker(_recon(positions=[]), deal_res=_deal_res(
        entry,
        _close_deal(deal_id=906, order_id=999, position_id=222, closed_volume=2000,
                    price=1.15, commission=300)))
    _open(tmp_path, broker)
    _seed_working(broker, 'c14', order_id=111, qty=volume_to_units(2000))
    return broker


def __test_filled_then_closed_trade_is_delivered_with_its_terms__(tmp_path):
    """Entry and close carry their own deal's price and commission."""
    broker = _filled_then_closed_broker(tmp_path, entry_commission=150)

    entry, close = _run(broker)

    assert entry.leg_type is LegType.ENTRY and entry.pine_id == 'long'
    assert entry.fill_price == pytest.approx(1.2) and entry.fee == pytest.approx(1.5)
    assert entry.timestamp == pytest.approx(1_700_000_000.0)
    assert close.leg_type is LegType.CLOSE and close.from_entry == 'long'
    assert close.fill_price == pytest.approx(1.15) and close.fee == pytest.approx(3.0)
    assert close.timestamp == pytest.approx(1_700_000_100.0)


def __test_filled_then_closed_deals_the_push_delivered_are_not_delivered_again__(tmp_path):
    """A deal the push stream already delivered is not delivered by the recovery."""
    broker = _filled_then_closed_broker(tmp_path)
    broker._seen_deal_ids.update({905, 906})

    assert _run(broker) == []
    assert 'c14' not in _live_coids(broker)


def __test_filled_then_closed_waits_for_every_fill_the_journal_counts__(tmp_path):
    """Working order 111 (20 units) had filled 10 units onto position 222 when
    it vanished, and the position's SL then closed those 10 units. The deal
    history first shows only one 5-unit deal (905) of the order: the recovery
    waits. Once the missing deal (904, stamped before the first read's end)
    shows, the next pass reads the order's whole window again and delivers the
    10-unit close once, with no entry fill the journal already counted."""
    now_ms = int(epoch_time() * 1000)

    def at(deal, ms):
        deal.executionTimestamp = ms
        return deal

    broker = _ReconcileBroker(_recon(positions=[]), deal_res=_deal_res(
        at(_deal(deal_id=905, order_id=111, position_id=222, filled_volume=500),
           now_ms - 30_000),
        at(_close_deal(deal_id=906, order_id=999, position_id=222, closed_volume=1000),
           now_ms - 10_000)))
    broker._wire.honour_window = True
    _open(tmp_path, broker)
    _seed_partial_working(broker, 'c14', order_id=111, qty=volume_to_units(2000),
                          filled=volume_to_units(1000), position_id=222)
    row = broker.store_ctx.get_order('c14')
    broker.store_ctx.upsert_order('c14', extras={
        **(row.extras or {}), 'submitted_at_ms': now_ms - 60_000})

    assert _run(broker) == []
    assert 'c14' in _live_coids(broker)

    broker._wire._deal_res.deal.insert(0, at(
        _deal(deal_id=904, order_id=111, position_id=222, filled_volume=500),
        now_ms - 40_000))
    events = _run(broker)

    assert [(e.leg_type, e.fill_qty) for e in events] == [
        (LegType.CLOSE, pytest.approx(volume_to_units(1000)))]
    assert 'c14' not in _live_coids(broker)


def __test_filled_then_closed_close_the_push_delivered_leaves_the_entry_fill__(tmp_path):
    broker = _filled_then_closed_broker(tmp_path)
    broker._seen_deal_ids.add(906)

    events = _run(broker)

    assert [e.leg_type for e in events] == [LegType.ENTRY]


def __test_bridge_excludes_zero_volume_close_detail_ids__(tmp_path):
    # A deal can carry closePositionDetail with closedVolume == 0 (an ack that
    # closed nothing). It contributes nothing to the closure volume and must
    # NOT enter the dedup/evidence channel — only deals that actually closed
    # volume back a CLOSED verdict.
    broker = _ReconcileBroker(
        _recon(),
        deal_res=_deal_res(
            _close_deal(deal_id=907, order_id=999, position_id=222,
                        closed_volume=0),
            _close_deal(deal_id=908, order_id=999, position_id=222,
                        closed_volume=1000),
        ),
    )
    _open(tmp_path, broker)

    bridge = asyncio.run(broker._find_fill_deal(111, 0, close_position_id=222))

    assert bridge.conclusive
    assert bridge.closed_cents == 1000
    assert bridge.closing_deal_ids == (908,)


def __test_partial_vanished_position_linked_not_stamped_on_no_fill__(tmp_path):
    # Surface (c): a partial-filled row whose order vanished and whose deal
    # history returns a conclusive no-fill (the residual was cancelled). It
    # still holds an open partial position (position_id linked, filled > 0), so
    # it must NOT be stamped missing_pending — that would later raise a false
    # UnexpectedCancelError against a live position.
    qty = volume_to_units(2000)
    partial = volume_to_units(1000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=222, volume=1000)]),
        deal_res=_deal_res(),  # conclusive: no new FILLED deal for this order
    )
    _open(tmp_path, broker)
    _seed_partial_working(broker, 'c15', order_id=111, qty=qty,
                          filled=partial, position_id=222)

    assert _run(broker) == []
    row = broker.store_ctx.get_order('c15')
    assert row is not None
    assert 'missing_pending_since' not in (row.extras or {})


def __test_stale_missing_pending_cleared_on_deal_promotion__(tmp_path):
    # A working row stamped missing_pending on a prior no-fill pass, whose fill
    # only surfaces later through the deal-history bridge, must have the stamp
    # CLEARED on promotion. Otherwise a partial promotion (which never re-enters
    # _reconcile_position_row) would carry the stale stamp until the grace window
    # falsely retires the now-filled row and raises UnexpectedCancelError.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=333, volume=2000)]),
        deal_res=_deal_res(_deal(deal_id=901, order_id=111, position_id=333,
                                 filled_volume=2000, price=1.1)),
    )
    _open(tmp_path, broker)
    broker.store_ctx.upsert_order(
        'c16', symbol='EURUSD', side='buy', qty=qty, filled_qty=0.0,
        state='confirmed', pine_entry_id='long', exchange_order_id='111',
        extras={'order_id': '111', 'position_id': None,
                'missing_pending_since': 0.0},
    )
    broker.store_ctx.add_ref('c16', 'order_id', '111')

    events = _run(broker)

    assert len(events) == 1 and events[0].event_type == 'filled'
    row = broker.store_ctx.get_order('c16')
    assert row is not None
    assert (row.extras or {}).get('position_id') == 333
    assert 'missing_pending_since' not in (row.extras or {})


def __test_stale_missing_pending_cleared_when_push_already_applied_fill__(tmp_path):
    # The delayed PUSH fill already advanced filled_qty AND linked the position
    # before the deal bridge sees the same deal: cumulative == row.filled_qty and
    # link_position is false, so the promotion-write branch is skipped. The stale
    # missing_pending stamp must STILL be cleared because the bridge proves the
    # fill — otherwise it survives into the grace window and falsely retires /
    # raises on the live filled row.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=333, volume=2000)]),
        deal_res=_deal_res(_deal(deal_id=902, order_id=111, position_id=333,
                                 filled_volume=2000, price=1.1)),
    )
    _open(tmp_path, broker)
    broker.store_ctx.upsert_order(
        'c17', symbol='EURUSD', side='buy', qty=qty, filled_qty=qty,
        state='confirmed', pine_entry_id='long', exchange_order_id='333',
        extras={'order_id': '111', 'position_id': 333,
                'missing_pending_since': 0.0},
    )
    broker.store_ctx.add_ref('c17', 'order_id', '111')
    broker._seen_deal_ids.add(902)  # PUSH already applied this fill

    events = _run(broker)

    assert events == []  # no duplicate emit — the fill was already booked
    row = broker.store_ctx.get_order('c17')
    assert row is not None
    assert 'missing_pending_since' not in (row.extras or {})


def __test_deal_bridge_from_ms_uses_submitted_at_anchor__(tmp_path):
    # The deal-history window is a per-order since-cursor, not a fixed lookback:
    # fromTimestamp derives from the row's submitted_at_ms anchor (minus a skew
    # margin), so a fill never ages out of a 300s window.
    qty = volume_to_units(2000)
    anchor_ms = 1_700_000_000_000
    broker = _ReconcileBroker(_recon(positions=[]), deal_res=_deal_res())
    _open(tmp_path, broker)
    broker.store_ctx.upsert_order(
        'c16', symbol='EURUSD', side='buy', qty=qty, filled_qty=0.0,
        state='confirmed', pine_entry_id='long', exchange_order_id='111',
        extras={'order_id': '111', 'position_id': None,
                'submitted_at_ms': anchor_ms},
    )
    broker.store_ctx.add_ref('c16', 'order_id', '111')

    _run(broker)

    req = broker._wire.requests[-1]
    assert req.fromTimestamp == anchor_ms - 60_000


# === Disappearance-grace synthetic cancel (2.4) ===========================

def _drive_tracker(broker) -> tuple[list, UnexpectedCancelError | None]:
    """Drive ``_emit_unexpected_cancellations``; capture events and any fail-safe signal."""
    async def collect():
        events: list = []
        err: UnexpectedCancelError | None = None
        try:
            async for e in broker._emit_unexpected_cancellations():
                events.append(e)
        except UnexpectedCancelError as exc:
            err = exc
        return events, err
    return asyncio.run(collect())


def _live_coids(broker) -> list[str]:
    return [r.client_order_id for r in broker.store_ctx.iter_live_orders()]


def __test_grace_expired_missing_settled_position_retires_benignly__(tmp_path):
    # A FULLY FILLED row stamped missing past the grace window, whose final
    # deal-history re-check shows no close and no further fill, is retired as
    # a benign post-fill close under the core cross-venue contract (measured
    # on Capital.com: venues surface late CANCELLED echoes for consumed
    # orders): no cancelled event, no unexpected-cancel policy. A genuine
    # exposure divergence is caught by the venue-vs-book reconcile instead.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_position(broker, 'c1', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert events == []
    assert err is None
    assert 'c1' not in _live_coids(broker)
    row = broker.store_ctx.get_order('c1')
    assert row is not None
    assert row.state == 'closed'
    assert row.closed_ts_ms is not None


def __test_grace_not_expired_missing_pending_is_noop__(tmp_path):
    # A freshly-stamped row is still inside the grace window: no cancel, no fail-safe signal.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon())
    _open(tmp_path, broker)
    _seed_position(broker, 'c2', position_id=222, qty=qty,
                   extras={'missing_pending_since': epoch_time()})

    events, err = _drive_tracker(broker)

    assert events == []
    assert err is None
    assert 'c2' in _live_coids(broker)


def __test_grace_expired_natural_close_booked_as_close_not_cancel__(tmp_path):
    # A native TP/SL (or external) close fired while the stream was down: the
    # grace-expired position's final deal-history re-check shows closedVolume>0,
    # so it is booked as a terminal CLOSE, never a synthetic unexpected_cancel —
    # no cancel event, no fail-safe signal; the close itself is delivered as a fill.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(),
        deal_res=_deal_res(_close_deal(
            deal_id=900, order_id=111, position_id=222, closed_volume=2000)),
    )
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert [(e.event_type, e.fill_id) for e in events] == [('filled', '900')]
    assert err is None                        # no fail-safe signal
    assert 'c3' not in _live_coids(broker)    # retired as a close
    assert 900 in broker._seen_deal_ids       # close deal recorded on the de-dup channel


def __test_grace_expired_close_judged_before_its_verdict_landed_is_delivered_after_a_restart__(
        tmp_path):
    # The process judged the vanished position's close deal, then stopped
    # before the disappearance tracker applied the CLOSED verdict that
    # delivers its fill. The next process finds the deal judged; it still
    # delivers the close fill the judgment journalled, once.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(),
        deal_res=_deal_res(_close_deal(
            deal_id=900, order_id=111, position_id=222, closed_volume=2000)),
    )
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})

    def stop_before_apply(*_args, **_kwargs):
        raise RuntimeError('process stopped')

    broker._disappearance_tracker()._apply_confirmation = stop_before_apply
    with pytest.raises(RuntimeError, match='process stopped'):
        _drive_tracker(broker)
    assert 900 in broker._judged_close_deals
    process = _next_process(broker)

    events, err = _drive_tracker(process)

    assert [(e.event_type, e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('filled', '900', 'long', pytest.approx(qty))]
    assert err is None
    assert 'c3' not in _live_coids(process)
    later = _next_process(process)
    assert _drive_tracker(later) == ([], None)


def __test_helper_engine(broker) -> OrderSyncEngine:
    """A sync engine of ``broker``'s run on its journal, as a process builds it."""
    return OrderSyncEngine(broker=broker, position=BrokerPosition(), symbol='EURUSD',
                           run_tag=broker.store_ctx.run_tag, mintick=0.00001,
                           store_ctx=broker.store_ctx)


def __test_helper_book(engine: OrderSyncEngine, events) -> None:
    """Hand ``events`` to the engine and book them, as the next bar does."""
    for event in events:
        engine.on_order_event(event)
    engine.apply_async_events()


def __test_helper_stream_of(broker, messages) -> list:
    """Start ``broker``'s event stream with ``messages`` queued; the events it delivers."""
    async def run():
        broker._exec_events = asyncio.Queue()
        for message in messages:
            broker._exec_events.put_nowait(message)
        stream = broker.watch_orders()
        events = []
        try:
            while True:
                events.append(await asyncio.wait_for(stream.__anext__(), timeout=0.05))
        except asyncio.TimeoutError:
            return events
        finally:
            await stream.aclose()
    return asyncio.run(run())


def __test_helper_stream_start(broker) -> list:
    """Start ``broker``'s event stream with nothing queued; the events it delivers first."""
    async def run():
        broker._exec_events = asyncio.Queue()
        stream = broker.watch_orders()
        events = []
        try:
            while True:
                events.append(await asyncio.wait_for(stream.__anext__(), timeout=0.05))
        except asyncio.TimeoutError:
            return events
        finally:
            await stream.aclose()
    return asyncio.run(run())


@pytest.mark.parametrize('verdict_applied', [False, True], ids=['before-verdict', 'after-verdict'])
def __test_vanished_close_a_process_died_on_is_in_the_restarted_books_once__(
        tmp_path, monkeypatch, verdict_applied):
    """The vanished position's close deal 900 was judged; the process died
    before its engine booked the close fill: before the tracker applied the
    CLOSED verdict (the rows are live, the journal does not hold the close),
    or after it retired the rows and before the yielded fill was booked (the
    journal holds it). The restart adopts the flat venue either way. Before
    the verdict, the next judgment delivers the close as a new fill; after it,
    the fill is delivered again from its delivery record, marked journaled
    before the restart: the engine records its receipt and books nothing, the
    record goes once the receipt shows, and no later process delivers it."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]),
        deal_res=_deal_res(_close_deal(
            deal_id=900, order_id=111, position_id=222, closed_volume=2000)))
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})
    engine = __test_helper_engine(broker)
    engine.reconcile()
    assert engine._position.size == pytest.approx(qty)
    broker._recon = _recon()
    if verdict_applied:
        events, err = _drive_tracker(broker)
        assert err is None and [(e.fill_id, e.journaled_before_restart) for e in events] == [
            ('900', False)]
        assert _live_coids(broker) == []
        assert set(broker.store_ctx.keyed_events_for_run_id(_FILL_DELIVERY_EVENT)) == {
            'fill:900'}
    else:
        def stop_before_apply(*_args, **_kwargs):
            raise RuntimeError('process stopped')

        broker._disappearance_tracker()._apply_confirmation = stop_before_apply
        with pytest.raises(RuntimeError, match='process stopped'):
            _drive_tracker(broker)
        assert _live_coids(broker) == ['c3']
        assert broker.store_ctx.keyed_events_for_run_id(_FILL_DELIVERY_EVENT) == {}
    # The process dies here: its engine never books the close fill.
    process = _next_process(broker)
    restarted = __test_helper_engine(process)
    restarted.reconcile()
    assert restarted._position.size == pytest.approx(0.0)

    redelivered = __test_helper_stream_start(process)
    judged, err = _drive_tracker(process)
    __test_helper_book(restarted, [*redelivered, *judged])

    assert err is None
    delivered = [(e.fill_id, e.journaled_before_restart) for e in (*redelivered, *judged)]
    assert delivered == [('900', verdict_applied)]
    assert restarted._position.size == pytest.approx(0.0)
    assert len(restarted._position.closed_trades) == 0
    assert _live_coids(process) == []
    assert process.store_ctx.keyed_events_for_run_id(ctrader_reconcile._VANISHED_CLOSE_EVENT) == {}
    if verdict_applied:
        assert process.store_ctx.fill_delivered('900')
        assert process._settle_deliveries() == []
        assert process.store_ctx.keyed_events_for_run_id(_FILL_DELIVERY_EVENT) == {}
        later = _next_process(process)
        assert __test_helper_stream_start(later) == []
        assert _drive_tracker(later) == ([], None)
    else:
        # Held for the venue's verdict, so its receipt waits for that; until
        # then the record keeps the fill for a later process.
        assert set(process.store_ctx.keyed_events_for_run_id(_FILL_DELIVERY_EVENT)) == {
            'fill:900'}


def __test_grace_expired_inconclusive_recheck_defers_retire__(tmp_path):
    # At grace expiry the final deal-history re-check is inconclusive (transport
    # down): never conclude a cancel from missing evidence. The row keeps its
    # stamp and stays live for a later pass — a false cancel would strand exposure.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), wire_fails=True)
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert events == []
    assert err is None
    assert 'c3' in _live_coids(broker)
    row = broker.store_ctx.get_order('c3')
    assert (row.extras or {}).get('missing_pending_since') == 0.0


class _OrderStatusWire(_FakeWire):
    """An empty deal history, and the vanished entry order's own status."""

    def __init__(self, *, status, expiry=None, details_error=None, executed=0,
                 details_deals=()):
        super().__init__(deal_res=_deal_res())
        self._status = status
        self._expiry = expiry
        self._details_error = details_error
        self._executed = executed
        self._details_deals = details_deals

    async def send_request(self, req, *, timeout=None, **kwargs):
        if not isinstance(req, _oa.ProtoOAOrderDetailsReq):
            return await super().send_request(req, timeout=timeout, **kwargs)
        self.requests.append(req)
        if self._details_error is not None:
            raise self._details_error
        order = _order(order_id=req.orderId, volume=2000, executed=self._executed or 0)
        order.orderStatus = self._status
        if self._expiry is not None:
            order.expirationTimestamp = self._expiry
        if self._executed is None:
            order.ClearField('executedVolume')
        return _oa.ProtoOAOrderDetailsRes(order=order, deal=list(self._details_deals))


def _foreign_expiry_wire() -> _OrderStatusWire:
    """The vanished entry order expired at an expiry the bot never set: someone
    else edited it, the one gap-path cancel the unexpected-cancel policy handles."""
    return _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED,
                            expiry=1_700_000_900_000)


def _vanished_working_broker(tmp_path, wire, *, missing_since=0.0,
                             causes=None) -> tuple[_ReconcileBroker, list]:
    """A zero-fill working entry stamped missing past the grace window, with
    the external-intervention reports captured (and the quarantine causes, in
    ``causes``, when given: ``('latch', cause)`` / ``('release', cause)``)."""
    broker = _ReconcileBroker(_recon())
    broker._wire = wire
    _open(tmp_path, broker)
    _seed_working(broker, 'w1', order_id=600, qty=volume_to_units(2000),
                  extras={'missing_pending_since': missing_since})
    reports: list = []

    def sink(reason, context, *, kind, cause=None):
        reports.append((reason, context, kind))
        if causes is not None:
            causes.append(('latch', cause))
        _store_test_latch(broker, reason, context, kind)

    def release(cause, resolution):
        assert resolution
        if causes is not None:
            causes.append(('release', cause))
        return True

    broker.quarantine_sink = sink
    broker.quarantine_release_sink = release
    return broker, reports


def _store_test_latch(broker, reason, context, kind) -> None:
    """Store a test sink's quarantine latch in ``store_ctx`` the way the engine's sink does."""
    broker.store_ctx.record_quarantine(
        'q-test', kind=str(kind), reason=reason, intent_key=None,
        context=context, entered_ts_ms=int(epoch_time() * 1000))


@pytest.mark.parametrize('status', [
    _model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED,
    _model.ProtoOAOrderStatus.ORDER_STATUS_REJECTED,
], ids=['expired', 'rejected'])
def __test_grace_expired_working_order_the_venue_ended_is_no_intervention__(tmp_path, status):
    """The venue expired / rejected the bot's pending entry while the stream was
    down: the cancel carries the venue's reason and the policy never runs."""
    broker, reports = _vanished_working_broker(tmp_path, _OrderStatusWire(status=status))

    events, err = _drive_tracker(broker)

    assert err is None and reports == []
    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert events[0].cancel_reason == CANCEL_REASON_VENUE_REJECTED
    assert 'w1' not in _live_coids(broker)
    assert len(_store_events(broker, 'venue_driven_cancel_recovered')) == 1
    assert _store_events(broker, 'unexpected_cancel') == []


def __test_grace_expired_working_order_with_a_foreign_expiry_is_reported__(tmp_path):
    """A pending entry that expired at an expiry the bot never set was edited by
    someone else: the unexpected cancel the policy handles."""
    broker, reports = _vanished_working_broker(tmp_path, _foreign_expiry_wire())

    events, err = _drive_tracker(broker)

    assert err is None
    assert len(events) == 1 and events[0].cancel_reason is None
    assert len(reports) == 1 and reports[0][2] is QuarantineKind.EXTERNAL_INTERVENTION
    assert 'w1' not in _live_coids(broker)
    assert _store_events(broker, 'ctrader_cancel_unattributed') == []


@pytest.mark.parametrize('wire', [
    _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED),
    _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED, executed=None),
], ids=['cancelled', 'cancelled-without-executed-volume'])
def __test_grace_expired_working_order_cancel_nobody_can_be_named_for_is_audited__(
        tmp_path, caplog, wire):
    """The stream missed the cancel of a pending entry; the order's status reads
    cancelled, and its details list no deal (the answer lists every deal the
    order's fills created), whether or not they carry the executed volume.
    Only the push carries ``isServerEvent``, so a server-initiated cancel and
    someone else's look the same: the cancel is booked, logged as an ERROR and
    audited, and the unexpected-cancel policy never runs."""
    broker, reports = _vanished_working_broker(tmp_path, wire)

    with caplog.at_level(logging.ERROR, logger='pynecore_ctrader.protection'):
        events, err = _drive_tracker(broker)

    assert err is None and reports == []
    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert events[0].cancel_reason in VENUE_DRIVEN_CANCEL_REASONS
    assert 'w1' not in _live_coids(broker)
    audits = _store_events(broker, 'ctrader_cancel_unattributed')
    assert len(audits) == 1 and audits[0]['order_id'] == 600
    assert _store_events(broker, 'unexpected_cancel_quarantine') == []
    assert any(record.levelno == logging.ERROR and 'cannot tell' in record.getMessage()
               for record in caplog.records)


def _not_found_wire() -> _OrderStatusWire:
    return _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED,
                            details_error=CTraderProtocolError('ORDER_NOT_FOUND', 'no such order'))


def __test_grace_expired_working_order_the_venue_does_not_find_quarantines__(tmp_path, caplog):
    """The venue no longer knows a vanished pending entry: no status, executed
    volume or deal list proves it unexecuted, and the deal history listing no
    fill may still lag. The row stays live with its stamp, the bot is
    quarantined (entry block) once with an ERROR under the order's own cause,
    and no cancel is booked; a later pass neither books nor quarantines again."""
    missed = epoch_time() - 60.0
    causes: list = []
    broker, reports = _vanished_working_broker(
        tmp_path, _not_found_wire(), missing_since=missed, causes=causes)

    with caplog.at_level(logging.ERROR, logger='pynecore_ctrader.reconcile'):
        events, err = _drive_tracker(broker)
        again, err_again = _drive_tracker(broker)

    assert events == [] and again == [] and err is None and err_again is None
    assert [(reason, kind) for reason, _context, kind in reports] == [
        ('vanished_order_not_found', QuarantineKind.ENTRY_BLOCK)]
    assert causes == [('latch', 'vanished_order:600')]
    assert reports[0][1]['order_id'] == 600
    assert 'w1' in _live_coids(broker)
    assert broker.store_ctx.get_order('w1').extras.get('missing_pending_since') == missed
    assert _store_events(broker, 'ctrader_cancel_unattributed') == []
    assert sum(record.levelno == logging.ERROR and 'not found' in record.getMessage()
               for record in caplog.records) == 1


def __test_not_found_vanished_order_is_decided_by_its_deal_history__(tmp_path, monkeypatch):
    """The bot gets out of the not-found quarantine by itself: once the venue's
    indexing bound passed since the order was missed and the complete deal
    history still lists no execution, the order executed nothing. It is
    retired as an unattributed cancel (no policy) and the cause is released."""
    missed = epoch_time() - 60.0
    clock = [epoch_time()]
    monkeypatch.setattr(ctrader_reconcile, 'epoch_time', lambda: clock[0])
    causes: list = []
    broker, reports = _vanished_working_broker(
        tmp_path, _not_found_wire(), missing_since=missed, causes=causes)

    assert _drive_tracker(broker) == ([], None)
    assert causes == [('latch', 'vanished_order:600')]
    clock[0] = missed + _UNKNOWN_DISPATCH_TTL_S + 1.0
    events, err = _drive_tracker(broker)

    assert err is None
    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert events[0].cancel_reason in VENUE_DRIVEN_CANCEL_REASONS
    assert 'w1' not in _live_coids(broker)
    assert len(_store_events(broker, 'ctrader_cancel_unattributed')) == 1
    assert causes == [('latch', 'vanished_order:600'), ('release', 'vanished_order:600')]
    assert broker._vanished_not_found_warned == set()
    assert len(reports) == 1


def __test_not_found_vanished_order_missed_long_ago_is_decided_at_once__(tmp_path):
    """A row missed longer ago than the indexing bound needs no quarantine."""
    causes: list = []
    broker, reports = _vanished_working_broker(tmp_path, _not_found_wire(), causes=causes)

    events, err = _drive_tracker(broker)

    assert err is None and reports == [] and causes == []
    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert 'w1' not in _live_coids(broker)


def __test_not_found_vanished_order_whose_fill_the_history_shows_ends_its_cause__(tmp_path):
    """The deal history later lists the order's fill: no cancel, the row waits
    for its promotion, and the quarantine cause is released."""
    causes: list = []
    wire = _not_found_wire()
    broker, _reports = _vanished_working_broker(
        tmp_path, wire, missing_since=epoch_time() - 60.0, causes=causes)
    assert _drive_tracker(broker) == ([], None)

    wire._deal_res = _deal_res(_deal(deal_id=61, order_id=600, position_id=777,
                                     filled_volume=2000))
    events, err = _drive_tracker(broker)

    assert events == [] and err is None
    assert 'w1' in _live_coids(broker)
    assert causes == [('latch', 'vanished_order:600'), ('release', 'vanished_order:600')]


def __test_vanished_order_cause_ends_when_another_path_retires_its_row__(tmp_path):
    """A row the stream or another pass retired, or whose order is back (its
    missing stamp gone), no longer waits: the next snapshot pass releases."""
    causes: list = []
    broker, _reports = _vanished_working_broker(
        tmp_path, _not_found_wire(), missing_since=epoch_time() - 60.0, causes=causes)
    assert _drive_tracker(broker) == ([], None)
    row = broker.store_ctx.get_order('w1')

    broker._release_settled_vanished_causes([(row, 600)])
    assert causes == [('latch', 'vanished_order:600')], "still missing: still held"
    broker.store_ctx.close_order('w1')
    broker._release_settled_vanished_causes([])

    assert causes == [('latch', 'vanished_order:600'), ('release', 'vanished_order:600')]
    assert broker._vanished_not_found_warned == set()


def __test_not_found_vanished_order_is_retired_once_a_resume_accepts_it__(tmp_path):
    """An operator resume accepted the venue as it stands: the not-found order
    is retired as an unattributed cancel, with no quarantine and no policy."""
    broker, reports = _vanished_working_broker(tmp_path, _not_found_wire())
    row = broker.store_ctx.get_order('w1')
    broker.store_ctx.upsert_order('w1', extras={
        **(row.extras or {}), MISSING_ACCEPTED_BY_RESUME_EXTRA: 0.0})

    events, err = _drive_tracker(broker)

    assert err is None and reports == []
    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert 'w1' not in _live_coids(broker)
    assert len(_store_events(broker, 'ctrader_cancel_unattributed')) == 1


@pytest.mark.parametrize('wire', [
    _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED),
    _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED),
    _not_found_wire(),
], ids=['cancelled', 'expired', 'not-found'])
def __test_vanished_order_with_a_reported_execution_floor_is_no_cancel__(tmp_path, wire):
    """An earlier read recorded that the venue reported 10 units executed
    (``executed_cents``), though the cursor counts none yet: whatever the
    order's details say now, the row is no zero-fill cancel. The stamp is
    cleared and the row waits for its fill."""
    broker, reports = _vanished_working_broker(tmp_path, wire)
    row = broker.store_ctx.get_order('w1')
    broker.store_ctx.upsert_order('w1', extras={**(row.extras or {}), 'executed_cents': 1000})

    events, err = _drive_tracker(broker)

    assert events == [] and err is None and reports == []
    assert 'w1' in _live_coids(broker)
    assert 'missing_pending_since' not in (broker.store_ctx.get_order('w1').extras or {})


def __test_vanished_position_linked_order_with_an_execution_floor_is_no_cancel__(tmp_path):
    """A working order linked to a position, whose position history shows no
    close, reaches the cancel verdict without asking the order's details: an
    executed volume the venue reported earlier (``executed_cents``) beyond the
    cursor still makes it no zero-fill cancel. The stamp is cleared and the
    row waits for its fill."""
    broker, reports = _vanished_working_broker(tmp_path, _OrderStatusWire(
        status=_model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED))
    row = broker.store_ctx.get_order('w1')
    broker.store_ctx.upsert_order('w1', extras={
        **(row.extras or {}), 'position_id': 222, 'executed_cents': 1000})

    events, err = _drive_tracker(broker)

    assert events == [] and err is None and reports == []
    assert 'w1' in _live_coids(broker)
    assert 'missing_pending_since' not in (broker.store_ctx.get_order('w1').extras or {})


@pytest.mark.parametrize('status, executed', [
    (_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED, 2000),
    (_model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED, 1000),
    (_model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED, 1000),
    (_model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED, 1000),
], ids=['filled', 'partly-filled', 'partly-filled-cancelled', 'partly-filled-expired'])
def __test_grace_expired_working_order_the_venue_shows_executed_is_no_cancel__(
        tmp_path, status, executed):
    """The deal history does not list the fill yet, but the order's own status
    shows it executed: no cancel and no policy; the stamp is cleared, and the
    snapshot bridge promotes the row once the history lists the fill."""
    broker, reports = _vanished_working_broker(
        tmp_path, _OrderStatusWire(status=status, executed=executed))

    events, err = _drive_tracker(broker)

    assert events == [] and err is None and reports == []
    assert 'w1' in _live_coids(broker)
    assert 'missing_pending_since' not in (broker.store_ctx.get_order('w1').extras or {})


def __test_grace_expired_working_order_whose_details_list_a_fill_is_no_cancel__(tmp_path):
    """The deal history lists no fill yet and the cancelled order's details
    omit its executed volume, but they list a deal its fill created: no
    cancel; the stamp is cleared, and the snapshot bridge promotes the row
    once the history lists the fill."""
    broker, reports = _vanished_working_broker(tmp_path, _OrderStatusWire(
        status=_model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED, executed=None,
        details_deals=(_deal(deal_id=905, order_id=600, position_id=222,
                             filled_volume=1000),)))

    events, err = _drive_tracker(broker)

    assert events == [] and err is None and reports == []
    assert 'w1' in _live_coids(broker)
    assert 'missing_pending_since' not in (broker.store_ctx.get_order('w1').extras or {})


def __test_grace_expired_working_order_the_venue_still_holds_is_no_cancel__(tmp_path):
    """The order's status reads accepted: it still works on the venue, so it is
    neither cancelled nor retired."""
    broker, reports = _vanished_working_broker(
        tmp_path, _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED))

    events, err = _drive_tracker(broker)

    assert events == [] and err is None and reports == []
    assert 'w1' in _live_coids(broker)
    assert 'missing_pending_since' not in (broker.store_ctx.get_order('w1').extras or {})


def __test_grace_expired_working_order_with_an_own_expiry_is_no_intervention__(tmp_path):
    """An expiry the venue's answer to the bot's own write showed is the bot's."""
    broker, reports = _vanished_working_broker(tmp_path, _OrderStatusWire(
        status=_model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED, expiry=1_700_000_900_000))
    answered = _order(order_id=600, volume=2000)
    answered.expirationTimestamp = 1_700_000_900_000
    broker._remember_own_order_attrs('w1', answered)

    events, err = _drive_tracker(broker)

    assert err is None and reports == []
    assert events[0].cancel_reason == CANCEL_REASON_VENUE_REJECTED


@pytest.mark.parametrize('error', [
    CTraderTimeoutError("details read timed out"),
    CTraderProtocolError('INTERNAL_ERROR', 'venue fault'),
], ids=['timeout', 'venue-error'])
def __test_grace_expired_working_order_status_unread_defers_the_verdict__(tmp_path, error):
    """Who ended the order cannot be read now: no verdict, the row waits."""
    broker, reports = _vanished_working_broker(tmp_path, _OrderStatusWire(
        status=_model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED, details_error=error))

    events, err = _drive_tracker(broker)

    assert events == [] and err is None and reports == []
    assert 'w1' in _live_coids(broker)


def __test_grace_expired_position_row_reads_no_order_status__(tmp_path):
    """A settled position's entry order filled: its status cannot name a venue
    cancel, so the re-check spends no request on it."""
    wire = _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED)
    broker = _ReconcileBroker(_recon())
    broker._wire = wire
    _open(tmp_path, broker)
    _seed_position(broker, 'c1', position_id=222, qty=volume_to_units(2000),
                   extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert events == [] and err is None
    assert not any(isinstance(req, _oa.ProtoOAOrderDetailsReq) for req in wire.requests)


def __test_grace_expired_position_close_wins_over_cancel_aged_fill__(tmp_path):
    # A position row with no submitted_at_ms anchor whose entry fill has aged out
    # of the bridge window, but whose KNOWN position id carries a recent close
    # deal (its own orderId differs from the entry order_id), is booked as a close
    # via the close_position_id fallback — never a false cancel.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(),
        deal_res=_deal_res(_close_deal(
            deal_id=901, order_id=999, position_id=222, closed_volume=2000)),
    )
    _open(tmp_path, broker)
    _seed_position(broker, 'c8', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert [(e.event_type, e.fill_id) for e in events] == [('filled', '901')]
    assert err is None
    assert 'c8' not in _live_coids(broker)
    # The closing deal belongs to a DIFFERENT order (999, not the entry's
    # 111), yet it is the evidence the retire was concluded from — it must
    # be on the de-dup channel so a late PUSH replay of the close cannot
    # re-book against the retired position.
    assert 901 in broker._seen_deal_ids


def __test_grace_expired_working_filled_clears_stamp__(tmp_path):
    # A stamped zero-fill working order whose final re-check shows it FILLED (no
    # close) during the gap is not a cancel: the missing-pending premise is now
    # false, so the stamp is cleared and the row left live for the next snapshot
    # pass to promote — never cancelled, never stuck re-bridging.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(),
        deal_res=_deal_res(_deal(
            deal_id=905, order_id=600, position_id=333, filled_volume=2000)),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'w2', order_id=600, qty=qty)
    row = broker.store_ctx.get_order('w2')
    stamped = dict(row.extras or {})
    stamped['missing_pending_since'] = 0.0
    broker.store_ctx.upsert_order('w2', extras=stamped)

    events, err = _drive_tracker(broker)

    assert events == []
    assert err is None
    assert 'w2' in _live_coids(broker)
    row = broker.store_ctx.get_order('w2')
    assert 'missing_pending_since' not in (row.extras or {})


def __test_partial_close_still_open_position_not_terminal_closed__(tmp_path):
    # Invariant behind the relaxed position-row close branch: closedVolume>0 is
    # set for PARTIAL closes too, so a terminal close is booked only when the
    # position was also absent from position[] for the grace window. Here the
    # position is BACK in the snapshot with a recent partial-close deal: pass 1 of
    # _run_reconcile_pass clears the stamp before the retire pass, so no terminal
    # close is booked and the still-open position stays live.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]),
        deal_res=_deal_res(_close_deal(
            deal_id=902, order_id=999, position_id=222, closed_volume=1000)),
    )
    _open(tmp_path, broker)
    _seed_position(broker, 'c9', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})

    async def collect():
        events: list = []
        err: UnexpectedCancelError | None = None
        try:
            async for e in broker._run_reconcile_pass():
                events.append(e)
        except UnexpectedCancelError as exc:
            err = exc
        return events, err

    events, err = asyncio.run(collect())

    assert err is None
    assert 'c9' in _live_coids(broker)            # still live, not terminal-closed
    row = broker.store_ctx.get_order('c9')
    assert 'missing_pending_since' not in (row.extras or {})  # stamp cleared by pass 1


def __test_policy_stop_with_quarantine_sink_raises_no_fail_safe_signal__(tmp_path):
    # With the runner-wired quarantine sink present, the default 'stop' policy
    # latches the engine's external-intervention quarantine instead of raising:
    # the event stream (and the process) stays alive while the bot leaves the
    # venue untouched, so the other bot-owned working order is not cancelled.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon())
    broker._wire = _foreign_expiry_wire()
    latched: list[tuple[str, dict, QuarantineKind]] = []

    def sink(reason, context, *, kind):
        latched.append((reason, context, kind))
        _store_test_latch(broker, reason, context, kind)

    broker.quarantine_sink = sink
    _open(tmp_path, broker)
    _seed_working(broker, 'q1', order_id=222, qty=qty,
                  extras={'missing_pending_since': 0.0})
    _seed_working(broker, 'q2', order_id=333, qty=qty)

    events, err = _drive_tracker(broker)

    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert err is None                      # no fail-safe signal: the stream stays open
    assert 'q1' not in _live_coids(broker)  # row still retired
    assert len(latched) == 1
    reason, context, kind = latched[0]
    assert 'q1' in reason
    assert context['policy'] == 'stop'
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert 'q2' in _live_coids(broker)
    assert not any(isinstance(r, _oa.ProtoOACancelOrderReq)
                   for r in broker._wire.requests)


def __test_policy_ignore_retires_without_a_fail_safe_signal__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon())
    broker._wire = _foreign_expiry_wire()
    broker.on_unexpected_cancel = 'ignore'
    _open(tmp_path, broker)
    _seed_working(broker, 'c4', order_id=222, qty=qty,
                  extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert err is None
    assert 'c4' not in _live_coids(broker)


def __test_policy_re_place_retires_without_a_fail_safe_signal__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon())
    broker._wire = _foreign_expiry_wire()
    broker.on_unexpected_cancel = 're_place'
    _open(tmp_path, broker)
    _seed_working(broker, 'c5', order_id=222, qty=qty,
                  extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert err is None
    assert 'c5' not in _live_coids(broker)


def __test_reconcile_pass_propagates_the_unexpected_cancel_signal__(tmp_path):
    # Integration: the snapshot pass does NOT re-clear a still-missing stamp,
    # and the fail-safe signal of the grace tracker (``stop`` without a wired
    # quarantine sink) propagates through _run_reconcile_pass (it is NOT
    # swallowed by the transient gap-filler guard): the engine latches the
    # external-intervention quarantine on it.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(positions=[]))
    broker._wire = _foreign_expiry_wire()
    _open(tmp_path, broker)
    _seed_working(broker, 'c6', order_id=222, qty=qty,
                  extras={'missing_pending_since': 0.0})

    async def collect():
        events: list = []
        err: UnexpectedCancelError | None = None
        try:
            async for e in broker._run_reconcile_pass():
                events.append(e)
        except UnexpectedCancelError as exc:
            err = exc
        return events, err

    events, err = asyncio.run(collect())

    assert any(e.event_type == 'cancelled' for e in events)
    assert isinstance(err, UnexpectedCancelError)
    assert 'c6' not in _live_coids(broker)


def __test_reconcile_pass_noop_when_live_connection_down__(tmp_path):
    # Regression: after a connection loss (or a teardown that set _wire = None,
    # e.g. the restart-adoption run that cancelled its adopted STOP while the
    # market-closed reconnect gate deferred recovery), the reconcile snapshot
    # must honour its documented no-op contract instead of raising
    # CTraderConnectionError("live connection not established"). Left to raise,
    # _run_reconcile_pass books it as a transient failure and spams the fail
    # streak every pass ("never recovers") until the process is killed. With the
    # real (un-stubbed) _reconcile and _wire = None it must yield nothing, not
    # raise, and leave the transient-failure streak untouched at zero.
    broker = CTrader(symbol=None, config=_make_config())
    broker._live_account_id = None
    broker._wire = None
    _open(tmp_path, broker)
    _seed_working(broker, 'w-down', order_id=700, qty=volume_to_units(2000))

    async def collect():
        events: list = []
        async for e in broker._run_reconcile_pass():
            events.append(e)
        return events

    events = asyncio.run(collect())

    assert events == []
    assert broker._reconcile_fail_streak == 0
    assert 'w-down' in _live_coids(broker)


# === Native fail-safe reconcile-observe (2.5) =============================

def _capture_failsafe(broker) -> list:
    captured: list = []
    broker.native_failsafe_observed_sink = (
        lambda ref, *, stop_level, profit_level, trailing_stop:
        captured.append((ref, stop_level, profit_level, trailing_stop))
    )
    return captured


def __test_reconcile_feeds_native_failsafe_static_levels__(tmp_path):
    # Each live entry whose broker position is open feeds the observed sink
    # with the absolute stopLoss / takeProfit carried by the POSITION
    # attributes, keyed by the row coid. The snapshot also carries a
    # venue-shaped protection order (levels in stopPrice/limitPrice, its
    # stopLoss/takeProfit unset) to prove the feed does not read that order:
    # reading its unset stopLoss fed None for a verifiably-present stop, which
    # the manager booked as an external edit and froze the symbol's entry gate
    # in DEGRADED for the parent's whole lifetime (task #114, cycles 19/20/25).
    qty = volume_to_units(2000)
    pos = _position(position_id=222, volume=2000,
                    stop_loss=1.05, take_profit=1.15)
    prot = _protection_order(position_id=222, stop_loss=1.05, take_profit=1.15)
    broker = _ReconcileBroker(_recon(orders=[prot], positions=[pos]))
    captured = _capture_failsafe(broker)
    _open(tmp_path, broker)
    _seed_position(broker, 'p1', position_id=222, qty=qty)

    _run(broker)

    assert captured == [('p1', pos.stopLoss, pos.takeProfit, None)]


def __test_reconcile_failsafe_trailing_suppresses_stop_level__(tmp_path):
    # cTrader has no relative trailing field: while trailing is active the
    # moving stop is NOT reported as stop_level (it cannot be matched against
    # the engine's relative desired-trailing) and trailing_stop stays None.
    qty = volume_to_units(2000)
    pos = _position(position_id=222, volume=2000,
                    stop_loss=1.05, take_profit=1.15, trailing=True)
    broker = _ReconcileBroker(_recon(positions=[pos]))
    captured = _capture_failsafe(broker)
    _open(tmp_path, broker)
    _seed_position(broker, 'p1', position_id=222, qty=qty)

    _run(broker)

    assert captured == [('p1', None, pos.takeProfit, None)]


def __test_reconcile_failsafe_no_protection_order_degrades__(tmp_path):
    # An open position carrying no protective levels (bracket cleared / never
    # landed) is still observed, with both levels None so the manager degrades.
    qty = volume_to_units(2000)
    pos = _position(position_id=222, volume=2000)
    broker = _ReconcileBroker(_recon(positions=[pos]))
    captured = _capture_failsafe(broker)
    _open(tmp_path, broker)
    _seed_position(broker, 'p1', position_id=222, qty=qty)

    _run(broker)

    assert captured == [('p1', None, None, None)]


def __test_reconcile_no_failsafe_observation_when_position_absent__(tmp_path):
    # A row whose position is not in the open snapshot is not observed.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(positions=[]))
    captured = _capture_failsafe(broker)
    _open(tmp_path, broker)
    _seed_position(broker, 'p1', position_id=222, qty=qty)

    _run(broker)

    assert captured == []


# === Closes the PUSH stream missed: intervention policy ===================

class _VenueWire(_FakeWire):
    """Serves the deal history and ``ProtoOAOrderDetailsReq`` for closing orders."""

    def __init__(self, *, deal_res, orders=(), details_error=None):
        super().__init__(deal_res=deal_res)
        self.orders = {order.orderId: order for order in orders}
        self.details_error = details_error
        self.details_errors_once: list[Exception] = []

    async def send_request(self, req, *, timeout=None, **_kwargs):
        if isinstance(req, _oa.ProtoOAOrderDetailsReq):
            self.requests.append(req)
            if self.details_errors_once:
                raise self.details_errors_once.pop(0)
            if self.details_error is not None:
                raise self.details_error
            if req.orderId not in self.orders:
                # An order the test gives no details of: the read times out.
                raise TimeoutError("order details down")
            return _oa.ProtoOAOrderDetailsRes(order=self.orders[req.orderId])
        return await super().send_request(req, timeout=timeout)


#: Client-order-id of another API client's order: a close it carries is
#: provably someone else's.
_FOREIGN_COID = 'intruder-1'


def _closing_order(order_id, *, order_type=_model.ProtoOAOrderType.MARKET,
                   stop_out=False, coid=_FOREIGN_COID):
    """The closing order ``ProtoOAOrderDetailsRes`` describes; by default another
    API client's order."""
    return _model.ProtoOAOrder(
        orderId=order_id, orderType=order_type,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
        closingOrder=True, positionId=222, isStopOut=stop_out, clientOrderId=coid,
        executedVolume=2000,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=2000,
                                          tradeSide=_model.ProtoOATradeSide.SELL),
    )


def _recovered_close_broker(tmp_path, *orders, details_error=None, executed_ms=None,
                            seed=True):
    """A streaming broker whose position 222 was closed by order 999 while the
    stream was down; the row is stamped missing past the grace window."""
    deal = _close_deal(deal_id=900, order_id=999, position_id=222, closed_volume=2000)
    if executed_ms is not None:
        deal.executionTimestamp = executed_ms
    broker = _ReconcileBroker(_recon())
    broker._wire = _VenueWire(deal_res=_deal_res(deal), orders=orders,
                              details_error=details_error)
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    if seed:
        _seed_position(broker, 'c3', position_id=222, qty=volume_to_units(2000),
                       extras={'missing_pending_since': 0.0})
    return broker, reports


def _details_requests(broker) -> list:
    return [r for r in broker._wire.requests if isinstance(r, _oa.ProtoOAOrderDetailsReq)]


def _recover(broker) -> list:
    """One grace-expiry pass, then the report flush ``watch_orders`` runs after it.

    :return: The events the pass delivered: the recovered close fills.
    """
    events, err = _drive_tracker(broker)
    assert err is None
    broker._flush_external_interventions()
    return events


def __test_recovered_manual_close_is_reported__(tmp_path):
    """A MARKET close of the bot's position the PUSH stream missed is a manual close."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(999))

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert 900 in broker._seen_deal_ids
    assert len(reports) == 1
    _reason, context, kind = reports[0]
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert context['position_id'] == 222 and context['order_id'] == 999
    assert context['deal_ids'] == [900] and context['recovered'] is True
    assert context['closed_qty'] == volume_to_units(2000)
    assert context['from_entry'] == 'long'

    # The pass after the retire never judges the same close again.
    _seed_position(broker, 'c4', position_id=222, qty=volume_to_units(2000),
                   extras={'missing_pending_since': 0.0})
    _recover(broker)
    assert len(reports) == 1


@pytest.mark.parametrize('policy', ['ignore', 're_place'])
def __test_recovered_manual_close_after_a_handled_report_is_reported_again__(
        tmp_path, policy, monkeypatch):
    """The run trades on after a report the policy let pass: a later manual
    close of the same position is judged and reported on its own."""
    broker, _reports = _recovered_close_broker(
        tmp_path, _closing_order(999), _closing_order(998))
    broker.on_unexpected_cancel = policy
    handled: list = []
    report = broker.report_external_intervention
    broker.report_external_intervention = lambda reason, context, **kw: (
        handled.append(context['order_id']), report(reason, context, **kw))
    _recover(broker)
    assert handled == [999]

    _seed_position(broker, 'c4', position_id=222, qty=volume_to_units(2000),
                   extras={'missing_pending_since': 0.0})
    broker._wire._deal_res = _deal_res(
        _close_deal(deal_id=900, order_id=999, position_id=222, closed_volume=2000),
        _close_deal(deal_id=901, order_id=998, position_id=222, closed_volume=2000))
    # The deal walk reads on only once the clock passed its last read: a
    # second pass in the same millisecond would see the first pass's deals
    later = ctrader_reconcile.epoch_time() + 1.0
    monkeypatch.setattr(ctrader_reconcile, 'epoch_time', lambda: later)
    _recover(broker)

    assert 'c4' not in _live_coids(broker)
    assert handled == [999, 998]


def __test_recovered_venue_bracket_close_is_not_reported__(tmp_path):
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(
        999, order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT))

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []


def __test_recovered_venue_bracket_close_is_delivered_as_the_entry_s_exit__(tmp_path):
    """The position's SL / TP fired while the wire was stalled and its push was
    lost (measured live: ctrader cycle 297, position 58811674). The recovered
    closing deal reaches the engine as the entry's close fill, exactly as the
    push would have, so the engine books the exit instead of finding an
    unexplained flat venue."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(
        999, order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT))
    wire = broker._wire
    assert wire is not None
    deal = wire._deal_res.deal[0]
    deal.executionPrice = 1.0995
    deal.commission = 250

    events = _recover(broker)

    assert len(events) == 1
    event = events[0]
    assert event.event_type == 'filled'
    assert event.leg_type is LegType.CLOSE
    assert event.from_entry == 'long' and event.pine_id is None
    assert event.order.side == 'sell' and event.order.reduce_only
    assert event.order.id == '999'
    assert event.order.status is OrderStatus.FILLED
    assert event.fill_qty == pytest.approx(volume_to_units(2000))
    assert event.fill_price == pytest.approx(1.0995)
    assert event.fee == pytest.approx(2.5)
    assert event.fill_id == '900'
    assert event.timestamp == pytest.approx(1_700_000_100.0)
    assert reports == []
    assert 'c3' not in _live_coids(broker)
    assert 900 in broker._seen_deal_ids


def __test_recovered_venue_close_of_a_startup_adopted_position_is_delivered_in_full__(
        tmp_path):
    """No entry row of the run carries a share of the position (a startup-adopted
    one): the venue's close is delivered as the venue shows it."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(
        999, order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT), seed=False)
    _seed_position(broker, 'c3', position_id=222, qty=volume_to_units(2000),
                   extras={'missing_pending_since': 0.0, ADOPTED_STARTUP_EXTRA_KEY: True})
    assert broker._own_share(222) is None

    events = _recover(broker)

    assert [(e.fill_id, e.fill_qty) for e in events] == [
        ('900', pytest.approx(volume_to_units(2000)))]
    assert reports == []


def __test_recovered_close_the_push_stream_delivered_is_not_delivered_again__(tmp_path):
    """A closing deal the push stream already delivered (its deal id is on the
    de-dup channel) retires the row without a second close fill."""
    broker, _reports = _recovered_close_broker(tmp_path, _closing_order(
        999, order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT))
    broker._seen_deal_ids.add(900)

    events = _recover(broker)

    assert events == []
    assert 'c3' not in _live_coids(broker)


def __test_recovered_close_accepted_by_an_operator_resume_delivers_no_fill__(tmp_path):
    """The operator resumed the run accepting the venue as it stands: the close
    is accepted, not delivered."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(
        999, order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT), seed=False)
    _seed_position(broker, 'c3', position_id=222, qty=volume_to_units(2000),
                   extras={'missing_pending_since': 0.0,
                           MISSING_ACCEPTED_BY_RESUME_EXTRA: 0.0})

    events = _recover(broker)

    assert events == []
    assert reports == []
    assert 'c3' not in _live_coids(broker)


def __test_recovered_stop_out_is_not_reported__(tmp_path):
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(999, stop_out=True))

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []


def __test_recovered_own_close_by_order_id_is_not_reported__(tmp_path):
    """The close this run sent, answered with order 999, needs no lookup."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(999))
    broker._settle_own_close(broker._open_own_close(222), 999)

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []
    assert _details_requests(broker) == []


def __test_recovered_close_after_an_own_close_of_unknown_outcome_is_not_reported__(tmp_path):
    """The run's close whose answer was lost may be the one that executed: its
    order carries no client-order-id, as a ``ProtoOAClosePositionReq``'s never does."""
    broker, reports = _recovered_close_broker(
        tmp_path, _closing_order(999, coid=''), executed_ms=int(epoch_time() * 1000))
    broker._settle_own_close(broker._open_own_close(222), None)

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []


def __test_recovered_close_by_an_own_order_is_not_reported__(tmp_path):
    """An order carrying one of this run's client-order-ids closed the volume."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(999, coid='rev1'))
    broker.store_ctx.upsert_order(
        'rev1', symbol='EURUSD', side='sell', qty=20.0, filled_qty=20.0,
        state='closed', pine_entry_id='short', exchange_order_id='999')

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []


def __test_recovered_close_by_an_order_of_the_run_without_a_row_is_not_reported__(tmp_path):
    """An order carrying the run's tag closed the volume, but its journal row
    belongs to an earlier process's run instance, which lookups do not see."""
    broker, reports = _recovered_close_broker(tmp_path)
    broker._wire.orders[999] = _closing_order(999, coid=f"{broker.store_ctx.run_tag}-rev1")
    assert broker.store_ctx.get_order(f"{broker.store_ctx.run_tag}-rev1") is None

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []


def __test_recovered_close_by_an_entry_order_of_a_closed_row_needs_no_lookup__(tmp_path):
    """A closing deal of an entry order whose row is closed is the bot's own."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(999))
    deals = [_PositionDeal(deal_id=900, order_id=999, executed_ms=1_700_000_100_000,
                           filled=2000, closed=2000, side='sell', price=1.1, fee=0.0)]

    taken = asyncio.run(broker._judge_recovered_closes(
        222, deals, from_entry='long', own_orders=(999,)))

    assert taken is not None
    assert _details_requests(broker) == []
    broker._flush_external_interventions()
    assert reports == []


def __test_recovered_close_lookup_failure_defers_the_retire__(tmp_path):
    """Who closed the position is not known yet: neither retired nor reported."""
    broker, reports = _recovered_close_broker(
        tmp_path, _closing_order(999),
        details_error=CTraderTimeoutError("no response"))

    _recover(broker)
    assert 'c3' in _live_coids(broker)
    assert reports == []
    assert 900 not in broker._seen_deal_ids

    broker._wire.details_error = None
    _recover(broker)
    assert 'c3' not in _live_coids(broker)
    assert len(reports) == 1


def __test_recovered_close_rate_limited_lookup_is_retried__(tmp_path, monkeypatch):
    """A throttled lookup waits only within the pass budget, then defers."""
    # Short, yet well above the journal commits the pass makes before its
    # lookup: a commit is a real fsync on Linux (~15 ms each).
    monkeypatch.setattr(ctrader_events, '_RECONCILE_PASS_BUDGET_S', 0.5)
    broker, reports = _recovered_close_broker(
        tmp_path, _closing_order(999),
        details_error=CTraderProtocolError('BLOCKED_PAYLOAD_TYPE', 'slow down'))

    async def one_pass():
        return [event async for event in broker._run_reconcile_pass()]

    assert asyncio.run(one_pass()) == []
    broker._flush_external_interventions()

    assert _details_requests(broker)
    assert 'c3' in _live_coids(broker)
    assert reports == []


@pytest.mark.parametrize('error_code', ['CANT_ROUTE_REQUEST', 'TIMEOUT_ERROR', 'INVALID_REQUEST'])
def __test_recovered_close_lookup_protocol_error_defers_the_retire__(tmp_path, error_code):
    """Only a not-found answer is final: any other venue error is asked again."""
    broker, reports = _recovered_close_broker(
        tmp_path, _closing_order(999),
        details_error=CTraderProtocolError(error_code, 'try later'))

    _recover(broker)
    assert 'c3' in _live_coids(broker)
    assert reports == []
    assert 900 not in broker._judged_close_deals

    broker._wire.details_error = None
    _recover(broker)
    assert 'c3' not in _live_coids(broker)
    assert len(reports) == 1


def __test_recovered_close_lookup_rewins_a_lost_account_session__(tmp_path):
    """A mid-session de-auth is re-won and the lookup retried in the same pass."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(999))
    broker._wire.details_errors_once.append(
        CTraderProtocolError('ACCOUNT_NOT_AUTHORIZED', 'Trading account is not authorized'))
    reauths: list = []

    async def reauth(**kwargs):
        reauths.append(kwargs)

    broker._reauth_account = reauth

    _recover(broker)

    assert len(reauths) == 1
    assert 'c3' not in _live_coids(broker)
    assert len(reports) == 1


def __test_recovered_close_lookup_unrecovered_session_defers_the_retire__(tmp_path):
    broker, reports = _recovered_close_broker(
        tmp_path, _closing_order(999),
        details_error=CTraderProtocolError('ACCOUNT_NOT_AUTHORIZED', 'not authorized'))

    async def reauth(**_kwargs):
        raise CTraderProtocolError('CH_ACCESS_TOKEN_INVALID', 'token revoked')

    async def drop_wire():
        pass

    broker._reauth_account = reauth
    broker._drop_live_wire_after_failed_reauth = drop_wire

    _recover(broker)

    assert 'c3' in _live_coids(broker)
    assert reports == []


def __test_recovered_close_the_venue_cannot_describe_is_audited_not_reported__(tmp_path):
    """No origin can be established for it: retired, never reported, but the
    bot's share it took is an ERROR and an audit event, not a silent pass."""
    broker, reports = _recovered_close_broker(
        tmp_path, _closing_order(999),
        details_error=CTraderProtocolError('ORDER_NOT_FOUND', 'unknown order'))

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []
    audits = _store_events(broker, 'ctrader_close_unattributed')
    assert len(audits) == 1 and audits[0]['deal_ids'] == [900]


def __test_recovered_manual_close_right_after_the_start_is_reported__(tmp_path):
    """No start-up window is skipped: the close executed a moment ago counts."""
    broker, reports = _recovered_close_broker(
        tmp_path, _closing_order(999), executed_ms=int(epoch_time() * 1000))

    _recover(broker)

    assert len(reports) == 1


def _restarted(tmp_path, broker, *orders):
    """A new process of the same run, adopting ``broker``'s journal after it stopped."""
    broker.store_ctx.close()
    restarted, reports = _recovered_close_broker(tmp_path, *orders, seed=False)
    restarted._wire._deal_res = broker._wire._deal_res
    assert 'c3' in _live_coids(restarted)
    return restarted, reports


def __test_recovered_close_an_earlier_process_sent_is_not_reported__(tmp_path):
    first, _ = _recovered_close_broker(tmp_path, _closing_order(999))
    first._settle_own_close(first._open_own_close(222), 999)

    restarted, reports = _restarted(tmp_path, first, _closing_order(999))
    _recover(restarted)

    assert 'c3' not in _live_coids(restarted)
    assert reports == []
    assert _details_requests(restarted) == []


def __test_recovered_close_after_an_earlier_process_died_mid_close_is_not_reported__(tmp_path):
    """The earlier process's close never got its answer: it may be the one that
    executed (its order carries no client-order-id)."""
    first, _ = _recovered_close_broker(
        tmp_path, _closing_order(999, coid=''), executed_ms=int(epoch_time() * 1000))
    first._open_own_close(222)

    restarted, reports = _restarted(tmp_path, first, _closing_order(999, coid=''))
    _recover(restarted)

    assert 'c3' not in _live_coids(restarted)
    assert reports == []


def __test_recovered_close_after_a_stale_unanswered_close_is_reported__(tmp_path):
    """An earlier process's close that never got its answer claims only its grace."""
    first, _ = _recovered_close_broker(
        tmp_path, _closing_order(999), executed_ms=int(epoch_time() * 1000))
    record = first._open_own_close(222)
    record.sent_ms -= 10 * 86_400_000
    first._journal_own_close(record)

    restarted, reports = _restarted(tmp_path, first, _closing_order(999))
    _recover(restarted)

    assert 'c3' not in _live_coids(restarted)
    assert len(reports) == 1
    assert _details_requests(restarted)


def __test_recovered_report_a_dead_process_never_delivered_is_replayed__(tmp_path):
    """The close was judged and journalled, but the process died before the report."""
    first, first_reports = _recovered_close_broker(tmp_path, _closing_order(999))
    events, err = _drive_tracker(first)
    assert [e.fill_id for e in events] == ['900'] and err is None
    assert 900 in first._judged_close_deals and first_reports == []
    first.store_ctx.close()

    restarted, reports = _recovered_close_broker(tmp_path, _closing_order(999), seed=False)
    restarted._flush_external_interventions()
    assert len(reports) == 1
    assert reports[0][1]['position_id'] == 222 and reports[0][1]['deal_ids'] == [900]
    restarted._flush_external_interventions()
    assert len(reports) == 1

    restarted.store_ctx.close()
    third, third_reports = _recovered_close_broker(tmp_path, _closing_order(999), seed=False)
    third._flush_external_interventions()
    assert third_reports == []


def _raising_sink(reason, context, *, kind):
    raise RuntimeError("quarantine sink down")


@pytest.mark.parametrize('sink', [None, _raising_sink], ids=['unwired-sink', 'raising-sink'])
def __test_recovered_report_raised_as_the_fail_safe_signal_is_not_replayed__(tmp_path, sink):
    """The ``stop`` policy cannot latch the quarantine through the sink (none is
    wired, or it raises): the report leaves the stream as the fail-safe
    :class:`UnexpectedCancelError`, on which the engine latches the
    external-intervention quarantine and reopens the stream. The report is
    journalled as handled before the signal leaves, so neither the reopened
    stream of the same process nor a restarted process reports it again."""
    first, _ = _recovered_close_broker(tmp_path, _closing_order(999))
    first.on_unexpected_cancel = 'stop'
    first.quarantine_sink = sink
    _drive_tracker(first)
    with pytest.raises(UnexpectedCancelError):
        first._flush_external_interventions()
    first._flush_external_interventions()
    first.store_ctx.close()

    restarted, reports = _recovered_close_broker(tmp_path, _closing_order(999), seed=False)
    restarted._flush_external_interventions()
    assert reports == []


def __test_recovered_close_after_an_earlier_refused_close_is_reported__(tmp_path):
    first, _ = _recovered_close_broker(
        tmp_path, _closing_order(999), executed_ms=int(epoch_time() * 1000))
    first._drop_own_close(first._open_own_close(222))

    restarted, reports = _restarted(tmp_path, first, _closing_order(999))
    _recover(restarted)

    assert len(reports) == 1


def __test_recovered_close_an_earlier_process_attributed_is_not_judged_again__(tmp_path):
    first, _ = _recovered_close_broker(tmp_path, _closing_order(999))
    first._mark_close_judged([900])

    restarted, reports = _restarted(tmp_path, first, _closing_order(999))
    _recover(restarted)

    assert 'c3' not in _live_coids(restarted)
    assert reports == []
    assert _details_requests(restarted) == []


def __test_recovered_close_the_push_path_attributed_is_not_judged_again__(tmp_path):
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(999))
    broker._mark_close_judged([900])

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []
    assert _details_requests(broker) == []


def __test_recovered_manual_close_of_a_working_order_position_is_reported__(tmp_path):
    """A working order filled and its position was closed by hand, both unseen."""
    broker = _ReconcileBroker(_recon(positions=[]))
    broker._wire = _VenueWire(
        deal_res=_deal_res(
            _deal(deal_id=905, order_id=111, position_id=222, filled_volume=2000),
            _close_deal(deal_id=906, order_id=999, position_id=222, closed_volume=2000)),
        orders=[_closing_order(999)])
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    _seed_working(broker, 'c14', order_id=111, qty=volume_to_units(2000))

    events = _run(broker)
    broker._flush_external_interventions()

    # The trade is the bot's though its close was not: both fills are booked.
    assert [(e.leg_type, e.fill_id) for e in events] == [
        (LegType.ENTRY, '905'), (LegType.CLOSE, '906')]
    assert broker.store_ctx.get_order('c14').state == 'closed'
    assert {905, 906} <= broker._seen_deal_ids
    assert len(reports) == 1
    assert reports[0][1]['deal_ids'] == [906]


def __test_recovered_working_close_lookup_failure_defers_the_retire__(tmp_path):
    broker = _ReconcileBroker(_recon(positions=[]))
    broker._wire = _VenueWire(
        deal_res=_deal_res(
            _deal(deal_id=905, order_id=111, position_id=222, filled_volume=2000),
            _close_deal(deal_id=906, order_id=999, position_id=222, closed_volume=2000)),
        orders=[_closing_order(999)],
        details_error=CTraderTimeoutError("no response"))
    _open(tmp_path, broker)
    _seed_working(broker, 'c14', order_id=111, qty=volume_to_units(2000))

    assert _run(broker) == []

    assert 'c14' in _live_coids(broker)
    assert 906 not in broker._seen_deal_ids


def __test_watch_orders_reports_a_recovered_manual_close_after_the_pass__(
        tmp_path, monkeypatch):
    """Without a quarantine sink the ``stop`` policy raises its fail-safe
    signal: it leaves the stream after the pass that retired the position, and
    the engine latches the external-intervention quarantine on it."""
    monkeypatch.setattr(ctrader_events, '_RECONCILE_CADENCE_S', 0.0)
    broker, _reports = _recovered_close_broker(tmp_path, _closing_order(999))
    broker.quarantine_sink = None

    async def drive():
        broker._exec_events = asyncio.Queue()
        async for _event in broker.watch_orders():
            pass

    with pytest.raises(BrokerManualInterventionError):
        asyncio.run(drive())
    assert 'c3' not in _live_coids(broker)


# === NETTING: recovered closes judged against the bot's share =============

_SLTP = _model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT


def _timed(deal, executed_ms):
    deal.executionTimestamp = executed_ms
    return deal


def _netting_history(*, foreign_closed, sl_closed, full=True, base_ms=1_700_000_000_000):
    """Position 222: the bot opened 20 (order 111), someone else added 10
    (order 500) and closed ``foreign_closed`` (order 600), the SL closed the
    rest (order 700). ``full=False`` leaves the openings out of the window."""
    deals = []
    if full:
        deals += [
            _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
                   base_ms + 1_000),
            _timed(_deal(deal_id=902, order_id=500, position_id=222, filled_volume=1000),
                   base_ms + 2_000),
        ]
    deals += [
        _timed(_close_deal(deal_id=903, order_id=600, position_id=222,
                           closed_volume=foreign_closed), base_ms + 3_000),
        _timed(_close_deal(deal_id=904, order_id=700, position_id=222,
                           closed_volume=sl_closed), base_ms + 4_000),
    ]
    return _deal_res(*deals)


def _netting_recovered_broker(tmp_path, history, *, positions=()):
    broker = _ReconcileBroker(_recon(positions=list(positions)))
    broker._wire = _VenueWire(deal_res=history, orders=[
        _closing_order(600), _closing_order(700, order_type=_SLTP)])
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    return broker, reports


@pytest.mark.parametrize('full', [True, False])
def __test_recovered_netting_close_of_only_foreign_exposure_is_not_reported__(tmp_path, full):
    """Someone closed only the exposure they added; the venue SL closed the
    bot's share. Exact replay from the opening, or bounds without it."""
    broker, reports = _netting_recovered_broker(
        tmp_path, _netting_history(foreign_closed=1000, sl_closed=2000, full=full))
    _seed_position(broker, 'c3', position_id=222, qty=20.0,
                   extras={'missing_pending_since': 0.0})
    broker.store_ctx.add_ref('c3', 'order_id', '111')

    events = _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []
    assert {903, 904} <= broker._judged_close_deals
    # Only the SL's close took the bot's share: it alone is the bot's exit.
    assert [(e.fill_id, e.fill_qty) for e in events] == [('904', pytest.approx(20.0))]


def __test_recovered_netting_close_below_the_share_is_reported__(tmp_path):
    history = _netting_history(foreign_closed=2000, sl_closed=1000)
    sl_deal = next(deal for deal in history.deal if deal.dealId == 903)
    sl_deal.commission = 400
    broker, reports = _netting_recovered_broker(tmp_path, history)
    _seed_position(broker, 'c3', position_id=222, qty=20.0,
                   extras={'missing_pending_since': 0.0})
    broker.store_ctx.add_ref('c3', 'order_id', '111')

    events = _recover(broker)

    assert len(reports) == 1
    context = reports[0][1]
    assert context['deal_ids'] == [903]
    assert context['booked_qty'] == pytest.approx(10.0)
    # Each close is delivered for the part of the bot's share it took, with
    # its commission prorated to that part.
    assert [(e.fill_id, e.fill_qty) for e in events] == [
        ('903', pytest.approx(10.0)), ('904', pytest.approx(10.0))]
    assert events[0].fee == pytest.approx(2.0)


def _shrunk_position_broker(tmp_path, *, closing_order):
    """Position 222 still open with 10 of the bot's 20: someone closed 10
    while the stream was down."""
    history = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               1_700_000_001_000),
        _timed(_close_deal(deal_id=903, order_id=closing_order.orderId, position_id=222,
                           closed_volume=1000), 1_700_000_003_000))
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=1000)]))
    broker._wire = _VenueWire(deal_res=history, orders=[closing_order])
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=20.0)
    broker.store_ctx.add_ref('c3', 'order_id', '111')
    return broker, reports


def _retired(broker, coid='c3') -> float:
    return (broker.store_ctx.get_order(coid).extras or {}).get('journal_exposure_retired', 0.0)


def __test_missed_partial_close_of_an_open_position_is_judged_and_retired__(tmp_path):
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))

    assert _run(broker) == []
    broker._flush_external_interventions()
    assert reports == [] and _retired(broker) == 0.0, "one pass is not proof"

    events = _run(broker)
    broker._flush_external_interventions()
    assert len(reports) == 1
    assert reports[0][1]['deal_ids'] == [903]
    assert reports[0][1]['booked_qty'] == pytest.approx(10.0)
    assert _retired(broker) == pytest.approx(10.0)
    # The close is booked as the live push would book it, report or not.
    assert [(e.leg_type, e.from_entry, e.order.side, e.fill_id, e.fill_qty)
            for e in events] == [(LegType.CLOSE, 'long', 'sell', '903', pytest.approx(10.0))]

    assert _run(broker) == []
    broker._flush_external_interventions()
    assert len(reports) == 1


def __test_missed_venue_partial_close_retires_without_a_report__(tmp_path):
    broker, reports = _shrunk_position_broker(
        tmp_path, closing_order=_closing_order(600, stop_out=True))

    _run(broker)
    events = _run(broker)
    broker._flush_external_interventions()

    assert reports == []
    assert _retired(broker) == pytest.approx(10.0)
    assert [(e.fill_id, e.fill_qty) for e in events] == [('903', pytest.approx(10.0))]
    assert 903 in broker._seen_deal_ids


def __test_missed_partial_close_the_push_delivered_is_not_delivered_again__(tmp_path):
    """The close's push was booked but its deal is judged again by the shrink pass."""
    broker, _reports = _shrunk_position_broker(
        tmp_path, closing_order=_closing_order(600, stop_out=True))
    broker._seen_deal_ids.add(903)

    _run(broker)
    events = _run(broker)

    assert events == []
    assert _retired(broker) == pytest.approx(10.0)


# === Fill recovery pending: holds the engine's external-change verdict =======

def __test_fill_recovery_is_pending_while_a_gone_position_s_row_is_live__(tmp_path):
    """From the moment the snapshot no longer lists the position until the pass
    retires its row, the engine's flatten verdict waits for the recovery."""
    broker, _reports = _recovered_close_broker(tmp_path, _closing_order(
        999, order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT), seed=False)
    _seed_position(broker, 'c3', position_id=222, qty=volume_to_units(2000))
    assert broker.fill_recovery_pending({'long'}) is False, "no snapshot read yet"

    broker._snapshot_open_positions = {}
    assert broker.fill_recovery_pending({'long'}) is True
    assert broker.fill_recovery_pending({'short'}) is False
    broker._external_close_reported.add(222)
    assert broker.fill_recovery_pending({'long'}) is False
    broker._external_close_reported.discard(222)

    store = broker.store_ctx
    assert store is not None
    store.upsert_order('c3', extras={'order_id': '111', 'position_id': 222,
                                     'missing_pending_since': 0.0})
    events = _recover(broker)
    assert [e.fill_id for e in events] == ['900']
    assert broker.fill_recovery_pending({'long'}) is False


def __test_fill_recovery_is_pending_while_a_shrink_is_not_judged__(tmp_path):
    broker, _reports = _shrunk_position_broker(
        tmp_path, closing_order=_closing_order(600, stop_out=True))
    broker._snapshot_open_positions = {222: _position(position_id=222, volume=2000)}
    assert broker.fill_recovery_pending({'long'}) is False

    broker._snapshot_open_positions = {222: _position(position_id=222, volume=1000)}
    assert broker.fill_recovery_pending({'long'}) is True
    _run(broker)
    assert broker.fill_recovery_pending({'long'}) is True, "one pass is not proof"
    _run(broker)
    assert broker.fill_recovery_pending({'long'}) is False


def __test_fill_recovery_is_pending_while_the_tracker_holds_a_stamp__(tmp_path):
    broker = _ReconcileBroker(_recon())
    _open(tmp_path, broker)
    _seed_working(broker, 'c1', order_id=111, qty=20.0,
                  extras={'missing_pending_since': 0.0})
    broker._snapshot_open_positions = {}

    assert broker.fill_recovery_pending({'long'}) is True


def __test_reconcile_read_keeps_the_snapshot_s_open_positions__(tmp_path, monkeypatch):
    broker = _ReconcileBroker(_recon())
    _open(tmp_path, broker)
    closed = _position(position_id=333, volume=1000)
    closed.positionStatus = _model.ProtoOAPositionStatus.POSITION_STATUS_CLOSED
    res = _recon(positions=[_position(position_id=222, volume=2000), closed])

    async def answer(_req):
        return res

    monkeypatch.setattr(broker, '_account_request', answer)
    asyncio.run(CTrader._reconcile(broker))

    assert broker._snapshot_open_positions is not None
    assert set(broker._snapshot_open_positions) == {222}


def __test_late_push_of_a_close_a_shrink_pass_retired_is_not_booked_again__(tmp_path):
    """The bot's own close (order 600) executed while the stream was down; the
    shrink pass judged its deal 903 from the deal history and retired it. Its
    push arriving afterwards is a late copy, never booked twice."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker._settle_own_close(broker._open_own_close(222, 10.0), 600)
    _run(broker)
    _run(broker)
    assert _retired(broker) == pytest.approx(10.0)

    order = _model.ProtoOAOrder(
        orderId=600, orderType=_model.ProtoOAOrderType.MARKET, closingOrder=True,
        positionId=222, executedVolume=1000,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=1000,
                                          tradeSide=_model.ProtoOATradeSide.SELL))
    push = _oa.ProtoOAExecutionEvent(executionType=_model.ProtoOAExecutionType.ORDER_FILLED)
    push.order.CopyFrom(order)
    push.deal.CopyFrom(_model.ProtoOADeal(
        dealId=903, orderId=600, positionId=222, filledVolume=1000, executionPrice=1.1,
        executionTimestamp=1_700_000_003_000,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=1000)))
    push.position.CopyFrom(_position(position_id=222, volume=1000))

    assert broker._translate_exec_event(push) is None
    assert _retired(broker) == pytest.approx(10.0)
    assert reports == []


def _own_close_903(*, price):
    """The fill push of the bot's own close (order 600) of 10 of position 222."""
    order = _model.ProtoOAOrder(
        orderId=600, orderType=_model.ProtoOAOrderType.MARKET, closingOrder=True,
        positionId=222, executedVolume=1000,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=1000,
                                          tradeSide=_model.ProtoOATradeSide.SELL))
    push = _oa.ProtoOAExecutionEvent(executionType=_model.ProtoOAExecutionType.ORDER_FILLED)
    push.order.CopyFrom(order)
    push.deal.CopyFrom(_close_deal_903(price=price))
    push.position.CopyFrom(_position(position_id=222, volume=1000))
    return push


def _close_deal_903(*, price):
    return _timed(_close_deal(deal_id=903, order_id=600, position_id=222, closed_volume=1000,
                              price=price), 1_700_000_003_000)


def _unexplained_shrink_broker(tmp_path):
    """Position 222 holds 10 of the bot's 20; the bot's own close of 10 (order
    600) executed, but neither its push nor its deal reached the bot yet: the
    deal history holds only the opening deal."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker._wire = _VenueWire(deal_res=_deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               1_700_000_001_000)), orders=[_closing_order(600)])
    broker._settle_own_close(broker._open_own_close(222, 10.0), 600)
    broker._snapshot_open_positions = {222: _position(position_id=222, volume=1000)}
    return broker, reports


def __test_late_push_of_a_close_an_unexplained_shrink_waits_for_is_booked_once__(tmp_path):
    """A confirmed shrink whose deal history shows no closing deal waits for
    its explanation; the close's push arriving afterwards is delivered with
    its own deal id and price, and the journal retires its volume once."""
    broker, reports = _unexplained_shrink_broker(tmp_path)
    assert _run(broker) == [] and _run(broker) == []
    assert broker.fill_recovery_pending({'long'}) is True, "the engine's verdict waits"

    event = broker._translate_exec_event(_own_close_903(price=1.2345))

    assert event is not None
    assert (event.leg_type, event.fill_id, event.fill_qty, event.fill_price) == (
        LegType.CLOSE, '903', pytest.approx(10.0), pytest.approx(1.2345))
    assert _retired(broker) == pytest.approx(10.0)
    assert _run(broker) == [] and _run(broker) == []
    assert _retired(broker) == pytest.approx(10.0)
    assert broker.fill_recovery_pending({'long'}) is False
    assert broker._translate_exec_event(_own_close_903(price=1.2345)) is None
    assert reports == []


def __test_closing_deal_the_history_shows_late_explains_a_waiting_shrink__(tmp_path):
    """The deal history shows the close only on a later pass: that pass
    delivers it, and its push arriving afterwards is not booked again."""
    broker, reports = _unexplained_shrink_broker(tmp_path)
    assert _run(broker) == [] and _run(broker) == []
    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               1_700_000_001_000),
        _close_deal_903(price=1.2345))

    events = _run(broker)

    assert [(e.leg_type, e.fill_id, e.fill_qty, e.fill_price) for e in events] == [
        (LegType.CLOSE, '903', pytest.approx(10.0), pytest.approx(1.2345))]
    assert _retired(broker) == pytest.approx(10.0)
    assert broker._translate_exec_event(_own_close_903(price=1.2345)) is None
    assert _run(broker) == []
    assert _retired(broker) == pytest.approx(10.0)
    assert reports == []


def __test_unexplained_shrink_is_retired_once_its_wait_ends__(tmp_path, caplog):
    """No deal explains the shrink before the wait ends: the shortfall is
    retired unbound with an ERROR and an audit event, and the engine's verdict
    is released; a push arriving after that is covered, never booked."""
    broker, reports = _unexplained_shrink_broker(tmp_path)
    assert _run(broker) == []
    broker._shrink_seen_since[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S

    with caplog.at_level(logging.ERROR):
        assert _run(broker) == []

    assert _retired(broker) == pytest.approx(10.0)
    assert any('no deal explains' in record.getMessage() for record in caplog.records)
    store = broker.store_ctx
    assert store is not None
    audits = [payload for _i, _c, _r, payload in store.iter_events_by_kind_for_run_id(
        ctrader_reconcile._SHRINK_UNEXPLAINED_EVENT)]
    assert [(a['position_id'], a['volume']) for a in audits] == [(222, pytest.approx(10.0))]
    assert broker.fill_recovery_pending({'long'}) is False
    assert broker._translate_exec_event(_own_close_903(price=1.2345)) is None
    assert _retired(broker) == pytest.approx(10.0)
    assert reports == []


def _second_unexplained_shrink_broker(tmp_path):
    """The 10-unit shortfall of :func:`_unexplained_shrink_broker` expired and
    was retired unbound; the position then shrank from 10 to 5 with no deal
    explaining that either, and the second wait is running."""
    broker, reports = _unexplained_shrink_broker(tmp_path)
    assert _run(broker) == []
    broker._shrink_seen_since[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S
    assert _run(broker) == [] and _run(broker) == []
    assert _retired(broker) == pytest.approx(10.0)
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])
    assert _run(broker) == [] and _run(broker) == []
    assert 222 in broker._unexplained_shrinks
    assert _retired(broker) == pytest.approx(10.0)
    return broker, reports


def __test_close_an_expired_wait_retired_is_not_booked_by_a_later_wait__(tmp_path):
    """The push of the first shortfall's close, arriving during the wait of a
    second shortfall, is a late copy of a close the journal retired already."""
    broker, reports = _second_unexplained_shrink_broker(tmp_path)

    assert broker._translate_exec_event(_own_close_903(price=1.2345)) is None

    assert _retired(broker) == pytest.approx(10.0)
    assert reports == []


def __test_history_close_an_expired_wait_retired_is_not_booked_by_a_later_wait__(tmp_path):
    """The deal history showing the first shortfall's close only during the
    second wait neither delivers nor debits it; the second shortfall keeps
    waiting for its own close."""
    broker, reports = _second_unexplained_shrink_broker(tmp_path)
    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               1_700_000_001_000),
        _close_deal_903(price=1.2345))

    assert _run(broker) == []

    assert _retired(broker) == pytest.approx(10.0)
    assert 222 in broker._unexplained_shrinks
    assert reports == []


def __test_close_inside_the_skew_window_an_expiry_retired_is_not_booked_again__(tmp_path):
    """The shortfall grows shortly before the wait ends (20 -> 15 -> 10) and the
    deal history shows neither close: the expiry retires the whole 10. The
    push of the second close, executed seconds before that read, is part of
    what the expiry retired and is not booked again."""
    broker, reports = _unexplained_shrink_broker(tmp_path)
    broker._wire.orders[601] = _closing_order(601)
    broker._settle_own_close(broker._open_own_close(222, 5.0), 601)
    broker._recon = _recon(positions=[_position(position_id=222, volume=1500)])
    assert _run(broker) == [] and _run(broker) == []
    assert 222 in broker._unexplained_shrinks
    broker._shrink_seen_since[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S
    broker._unexplained_shrinks[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S
    second_close_ms = int(epoch_time() * 1000) - 10_000
    broker._recon = _recon(positions=[_position(position_id=222, volume=1000)])
    assert _run(broker) == [] and _run(broker) == []
    assert _retired(broker) == pytest.approx(10.0)
    assert 222 not in broker._unexplained_shrinks

    order = _model.ProtoOAOrder(
        orderId=601, orderType=_model.ProtoOAOrderType.MARKET, closingOrder=True,
        positionId=222, executedVolume=500,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=500,
                                          tradeSide=_model.ProtoOATradeSide.SELL))
    push = _oa.ProtoOAExecutionEvent(executionType=_model.ProtoOAExecutionType.ORDER_FILLED)
    push.order.CopyFrom(order)
    push.deal.CopyFrom(_model.ProtoOADeal(
        dealId=906, orderId=601, positionId=222, filledVolume=500, executionPrice=1.1,
        executionTimestamp=second_close_ms,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=500)))
    push.position.CopyFrom(_position(position_id=222, volume=1000))

    assert broker._translate_exec_event(push) is None
    assert _retired(broker) == pytest.approx(10.0)
    assert reports == []


def _foreign_hidden_shrink_broker(tmp_path):
    """NETTING position 222: the bot opened 20 (deal 901), another client added
    5 (deal 902); the bot's own close of 10 (order 600) executed while the
    stream was down, and the deal history does not show its deal 903 yet. The
    venue holds 15: the journal share of 20 is 5 above it (a waiting
    shortfall) and 10 above the bot's real share, the other client's 5 hiding
    the rest."""
    base_ms = 1_700_000_000_000
    deals = [
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               base_ms + 1_000),
        _timed(_deal(deal_id=902, order_id=500, position_id=222, filled_volume=500),
               base_ms + 2_000),
    ]
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=1500)]))
    broker._wire = _VenueWire(deal_res=_deal_res(*deals),
                              orders=[_closing_order(600, coid=''), _closing_order(700)])
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=20.0)
    broker.store_ctx.add_ref('c3', 'order_id', '111')
    broker._settle_own_close(broker._open_own_close(222, 10.0), 600)
    return broker, reports, deals


def _pass(broker) -> list:
    """One reconcile pass, then the report flush ``watch_orders`` runs after it."""
    events = _run(broker)
    broker._flush_external_interventions()
    return events


def __test_close_of_hidden_foreign_exposure_during_a_shrink_wait_takes_no_share__(tmp_path):
    """During the shortfall's wait the other client closes its own 5 (order
    700, deal 904): the bot's real share (10) is the whole venue (10), so the
    close took none of it. The journal share (20) counts the unseen own close,
    which may have executed before 904: the close is ambiguous, neither
    reported nor debited. Once 903 shows, the own close retires its 10 and the
    journal ends at the bot's real share, the venue's 10."""
    broker, reports, deals = _foreign_hidden_shrink_broker(tmp_path)
    assert _pass(broker) == [] and _pass(broker) == []
    assert 222 in broker._unexplained_shrinks
    deals.append(_timed(_close_deal(deal_id=904, order_id=700, position_id=222,
                                    closed_volume=500), 1_700_000_005_000))
    broker._wire._deal_res = _deal_res(*deals)
    broker._recon = _recon(positions=[_position(position_id=222, volume=1000)])

    assert [event for _ in range(3) for event in _pass(broker)] == []

    assert reports == []
    assert _retired(broker) == pytest.approx(0.0)
    deals.append(_close_deal_903(price=1.2345))
    broker._wire._deal_res = _deal_res(*deals)

    events = [event for _ in range(3) for event in _pass(broker)]

    assert [(e.fill_id, e.fill_qty) for e in events] == [('903', pytest.approx(10.0))]
    assert _retired(broker) == pytest.approx(10.0)
    assert broker._own_share(222) == pytest.approx(10.0)
    assert reports == []


def __test_close_of_the_bot_s_share_during_a_shrink_wait_is_still_reported__(tmp_path):
    """The same wait without the other client's exposure: the bot holds the
    whole venue (10) after its unseen close, and order 700 closing 5 takes 5
    of it whichever way the unseen close ran. That is reported and retired;
    903 retires its own 10 once it shows."""
    broker, reports, deals = _foreign_hidden_shrink_broker(tmp_path)
    deals.pop()
    broker._wire._deal_res = _deal_res(*deals)
    broker._recon = _recon(positions=[_position(position_id=222, volume=1000)])
    assert _pass(broker) == [] and _pass(broker) == []
    deals.append(_timed(_close_deal(deal_id=904, order_id=700, position_id=222,
                                    closed_volume=500), 1_700_000_005_000))
    broker._wire._deal_res = _deal_res(*deals)
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])

    events = [event for _ in range(3) for event in _pass(broker)]

    assert [(e.fill_id, e.fill_qty) for e in events] == [('904', pytest.approx(5.0))]
    assert [(r[1]['deal_ids'], r[1]['booked_qty']) for r in reports] == [
        ([904], pytest.approx(5.0))]
    deals.append(_close_deal_903(price=1.2345))
    broker._wire._deal_res = _deal_res(*deals)

    events = [event for _ in range(3) for event in _pass(broker)]

    assert [(e.fill_id, e.fill_qty) for e in events] == [('903', pytest.approx(10.0))]
    assert _retired(broker) == pytest.approx(15.0)
    assert broker._own_share(222) == pytest.approx(5.0)
    assert len(reports) == 1


def _expired_two_entry_shrink_broker(tmp_path):
    """L1 (c1, order 111) and L2 (c2, order 112) hold 10 each on position
    222; the venue shows 15 and no deal explains the 5 missing. The wait
    expires and retires the 5 from L1, the oldest. The bot's close of L2
    (order 602, 10 units) is sent right after that read. The deal history
    honours each read's time window, and each row's window starts at its own
    order's dispatch, as the execution path stamps it."""
    history = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000))
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=1500)]))
    broker._wire = _VenueWire(deal_res=history, orders=[_closing_order(600)])
    broker._wire.honour_window = True
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    for coid, pine_id, order_id, submitted_ms in (
            ('c1', 'L1', '111', 1_700_000_001_000), ('c2', 'L2', '112', 1_700_000_002_000)):
        broker.store_ctx.upsert_order(
            coid, symbol='EURUSD', side='buy', qty=10.0, filled_qty=10.0,
            state='confirmed', pine_entry_id=pine_id, exchange_order_id='222',
            extras={'order_id': order_id, 'position_id': 222,
                    'submitted_at_ms': submitted_ms})
        broker.store_ctx.add_ref(coid, 'order_id', order_id)
    broker._settle_own_close(broker._open_own_close(222, 5.0), 600)
    broker._snapshot_open_positions = {222: _position(position_id=222, volume=1500)}
    assert _run(broker) == [] and _run(broker) == []
    assert 222 in broker._unexplained_shrinks
    broker._shrink_seen_since[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S
    broker._unexplained_shrinks[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S
    assert _run(broker) == []
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(0.0)
    assert 222 not in broker._unexplained_shrinks
    broker._close_dispatch_pine_by_position[222] = '__pyne_partial__L2'
    broker._close_settle_entry_by_position[222] = 'L2'
    broker._wire.orders[602] = _model.ProtoOAOrder(
        orderId=602, orderType=_model.ProtoOAOrderType.MARKET, closingOrder=True,
        positionId=222, executedVolume=1000,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=1000,
                                          tradeSide=_model.ProtoOATradeSide.SELL))
    broker._settle_own_close(broker._open_own_close(222, 10.0), 602)
    return broker, reports


def _shift_expiry(broker, shift):
    """Move the expiry of :func:`_expired_two_entry_shrink_broker` and the
    closes sent around it ``shift`` ms into the past, as if that much time
    had passed since; return the overlap as it was before the move. The deal
    history reads made so far move with them: each walk resumes from where
    the expiry's reads ended, never from the real time they ran at."""
    start, end = broker._expiry_overlaps[222]
    broker._expiry_overlaps[222] = (start - shift, end - shift)
    broker._closes_covered_until[222] -= shift
    for walk in broker._deal_walks.values():
        walk.high_ms = max(walk.from_ms, walk.high_ms - shift)
        for segment in walk.segments:
            segment[0] = max(walk.from_ms, segment[0] - shift)
            segment[1] = max(walk.from_ms, segment[1] - shift)
    for record in broker._own_closes:
        record.sent_ms -= shift
        if record.answered_ms is not None:
            record.answered_ms -= shift
    return start, end


def __test_named_close_pushed_inside_an_expiry_overlap_is_booked_against_its_entry__(
        tmp_path):
    """A close the expiry snapshot did not hold yet, executed inside the clock
    skew after its read, is never booked from its push, even one reporting
    the position it left: the deal history's judgment books it under its own
    deal and entry, exactly once."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    push = _oa.ProtoOAExecutionEvent(executionType=_model.ProtoOAExecutionType.ORDER_FILLED)
    push.order.CopyFrom(broker._wire.orders[602])
    push.deal.CopyFrom(_model.ProtoOADeal(
        dealId=907, orderId=602, positionId=222, filledVolume=1000, executionPrice=1.1,
        executionTimestamp=close_ms,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=1000)))
    push.position.CopyFrom(_position(position_id=222, volume=500))

    assert broker._translate_exec_event(push) is None
    assert _retired(broker, 'c2') == pytest.approx(0.0)
    broker._snapshot_open_positions = {222: _position(position_id=222, volume=500)}
    assert broker.fill_recovery_pending({'L2'}) is True, "the engine's verdict waits"

    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms))
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])

    events = [event for _ in range(2) for event in _run(broker)]

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0))]
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(10.0)
    assert reports == []


@pytest.mark.parametrize('pushed', [True, False])
def __test_vanished_position_does_not_book_a_close_its_expiry_retired__(tmp_path, pushed):
    """After the expiry (20 -> 15), the deal history of the position, gone
    by now, shows the retired 5-unit close (order 600) and the 10-unit close
    of L2 (order 602), both in the overlap, then a 5-unit close (order 603)
    that emptied it. The vanished rows' judgment delivers only the 15 the
    journal still counted: 602 and 603, never the close the expiry retired,
    whether or not the overlap pushes arrived first."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    last_ms = end - shift + 1_000
    broker._wire.orders[603] = _closing_order(603)
    broker._settle_own_close(broker._open_own_close(222, 5.0), 603)
    if pushed:
        for deal_id, order_id, cents, executed_ms, left in (
                (908, 600, 500, retired_ms, 1500), (907, 602, 1000, close_ms, 500)):
            push = _oa.ProtoOAExecutionEvent(
                executionType=_model.ProtoOAExecutionType.ORDER_FILLED)
            push.order.CopyFrom(_model.ProtoOAOrder(
                orderId=order_id, orderType=_model.ProtoOAOrderType.MARKET,
                closingOrder=True, positionId=222, executedVolume=cents,
                orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
                tradeData=_model.ProtoOATradeData(
                    symbolId=1, volume=cents, tradeSide=_model.ProtoOATradeSide.SELL)))
            push.deal.CopyFrom(_model.ProtoOADeal(
                dealId=deal_id, orderId=order_id, positionId=222, filledVolume=cents,
                executionPrice=1.1, executionTimestamp=executed_ms,
                closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=cents)))
            push.position.CopyFrom(_position(position_id=222, volume=left))
            assert broker._translate_exec_event(push) is None
    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms),
        _timed(_close_deal(deal_id=909, order_id=603, position_id=222, closed_volume=500),
               last_ms))
    for coid in ('c1', 'c2'):
        row = broker.store_ctx.get_order(coid)
        broker.store_ctx.upsert_order(
            coid, extras={**(row.extras or {}), 'missing_pending_since': 0.0})
    assert broker._own_share(222) == pytest.approx(15.0)

    events, err = _drive_tracker(broker)

    assert err is None
    assert sorted((e.fill_id, e.from_entry, e.fill_qty)
                  for e in events if e.fill_id is not None) == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _live_coids(broker) == []
    assert reports == []


def __test_vanished_position_waits_for_an_own_close_that_may_still_execute__(tmp_path):
    """The vanished position of the test above, with one more own close
    (order 604, 5 units) sent after the expiry, answered and still within its
    grace. The rows' judgment waits for it; once it ends without a fill, 602
    and 603 are delivered whole, never cut by 604's volume."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    last_ms = end - shift + 1_000
    broker._wire.orders[603] = _closing_order(603)
    broker._settle_own_close(broker._open_own_close(222, 5.0), 603)
    pending = broker._open_own_close(222, 5.0)
    broker._settle_own_close(pending, 604)
    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms),
        _timed(_close_deal(deal_id=909, order_id=603, position_id=222, closed_volume=500),
               last_ms))
    for coid in ('c1', 'c2'):
        row = broker.store_ctx.get_order(coid)
        broker.store_ctx.upsert_order(
            coid, extras={**(row.extras or {}), 'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert err is None
    assert [e for e in events if e.fill_id is not None] == []
    assert sorted(_live_coids(broker)) == ['c1', 'c2']

    pending.complete = True
    events, err = _drive_tracker(broker)

    assert err is None
    assert sorted((e.fill_id, e.from_entry, e.fill_qty)
                  for e in events if e.fill_id is not None) == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _live_coids(broker) == []
    assert reports == []


def __test_retired_overlap_deal_shown_first_does_not_take_a_missing_close_shortfall__(
        tmp_path):
    """After the expiry (20 -> 15), the bot's close of L2 (order 602, 10
    units) takes the venue to 5, but the deal history shows only the 5-unit
    close the expiry retired (order 600). That deal never takes the shortfall
    the missing close leaves: nothing is delivered or retired until the close
    of L2 shows, which is then booked under its own deal and entry."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    # The close of L2 was sent a second after the expiry's read.
    for record in broker._own_closes:
        if record.order_id == 602:
            record.sent_ms += 1_000
    entries = (
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms))
    broker._wire._deal_res = _deal_res(*entries)
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])

    assert [event for _ in range(2) for event in _run(broker)] == []
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(0.0)

    broker._wire._deal_res = _deal_res(*entries, _timed(
        _close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000), close_ms))
    events = [event for _ in range(2) for event in _run(broker)]

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0))]
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(10.0)
    assert reports == []


def __test_confirmed_close_missing_from_history_does_not_let_a_retired_deal_book__(
        tmp_path):
    """As above, but the answer of the close of L2 already confirmed its
    10-unit fill (deal 907), which the deal history does not show yet. The
    5-unit close the expiry retired (order 600) is never booked in its place:
    nothing is delivered until 907 shows, which is then booked whole."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    record = next(r for r in broker._own_closes if r.order_id == 602)
    record.sent_ms += 1_000
    record.count_executed(907, 10.0)
    record.complete = True
    entries = (
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms))
    broker._wire._deal_res = _deal_res(*entries)
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])

    assert [event for _ in range(3) for event in _run(broker)] == []
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(0.0)

    broker._wire._deal_res = _deal_res(*entries, _timed(
        _close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000), close_ms))
    events = [event for _ in range(2) for event in _run(broker)]

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0))]
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(10.0)
    assert reports == []


def __test_pending_own_close_does_not_suppress_a_shown_overlap_close__(tmp_path):
    """The history already shows the 10-unit close of L2 (deal 907) in the
    overlap, while a second own close (order 604, 5 units), sent after the
    expiry, is answered but may still execute. The judgment waits for it; once
    604 ends without a fill, 907 is booked whole, not cut by 604's volume."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    second = broker._open_own_close(222, 5.0)
    broker._settle_own_close(second, 604)
    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms))
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])

    assert [event for _ in range(3) for event in _run(broker)] == []
    assert _retired(broker, 'c2') == pytest.approx(0.0)

    second.complete = True
    events = [event for _ in range(2) for event in _run(broker)]

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0))]
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(10.0)
    assert reports == []


def _shrink_past_deadline_with_a_pending_close(tmp_path):
    """The shrink of the test above, past its wait, while the own close 604
    (5 units, sent after the expiry) is answered and may still execute; the
    close of L2 (deal 907, order 602) was sent after the expiry's read too.

    :return: ``(broker, reports, second)``, ``second`` being 604's record.
    """
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    for record in broker._own_closes:
        if record.order_id == 602:
            record.sent_ms += 1_000
    second = broker._open_own_close(222, 5.0)
    broker._settle_own_close(second, 604)
    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms))
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])

    assert _run(broker) == []
    broker._shrink_seen_since[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S
    return broker, reports, second


def __test_shrink_past_its_deadline_waits_for_an_own_close_that_may_still_execute__(
        tmp_path):
    """Past the shrink's wait, the judgment still waits while 604 may execute,
    as the vanished rows' judgment does: nothing is delivered or retired. Once
    604 ends without a fill, 907 is booked whole, never cut by the volume
    reserved for 604, and nothing more follows."""
    broker, reports, second = _shrink_past_deadline_with_a_pending_close(tmp_path)

    assert [event for _ in range(2) for event in _run(broker)] == []
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(0.0)
    # The engine's position check reads the same snapshot: its verdict stays held.
    broker._snapshot_open_positions = {222: _position(position_id=222, volume=500)}
    assert broker.fill_recovery_pending({'L1', 'L2'})

    second.complete = True
    events = _run(broker)

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0))]
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(10.0)
    assert [event for _ in range(2) for event in _run(broker)] == []
    assert _retired(broker, 'c2') == pytest.approx(10.0)
    assert reports == []


def __test_shrink_past_its_deadline_stops_waiting_at_the_own_close_s_grace__(tmp_path):
    """As above, but 604 never reports its end: the wait past the deadline is
    bounded by 604's grace, after which 907 is booked whole."""
    broker, reports, second = _shrink_past_deadline_with_a_pending_close(tmp_path)

    assert _run(broker) == []
    assert _retired(broker, 'c2') == pytest.approx(0.0)

    second.expires_at = 0.0
    events = _run(broker)

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0))]
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(10.0)
    assert reports == []


def __test_vanished_position_waits_for_a_confirmed_close_missing_from_history__(tmp_path):
    """The vanished position of the tests above, but the answer of the close
    of L2 (order 602) already confirmed its 10-unit fill (deal 907), which
    the deal history does not show yet: it shows only the close the expiry
    retired (908) and the 5-unit close that emptied the position (909). The
    rows' judgment waits within the shrink wait; once 907 shows, 907 and 909
    are delivered whole and 908 never."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    last_ms = end - shift + 1_000
    record = next(r for r in broker._own_closes if r.order_id == 602)
    record.sent_ms += 1_000
    record.count_executed(907, 10.0)
    record.complete = True
    broker._wire.orders[603] = _closing_order(603)
    broker._settle_own_close(broker._open_own_close(222, 5.0), 603)
    entries = (
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=909, order_id=603, position_id=222, closed_volume=500),
               last_ms))
    broker._wire._deal_res = _deal_res(*entries)
    missing_since = epoch_time() - ctrader_reconcile._MISSING_PENDING_GRACE_S - 1.0
    for coid in ('c1', 'c2'):
        row = broker.store_ctx.get_order(coid)
        broker.store_ctx.upsert_order(
            coid, extras={**(row.extras or {}), 'missing_pending_since': missing_since})

    events, err = _drive_tracker(broker)

    assert err is None
    assert [e for e in events if e.fill_id is not None] == []
    assert sorted(_live_coids(broker)) == ['c1', 'c2']

    broker._wire._deal_res = _deal_res(*entries, _timed(
        _close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000), close_ms))
    events, err = _drive_tracker(broker)

    assert err is None
    assert sorted((e.fill_id, e.from_entry, e.fill_qty)
                  for e in events if e.fill_id is not None) == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _live_coids(broker) == []
    assert reports == []


def __test_vanished_position_rereads_the_window_for_a_late_shown_earlier_deal__(tmp_path):
    """As above, with the deal history honouring each read's time window as
    the venue does. The close of L2 (deal 907) executed more than
    :data:`_SINCE_ANCHOR_SKEW_MS` before the waiting check read the history,
    and shows only afterwards: a resumed walk would read from that read's end
    on and never see it. The waiting check discards the walk, so the next one
    reads the order's whole window again and finds 907."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    assert broker._wire.honour_window
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    last_ms = end - shift + 1_000
    record = next(r for r in broker._own_closes if r.order_id == 602)
    record.sent_ms += 1_000
    record.count_executed(907, 10.0)
    record.complete = True
    broker._wire.orders[603] = _closing_order(603)
    broker._settle_own_close(broker._open_own_close(222, 5.0), 603)
    entries = (
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=909, order_id=603, position_id=222, closed_volume=500),
               last_ms))
    broker._wire._deal_res = _deal_res(*entries)
    missing_since = epoch_time() - ctrader_reconcile._MISSING_PENDING_GRACE_S - 1.0
    for coid in ('c1', 'c2'):
        row = broker.store_ctx.get_order(coid)
        broker.store_ctx.upsert_order(
            coid, extras={**(row.extras or {}), 'missing_pending_since': missing_since})

    events, err = _drive_tracker(broker)

    assert err is None
    assert [e for e in events if e.fill_id is not None] == []
    assert sorted(_live_coids(broker)) == ['c1', 'c2']
    first_read = broker._wire.requests[-1]
    assert isinstance(first_read, _oa.ProtoOADealListReq)
    # 907 lies before the part of the window a resumed walk would read again.
    assert close_ms < first_read.toTimestamp - ctrader_reconcile._SINCE_ANCHOR_SKEW_MS

    broker._wire._deal_res = _deal_res(*entries, _timed(
        _close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000), close_ms))
    events, err = _drive_tracker(broker)

    assert err is None
    assert sorted((e.fill_id, e.from_entry, e.fill_qty)
                  for e in events if e.fill_id is not None) == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _live_coids(broker) == []
    assert reports == []


def __test_vanished_position_waits_for_a_confirmed_close_before_any_overlap_deal__(
        tmp_path):
    """As above, but the first check's history shows no overlap deal at all:
    only the 5-unit close that emptied the position (909). The confirmed
    10-unit close of L2 (order 602) is still missing from it, so the rows
    stay pending; once 908 and 907 show, 907 and 909 are delivered whole and
    908 never."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    last_ms = end - shift + 1_000
    record = next(r for r in broker._own_closes if r.order_id == 602)
    record.sent_ms += 1_000
    record.count_executed(907, 10.0)
    record.complete = True
    broker._wire.orders[603] = _closing_order(603)
    broker._settle_own_close(broker._open_own_close(222, 5.0), 603)
    entries = (
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000))
    final = _timed(
        _close_deal(deal_id=909, order_id=603, position_id=222, closed_volume=500), last_ms)
    broker._wire._deal_res = _deal_res(*entries, final)
    missing_since = epoch_time() - ctrader_reconcile._MISSING_PENDING_GRACE_S - 1.0
    for coid in ('c1', 'c2'):
        row = broker.store_ctx.get_order(coid)
        broker.store_ctx.upsert_order(
            coid, extras={**(row.extras or {}), 'missing_pending_since': missing_since})

    events, err = _drive_tracker(broker)

    assert err is None
    assert [e for e in events if e.fill_id is not None] == []
    assert sorted(_live_coids(broker)) == ['c1', 'c2']

    broker._wire._deal_res = _deal_res(
        *entries,
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms),
        final)
    events, err = _drive_tracker(broker)

    assert err is None
    assert sorted((e.fill_id, e.from_entry, e.fill_qty)
                  for e in events if e.fill_id is not None) == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _live_coids(broker) == []
    assert reports == []


@pytest.mark.parametrize('read_past_it', [False, True])
def __test_vanished_adopted_position_books_its_close_from_the_position_history__(
        tmp_path, read_past_it):
    """A row startup adopted from an open position knows its position but no
    entry order. Its 5-unit shrink expires, then the position's SL closes the
    rest inside the expiry overlap and the position vanishes. The rows'
    judgment reads the position's own history and books that close, never
    retiring the row without its fill, even when an earlier read of the
    position's history ended more than the re-read margin after the close's
    execution without showing it (``read_past_it``)."""
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=500)]))
    broker._wire = _VenueWire(deal_res=_deal_res(), orders=[
        _closing_order(700, order_type=_SLTP)])
    broker._wire.honour_window = True
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)

    async def tick(_symbol_id):
        return 0.00001

    # The adopted protection's check needs the symbol's tick, which no order
    # of this run loaded: the venue's rules are not under test here.
    broker._resolve_tick = tick
    broker._adopt_own_leg(_position(position_id=222, volume=1000))
    coid = _live_coids(broker)[0]
    assert 'order_id' not in (broker.store_ctx.get_order(coid).extras or {})
    broker._snapshot_open_positions = {222: _position(position_id=222, volume=500)}
    assert _run(broker) == [] and _run(broker) == []
    assert 222 in broker._unexplained_shrinks
    broker._shrink_seen_since[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S
    broker._unexplained_shrinks[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S
    assert _run(broker) == []
    assert _retired(broker, coid) == pytest.approx(5.0)
    start, end = broker._expiry_overlaps[222]
    broker._wire._deal_res = _deal_res(_timed(
        _close_deal(deal_id=950, order_id=700, position_id=222, closed_volume=500),
        (start + end) // 2))
    if read_past_it:
        walk = broker._deal_walks[-222]
        assert not walk.segments
        walk.high_ms = (start + end) // 2 + ctrader_reconcile._SINCE_ANCHOR_SKEW_MS + 1_000
    row = broker.store_ctx.get_order(coid)
    broker.store_ctx.upsert_order(coid, extras={
        **(row.extras or {}),
        'missing_pending_since': epoch_time() - ctrader_reconcile._MISSING_PENDING_GRACE_S - 1.0})

    events, err = _drive_tracker(broker)

    assert err is None
    assert [(e.fill_id, e.fill_qty) for e in events if e.fill_id is not None] == [
        ('950', pytest.approx(5.0))]
    assert _live_coids(broker) == []
    assert reports == []


def __test_vanished_position_waits_for_a_confirmed_close_before_any_close_shows__(tmp_path):
    """As the tests above, but the first check's history shows no closing
    deal at all, only the entry fills. The confirmed 10-unit close of L2
    (order 602) is missing from it, so the rows are not retired as a benign
    close; once 908, 907 and 909 show, 907 and 909 are delivered whole and
    908 never."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    last_ms = end - shift + 1_000
    record = next(r for r in broker._own_closes if r.order_id == 602)
    record.sent_ms += 1_000
    record.count_executed(907, 10.0)
    record.complete = True
    broker._wire.orders[603] = _closing_order(603)
    broker._settle_own_close(broker._open_own_close(222, 5.0), 603)
    entries = (
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000))
    broker._wire._deal_res = _deal_res(*entries)
    missing_since = epoch_time() - ctrader_reconcile._MISSING_PENDING_GRACE_S - 1.0
    for coid in ('c1', 'c2'):
        row = broker.store_ctx.get_order(coid)
        broker.store_ctx.upsert_order(
            coid, extras={**(row.extras or {}), 'missing_pending_since': missing_since})

    events, err = _drive_tracker(broker)

    assert err is None
    assert [e for e in events if e.fill_id is not None] == []
    assert sorted(_live_coids(broker)) == ['c1', 'c2']

    broker._wire._deal_res = _deal_res(
        *entries,
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms),
        _timed(_close_deal(deal_id=909, order_id=603, position_id=222, closed_volume=500),
               last_ms))
    events, err = _drive_tracker(broker)

    assert err is None
    assert sorted((e.fill_id, e.from_entry, e.fill_qty)
                  for e in events if e.fill_id is not None) == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _live_coids(broker) == []
    assert reports == []


def __test_vanished_rows_dispatched_after_the_overlap_read_its_closes__(tmp_path):
    """The vanished position of the tests above, with L2's row dispatched
    after the expiry overlap: its own window starts after the close of L2
    (deal 907) and the close the expiry retired (908), and its grace ends
    first. Its judgment, which retires L1's row too, reads the windows of
    every row of the position, so 907 is delivered with 909 and 908 never,
    instead of the rows closing on 909 alone."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 240_000
    # The last close is sent before the expiry moves, so it moves with it.
    broker._wire.orders[603] = _closing_order(603)
    broker._settle_own_close(broker._open_own_close(222, 5.0), 603)
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    last_ms = end - shift + 90_000
    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms),
        _timed(_close_deal(deal_id=909, order_id=603, position_id=222, closed_volume=500),
               last_ms))
    for coid, extras in (('c1', {'missing_pending_since': epoch_time()}),
                         ('c2', {'submitted_at_ms': last_ms, 'missing_pending_since': 0.0})):
        row = broker.store_ctx.get_order(coid)
        broker.store_ctx.upsert_order(coid, extras={**(row.extras or {}), **extras})
    assert close_ms < last_ms - ctrader_reconcile._SINCE_ANCHOR_SKEW_MS

    events, err = _drive_tracker(broker)

    assert err is None
    assert sorted((e.fill_id, e.from_entry, e.fill_qty)
                  for e in events if e.fill_id is not None) == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _live_coids(broker) == []
    assert reports == []


def _filled_then_closed_after_expiry_broker(tmp_path, *, second_entry=False):
    """After the expiry (20 -> 15), the close of L2 (deal 907, order 602,
    10 units) leaves 5; a working entry L3 (c3, order 113, dispatched after
    the overlap) fills 3 units onto the same NETTING position, and the bot's
    close of the rest (order 603, 8 units) empties it, all while the stream
    was down. With ``second_entry``, a second working entry L4 (c4, order
    114) fills 2 units after L3 and the close of the rest takes 10. Return
    the broker and the deal history's closes."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 240_000
    # The last close is sent before the expiry moves, so it moves with it.
    broker._wire.orders[603] = _closing_order(603)
    rest = 10.0 if second_entry else 8.0
    broker._settle_own_close(broker._open_own_close(222, rest), 603)
    start, end = _shift_expiry(broker, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    entry_ms = end - shift + 70_000
    broker.store_ctx.upsert_order(
        'c3', symbol='EURUSD', side='buy', qty=3.0, filled_qty=0.0, state='confirmed',
        pine_entry_id='L3', exchange_order_id='113',
        extras={'order_id': '113', 'position_id': None, 'submitted_at_ms': entry_ms})
    broker.store_ctx.add_ref('c3', 'order_id', '113')
    entries = [_timed(_deal(deal_id=903, order_id=113, position_id=222, filled_volume=300),
                      entry_ms + 1_000)]
    if second_entry:
        broker.store_ctx.upsert_order(
            'c4', symbol='EURUSD', side='buy', qty=2.0, filled_qty=0.0, state='confirmed',
            pine_entry_id='L4', exchange_order_id='114',
            extras={'order_id': '114', 'position_id': None, 'submitted_at_ms': entry_ms})
        broker.store_ctx.add_ref('c4', 'order_id', '114')
        entries.append(_timed(
            _deal(deal_id=904, order_id=114, position_id=222, filled_volume=200),
            entry_ms + 1_500))
    for entry in entries:
        entry.tradeSide = _model.ProtoOATradeSide.BUY
    closes = (
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms),
        *entries,
        _timed(_close_deal(deal_id=909, order_id=603, position_id=222,
                           closed_volume=int(rest * 100)),
               entry_ms + 2_000))
    broker._recon = _recon()
    assert close_ms < entry_ms - ctrader_reconcile._SINCE_ANCHOR_SKEW_MS
    return broker, reports, closes


def __test_filled_then_closed_entry_judges_the_position_s_closes_like_its_rows__(tmp_path):
    """The working entry's recovery retires every row of the position, so it
    judges the closes as the vanished rows would: from the closes earlier
    passes retired on, which its own window starts after. It delivers its
    fill, the close of L2 (907) and the last close (909) whole, under their
    own labels, and never the close the expiry retired (908)."""
    broker, reports, closes = _filled_then_closed_after_expiry_broker(tmp_path)
    broker._wire._deal_res = _deal_res(*closes)

    events = _run(broker)

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('903', None, pytest.approx(3.0)),
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(8.0))]
    assert _live_coids(broker) == []
    assert reports == []


def __test_filled_then_closed_entry_waits_for_a_confirmed_close_missing_from_history__(
        tmp_path):
    """As above, but the answer of the close of L2 (order 602) already
    confirmed its 10-unit fill, whose deal 907 the history does not show yet.
    The entry's recovery leaves every row live until 907 shows."""
    broker, reports, closes = _filled_then_closed_after_expiry_broker(tmp_path)
    record = next(r for r in broker._own_closes if r.order_id == 602)
    record.sent_ms += 1_000
    record.count_executed(907, 10.0)
    record.complete = True
    broker._wire._deal_res = _deal_res(*(deal for deal in closes if deal.dealId != 907))

    assert _run(broker) == []
    assert sorted(_live_coids(broker)) == ['c1', 'c2', 'c3']

    broker._wire._deal_res = _deal_res(*closes)
    events = _run(broker)

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('903', None, pytest.approx(3.0)),
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(8.0))]
    assert _live_coids(broker) == []
    assert reports == []


def __test_filled_then_closed_entries_of_one_position_are_recovered_together__(tmp_path):
    """Two working entries (L3, L4) filled onto the position before it was
    emptied. The first one's recovery judges the closes against the share
    counting both fills, delivers both entry fills before the closes, and
    retires both rows: the delivered stream ends flat like the venue."""
    broker, reports, closes = _filled_then_closed_after_expiry_broker(
        tmp_path, second_entry=True)
    broker._wire._deal_res = _deal_res(*closes)

    events = [event for _ in range(2) for event in _run(broker)]

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('903', None, pytest.approx(3.0)),
        ('904', None, pytest.approx(2.0)),
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(10.0))]
    assert sum(e.fill_qty for e in events if e.leg_type is LegType.ENTRY) + 15.0 == (
        pytest.approx(sum(e.fill_qty for e in events if e.leg_type is LegType.CLOSE)))
    assert _live_coids(broker) == []
    assert reports == []


def __test_settled_snapshot_after_the_overlap_ends_it__(tmp_path):
    """A settled snapshot read more than the clock skew after the overlap
    ended shows the venue holding the share: every overlap close is then
    retired, and a later shortfall never books one of them."""
    broker, _reports = _expired_two_entry_shrink_broker(tmp_path)
    _start, end = _shift_expiry(broker, 2 * ctrader_reconcile._CLOCK_SKEW_MS + 1_000)
    assert _run(broker) == []
    assert 222 not in broker._expiry_overlaps
    assert broker._closes_covered_until[222] == (
        end - 2 * ctrader_reconcile._CLOCK_SKEW_MS - 1_000)


def __test_overlap_push_after_an_entry_and_a_close_does_not_book_a_retired_close__(
        tmp_path):
    """The case a push-side volume comparison cannot order: the expiry
    retired a 5-unit close (20 -> 15), then a 5-unit entry (20) and a booked
    5-unit close (15) brought the share back to the expiry's level. The late
    push of the retired close still reports the 15 it left; it books
    nothing, and the deal history, finding no shortfall, books nothing
    either."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    broker.store_ctx.upsert_order(
        'c3', symbol='EURUSD', side='buy', qty=5.0, filled_qty=5.0,
        state='confirmed', pine_entry_id='L3', exchange_order_id='222',
        extras={'order_id': '113', 'position_id': 222})
    broker.store_ctx.add_ref('c3', 'order_id', '113')
    broker._retire_partial_close_exposure(222, 5.0, source='position_shrink', entry_id='L3')
    assert broker._own_share(222) == pytest.approx(15.0)
    order = _model.ProtoOAOrder(
        orderId=600, orderType=_model.ProtoOAOrderType.MARKET, closingOrder=True,
        positionId=222, executedVolume=500,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=500,
                                          tradeSide=_model.ProtoOATradeSide.SELL))
    push = _oa.ProtoOAExecutionEvent(executionType=_model.ProtoOAExecutionType.ORDER_FILLED)
    push.order.CopyFrom(order)
    push.deal.CopyFrom(_model.ProtoOADeal(
        dealId=908, orderId=600, positionId=222, filledVolume=500, executionPrice=1.1,
        executionTimestamp=int(epoch_time() * 1000) - 10_000,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=500)))
    push.position.CopyFrom(_position(position_id=222, volume=1500))

    assert broker._translate_exec_event(push) is None
    broker._recon = _recon(positions=[_position(position_id=222, volume=1500)])
    assert _run(broker) == [] and _run(broker) == []

    assert broker._own_share(222) == pytest.approx(15.0)
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(0.0)
    assert reports == []


def __test_named_close_in_history_inside_an_expiry_overlap_is_booked_against_its_entry__(
        tmp_path):
    """The same close known only from the deal history: the next shrink
    judgment books it, newest first against the shortfall, under its own
    deal and entry, and nothing the expiry retired is booked again."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    # The expiry read happened two minutes ago, the close right after it.
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms))
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])

    events = [event for _ in range(2) for event in _run(broker)]

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0))]
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(10.0)
    assert reports == []


def __test_positionless_overlap_push_leaves_its_close_to_the_deal_history__(tmp_path):
    """A push of the overlap close that reports no position cannot tell it
    from one the expiry retired: it books nothing and leaves its deal unseen,
    so the deal history's judgment delivers the fill under its own deal and
    entry, exactly once."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    shift = 120_000
    start, end = _shift_expiry(broker, shift)
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    push = _oa.ProtoOAExecutionEvent(executionType=_model.ProtoOAExecutionType.ORDER_FILLED)
    push.order.CopyFrom(broker._wire.orders[602])
    push.deal.CopyFrom(_model.ProtoOADeal(
        dealId=907, orderId=602, positionId=222, filledVolume=1000, executionPrice=1.1,
        executionTimestamp=close_ms,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=1000)))

    assert broker._translate_exec_event(push) is None
    assert _retired(broker, 'c2') == pytest.approx(0.0)

    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms))
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])

    events = [event for _ in range(2) for event in _run(broker)]

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0))]
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c2') == pytest.approx(10.0)
    assert reports == []


def __test_overlap_push_after_a_refill_does_not_book_a_retired_close_again__(tmp_path):
    """The expiry retired a 5-unit close executed just before its read (20 ->
    15); a new 5-unit entry then restored journal and venue to 20. The late
    push of the retired close still reports the 15 it left: against the share
    of now it would look new, so it is left to the deal history, which finds
    no shortfall, and nothing is booked again."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    broker.store_ctx.upsert_order(
        'c3', symbol='EURUSD', side='buy', qty=5.0, filled_qty=5.0,
        state='confirmed', pine_entry_id='L3', exchange_order_id='222',
        extras={'order_id': '113', 'position_id': 222})
    broker.store_ctx.add_ref('c3', 'order_id', '113')
    assert broker._own_share(222) == pytest.approx(20.0)
    order = _model.ProtoOAOrder(
        orderId=600, orderType=_model.ProtoOAOrderType.MARKET, closingOrder=True,
        positionId=222, executedVolume=500,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=500,
                                          tradeSide=_model.ProtoOATradeSide.SELL))
    push = _oa.ProtoOAExecutionEvent(executionType=_model.ProtoOAExecutionType.ORDER_FILLED)
    push.order.CopyFrom(order)
    push.deal.CopyFrom(_model.ProtoOADeal(
        dealId=908, orderId=600, positionId=222, filledVolume=500, executionPrice=1.1,
        executionTimestamp=int(epoch_time() * 1000) - 10_000,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=500)))
    push.position.CopyFrom(_position(position_id=222, volume=1500))

    assert broker._translate_exec_event(push) is None
    broker._recon = _recon(positions=[_position(position_id=222, volume=2000)])
    assert _run(broker) == [] and _run(broker) == []

    assert broker._own_share(222) == pytest.approx(20.0)
    assert _retired(broker, 'c1') == pytest.approx(5.0)
    assert _retired(broker, 'c3') == pytest.approx(0.0)
    assert reports == []


def __test_waiting_shrink_reread_cut_short_resumes_where_it_stopped__(tmp_path):
    """A full reread of the deal history the pass budget cut short is resumed
    on the next pass, never restarted from the window's start."""
    broker, _reports = _unexplained_shrink_broker(tmp_path)
    assert _run(broker) == [] and _run(broker) == []
    walk = broker._deal_walks[-222]
    walk.segments.append([walk.from_ms, walk.from_ms + 1])

    _run(broker)

    assert broker._deal_walks[-222] is walk


def __test_equal_shrink_after_the_share_came_back_is_judged_again__(tmp_path):
    """A shrink judged once does not hide a later one to the same volumes on
    the same position, once the position held the share in between."""
    broker, reports = _shrunk_position_broker(
        tmp_path, closing_order=_closing_order(600, stop_out=True))
    broker._wire.orders[601] = _closing_order(601, stop_out=True)
    _run(broker)
    _run(broker)
    assert _retired(broker) == pytest.approx(10.0)
    _seed_position(broker, 'c4', position_id=222, qty=10.0, extras={'order_id': '112'})
    broker.store_ctx.add_ref('c4', 'order_id', '112')
    broker._recon = _recon(positions=[_position(position_id=222, volume=2000)])
    _run(broker)
    # The first shrink was judged in venue time before the second close.
    broker._closes_covered_until[222] = 1_700_000_003_500
    broker._recon = _recon(positions=[_position(position_id=222, volume=1000)])
    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               1_700_000_001_000),
        _timed(_close_deal(deal_id=903, order_id=600, position_id=222, closed_volume=1000),
               1_700_000_003_000),
        _timed(_deal(deal_id=904, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_004_000),
        _timed(_close_deal(deal_id=905, order_id=601, position_id=222, closed_volume=1000),
               1_700_000_005_000))

    _run(broker)
    _run(broker)

    assert _retired(broker) + _retired(broker, 'c4') == pytest.approx(20.0)
    assert reports == []


def __test_equal_shrink_after_a_rebound_no_snapshot_saw_is_judged_again__(tmp_path):
    """An entry fill that restores the share and a second equal close, both
    between two snapshots, are a new shrink: the first judgment hides nothing."""
    broker, reports = _shrunk_position_broker(
        tmp_path, closing_order=_closing_order(600, stop_out=True))
    broker._wire.orders[601] = _closing_order(601, stop_out=True)
    _run(broker)
    _run(broker)
    assert _retired(broker) == pytest.approx(10.0)
    # No snapshot shows the position back at 20: the entry and the second
    # close both happen before the next read.
    _seed_position(broker, 'c4', position_id=222, qty=10.0, extras={'order_id': '112'})
    broker.store_ctx.add_ref('c4', 'order_id', '112')
    broker._closes_covered_until[222] = 1_700_000_003_500
    broker._recon = _recon(positions=[_position(position_id=222, volume=1000)])
    broker._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               1_700_000_001_000),
        _timed(_close_deal(deal_id=903, order_id=600, position_id=222, closed_volume=1000),
               1_700_000_003_000),
        _timed(_deal(deal_id=904, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_004_000),
        _timed(_close_deal(deal_id=905, order_id=601, position_id=222, closed_volume=1000),
               1_700_000_005_000))

    _run(broker)
    _run(broker)

    assert _retired(broker) + _retired(broker, 'c4') == pytest.approx(20.0)
    assert reports == []


def __test_shrink_is_not_judged_while_an_event_waits_in_the_queue__(tmp_path):
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker._exec_events = asyncio.Queue()
    broker._exec_events.put_nowait(object())

    for _ in range(3):
        _run(broker)
    broker._flush_external_interventions()

    assert reports == [] and _retired(broker) == 0.0


def __test_shrink_a_restart_found_is_judged_against_the_share_before_it__(tmp_path):
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker._apply_adoption_baseline(broker._recon)
    assert _retired(broker) == pytest.approx(10.0)
    assert _startup_volumes(broker) == {222: pytest.approx(10.0)}

    _run(broker)
    broker._flush_external_interventions()

    assert len(reports) == 1 and reports[0][1]['booked_qty'] == pytest.approx(10.0)
    assert _retired(broker) == pytest.approx(10.0)
    assert broker._startup_shrinks == {}


def _masked_own_close_broker(tmp_path):
    """NETTING position 222: the bot opened 10 (order 111), someone else added
    5 (order 500), then the bot's own close of its 10 (order 888) executed but
    its fill push was missed. The venue still holds the other party's 5."""
    history = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=500, position_id=222, filled_volume=500),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=903, order_id=888, position_id=222, closed_volume=1000),
               1_700_000_003_000))
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=500)]))
    broker._wire = _VenueWire(deal_res=history, orders=[])
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=10.0)
    broker.store_ctx.add_ref('c3', 'order_id', '111')
    broker._settle_own_close(broker._open_own_close(222, 10.0), 888)
    return broker, reports


def _foreign_close_push(*, order_id, closed_cents, remaining_cents):
    """Another client's MARKET close of position 222, as its fill push."""
    order = _model.ProtoOAOrder(
        orderId=order_id, orderType=_model.ProtoOAOrderType.MARKET,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED, closingOrder=True,
        positionId=222, executedVolume=closed_cents,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=closed_cents,
                                          tradeSide=_model.ProtoOATradeSide.SELL))
    status = (_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN if remaining_cents
              else _model.ProtoOAPositionStatus.POSITION_STATUS_CLOSED)
    return _oa.ProtoOAExecutionEvent(
        executionType=_model.ProtoOAExecutionType.ORDER_FILLED, order=order,
        deal=_model.ProtoOADeal(dealId=order_id + 1, orderId=order_id, positionId=222,
                                filledVolume=closed_cents, executionPrice=1.1),
        position=_model.ProtoOAPosition(
            positionId=222, positionStatus=status,
            tradeData=_model.ProtoOATradeData(symbolId=1, volume=remaining_cents,
                                              tradeSide=_model.ProtoOATradeSide.BUY)))


def __test_netting_shrink_retires_what_the_deals_took_not_the_shortfall__(tmp_path):
    """The venue (5) is only 5 below the bot's journal share (10), but the
    bot's own close took all 10: the journal keeps no phantom share, so the
    other party closing its own 5 later is not taken for an intervention."""
    broker, reports = _masked_own_close_broker(tmp_path)

    for _ in range(2):
        _run(broker)
    broker._flush_external_interventions()

    assert reports == []
    assert broker._own_share(222) == pytest.approx(0.0)
    assert broker._translate_exec_event(_foreign_close_push(
        order_id=70900000, closed_cents=500, remaining_cents=0)) is None
    broker._flush_external_interventions()
    assert reports == []


def __test_netting_own_close_found_in_the_deal_history_ends_its_uncertainty__(tmp_path):
    """Bot 20 + other party 5; the bot's close of 10 (order 888) executed but
    its fill push was missed (25 -> 15). Judging the shrink finds the close in
    the deal history: the journal holds the bot's 10 and nothing of the close
    runs unseen any more, so another party closing 7 later (15 -> 8) took 2
    of the bot's 10."""
    history = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=500, position_id=222, filled_volume=500),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=903, order_id=888, position_id=222, closed_volume=1000),
               1_700_000_003_000))
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=1500)]))
    broker._wire = _VenueWire(deal_res=history, orders=[])
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=20.0)
    broker.store_ctx.add_ref('c3', 'order_id', '111')
    broker._settle_own_close(broker._open_own_close(222, 10.0), 888)
    assert broker._unresolved_own_close(222) == pytest.approx(10.0)

    for _ in range(2):
        _run(broker)
    broker._flush_external_interventions()

    assert reports == []
    assert broker._own_share(222) == pytest.approx(10.0)
    assert broker._unresolved_own_close(222) == pytest.approx(0.0)
    out = broker._translate_exec_event(_foreign_close_push(
        order_id=70900000, closed_cents=700, remaining_cents=800))
    broker._flush_external_interventions()
    assert out is not None and out.fill_qty == pytest.approx(2.0)
    assert len(reports) == 1 and reports[0][1]['booked_qty'] == pytest.approx(2.0)


def __test_netting_shrink_books_an_own_close_of_a_newer_entry_against_that_entry__(tmp_path):
    """L1 (order 111) and L2 (order 112) hold 10 each; the bot's engine-made
    close of L2 (order 888, 5 units) was answered, then its fill push was
    missed and the process restarted. The shrink pass finds the close in the
    deal history by its order id: it is delivered under that close's label
    and retired from L2, the entry the core settles it against, not L1."""
    history = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=903, order_id=888, position_id=222, closed_volume=500),
               1_700_000_003_000))
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=1500)]))
    broker._wire = _VenueWire(deal_res=history, orders=[])
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    for coid, pine_id, order_id in (('c1', 'L1', '111'), ('c2', 'L2', '112')):
        broker.store_ctx.upsert_order(
            coid, symbol='EURUSD', side='buy', qty=10.0, filled_qty=10.0,
            state='confirmed', pine_entry_id=pine_id, exchange_order_id='222',
            extras={'order_id': order_id, 'position_id': 222})
        broker.store_ctx.add_ref(coid, 'order_id', order_id)
    broker._close_dispatch_pine_by_position[222] = '__pyne_partial__L2'
    broker._close_settle_entry_by_position[222] = 'L2'
    broker._settle_own_close(broker._open_own_close(222, 5.0), 888)
    process = _next_process(broker)

    events = [event for _ in range(2) for event in _run(process)]
    process._flush_external_interventions()

    assert reports == []
    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('903', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _retired(process, 'c1') == pytest.approx(0.0)
    assert _retired(process, 'c2') == pytest.approx(5.0)


def __test_netting_shrink_interrupted_at_its_debit_is_judged_again_after_a_restart__(tmp_path):
    """L1 and L2 hold 10 each; the bot's close of L2 (order 888, 5 units)
    executed with its fill push missed. The process judging the shrink stops
    at the debit of L2: nothing of that judgment is durable, so the next
    process judges deal 903 again and books it against L2, not oldest first."""
    history = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=903, order_id=888, position_id=222, closed_volume=500),
               1_700_000_003_000))
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=1500)]))
    broker._wire = _VenueWire(deal_res=history, orders=[])
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    for coid, pine_id, order_id in (('c1', 'L1', '111'), ('c2', 'L2', '112')):
        broker.store_ctx.upsert_order(
            coid, symbol='EURUSD', side='buy', qty=10.0, filled_qty=10.0,
            state='confirmed', pine_entry_id=pine_id, exchange_order_id='222',
            extras={'order_id': order_id, 'position_id': 222})
        broker.store_ctx.add_ref(coid, 'order_id', order_id)
    broker._close_dispatch_pine_by_position[222] = '__pyne_partial__L2'
    broker._close_settle_entry_by_position[222] = 'L2'
    broker._settle_own_close(broker._open_own_close(222, 5.0), 888)
    stopped = _next_process(broker)

    def stop_at_debit(*_args, **_kwargs):
        raise RuntimeError('process stopped')

    stopped._retire_partial_close_exposure = stop_at_debit
    _run(stopped)
    with pytest.raises(RuntimeError, match='process stopped'):
        _run(stopped)
    process = _next_process(stopped)

    events = [event for _ in range(2) for event in _run(process)]
    process._flush_external_interventions()

    assert reports == []
    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('903', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _retired(process, 'c1') == pytest.approx(0.0)
    assert _retired(process, 'c2') == pytest.approx(5.0)
    # A process that stops after the judgment, before the engine books the
    # close fill, leaves the engine of the next one its book from the journal.
    engine = OrderSyncEngine(broker=process, position=BrokerPosition(), symbol='EURUSD',
                             run_tag=process.store_ctx.run_tag, mintick=0.00001,
                             store_ctx=process.store_ctx)
    assert engine._recover_adopted_parent_trades(15.0, fifo_trim=True) == [
        ('L1', pytest.approx(10.0)), ('L2', pytest.approx(5.0))]


def __test_netting_shrink_interrupted_at_its_debit_is_judged_again_by_the_same_process__(
        tmp_path):
    """As above, but the pass that failed at the debit of L2 is retried by the
    same process: the state it kept of the rolled-back judgment is put back,
    so deal 903 is judged again and booked against L2."""
    history = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=903, order_id=888, position_id=222, closed_volume=500),
               1_700_000_003_000))
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=1500)]))
    broker._wire = _VenueWire(deal_res=history, orders=[])
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    for coid, pine_id, order_id in (('c1', 'L1', '111'), ('c2', 'L2', '112')):
        broker.store_ctx.upsert_order(
            coid, symbol='EURUSD', side='buy', qty=10.0, filled_qty=10.0,
            state='confirmed', pine_entry_id=pine_id, exchange_order_id='222',
            extras={'order_id': order_id, 'position_id': 222})
        broker.store_ctx.add_ref(coid, 'order_id', order_id)
    broker._close_dispatch_pine_by_position[222] = '__pyne_partial__L2'
    broker._close_settle_entry_by_position[222] = 'L2'
    broker._settle_own_close(broker._open_own_close(222, 5.0), 888)
    process = _next_process(broker)

    def stop_at_debit(*_args, **_kwargs):
        raise RuntimeError('debit failed')

    process._retire_partial_close_exposure = stop_at_debit
    _run(process)
    with pytest.raises(RuntimeError, match='debit failed'):
        _run(process)
    del process._retire_partial_close_exposure

    events = [event for _ in range(2) for event in _run(process)]
    process._flush_external_interventions()

    assert reports == []
    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('903', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _retired(process, 'c1') == pytest.approx(0.0)
    assert _retired(process, 'c2') == pytest.approx(5.0)


def __test_close_answered_after_a_rolled_back_judgment_settles_the_record_the_run_tracks__(
        tmp_path):
    """The bot's close of position 222 waits for its answer while a close
    judgment rolls back. The answer then names order 888: it settles the close
    record the run tracks, so the position's share is settled again and the
    close no longer counts as in flight."""
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=1000)]))
    _open(tmp_path, broker)

    async def scenario() -> None:
        sent = asyncio.Event()
        release = asyncio.Event()

        async def dispatch(req, *, coid, context, predecessor_cancel_ids=None):
            sent.set()
            await release.wait()
            event = _oa.ProtoOAExecutionEvent(
                executionType=_model.ProtoOAExecutionType.ORDER_ACCEPTED)
            event.order.CopyFrom(_model.ProtoOAOrder(
                orderId=888, clientOrderId=coid, orderType=_model.ProtoOAOrderType.MARKET,
                orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED,
                tradeData=_model.ProtoOATradeData(
                    symbolId=1, volume=req.volume, tradeSide=_model.ProtoOATradeSide.SELL)))
            return event

        broker._dispatch_order = dispatch
        send = asyncio.create_task(broker._send_own_close(
            _oa.ProtoOAClosePositionReq(ctidTraderAccountId=999, positionId=222, volume=500),
            coid='close-1', context='test'))
        await sent.wait()
        with pytest.raises(RuntimeError, match='judgment failed'):
            with broker._judgment_span():
                raise RuntimeError('judgment failed')
        release.set()
        await send

    asyncio.run(scenario())

    [record] = broker._own_close_history
    assert record.order_id == 888
    assert record.answered_ms is not None
    assert all(tracked is record for tracked in broker._own_closes)
    assert broker._share_settled(222)


def __test_netting_shrink_gives_a_foreign_deal_no_identity_of_an_unanswered_close__(tmp_path):
    """L1 and L2 hold 10 each; the bot's close of L2 never got its answer and
    the process restarted. Another client's order (888, its own client-order-id)
    closed 5 inside that close's window. The lookup proves the order foreign:
    it is reported, and delivered and retired as an unbound close (oldest
    first), never under the unanswered close's label or settled against L2."""
    now_ms = int(epoch_time() * 1000)
    history = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=903, order_id=888, position_id=222, closed_volume=500),
               now_ms - 300_000))
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=1500)]))
    broker._wire = _VenueWire(deal_res=history, orders=[_closing_order(888)])
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    for coid, pine_id, order_id in (('c1', 'L1', '111'), ('c2', 'L2', '112')):
        broker.store_ctx.upsert_order(
            coid, symbol='EURUSD', side='buy', qty=10.0, filled_qty=10.0,
            state='confirmed', pine_entry_id=pine_id, exchange_order_id='222',
            extras={'order_id': order_id, 'position_id': 222})
        broker.store_ctx.add_ref(coid, 'order_id', order_id)
    broker._close_dispatch_pine_by_position[222] = '__pyne_partial__L2'
    broker._close_settle_entry_by_position[222] = 'L2'
    record = broker._open_own_close(222, 5.0)
    record.sent_ms = now_ms - 310_000
    broker._journal_own_close(record)
    process = _next_process(broker)

    events = [event for _ in range(2) for event in _run(process)]
    process._flush_external_interventions()

    assert [r.orderId for r in _details_requests(process)] == [888]
    assert len(reports) == 1 and reports[0][1]['order_id'] == 888
    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('903', None, pytest.approx(5.0))]
    assert _retired(process, 'c1') == pytest.approx(5.0)
    assert _retired(process, 'c2') == pytest.approx(0.0)


def __test_recovered_venue_close_inside_an_unanswered_close_window_keeps_its_origin__(tmp_path):
    """A stop-out of the position inside the window of the earlier process's
    unanswered close is the venue's, never taken as that close's."""
    first, _ = _recovered_close_broker(
        tmp_path, _closing_order(999, stop_out=True, coid=''),
        executed_ms=int(epoch_time() * 1000))
    first._open_own_close(222)

    restarted, reports = _restarted(
        tmp_path, first, _closing_order(999, stop_out=True, coid=''))
    _recover(restarted)

    assert 'c3' not in _live_coids(restarted)
    assert reports == []
    assert [r.orderId for r in _details_requests(restarted)] == [999]
    assert restarted._judged_close_origins[900] is CloseOrigin.VENUE


def __test_recovered_foreign_close_inside_an_unanswered_close_window_is_reported__(tmp_path):
    """Another client's order (its own client-order-id) closed the position
    inside the window of the earlier process's unanswered close: the close the
    run sent carries no client-order-id, so the order is not the run's."""
    first, _ = _recovered_close_broker(
        tmp_path, _closing_order(999), executed_ms=int(epoch_time() * 1000))
    first._open_own_close(222)

    restarted, reports = _restarted(tmp_path, first, _closing_order(999))
    _recover(restarted)

    assert 'c3' not in _live_coids(restarted)
    assert [r.orderId for r in _details_requests(restarted)] == [999]
    assert len(reports) == 1 and reports[0][1]['order_id'] == 999
    assert restarted._judged_close_origins[900] is CloseOrigin.EXTERNAL


def __test_netting_shrink_a_restart_found_retires_what_the_deals_took__(tmp_path):
    """The same, found by a restart: the startup baseline retires only the
    shortfall (5); judging the deals retires the rest of the share they took."""
    broker, reports = _masked_own_close_broker(tmp_path)
    broker._apply_adoption_baseline(broker._recon)
    assert _startup_volumes(broker) == {222: pytest.approx(5.0)}

    _run(broker)
    broker._flush_external_interventions()

    assert reports == []
    assert broker._startup_shrinks == {}
    assert broker._own_share(222) == pytest.approx(0.0)
    assert broker._translate_exec_event(_foreign_close_push(
        order_id=70900000, closed_cents=500, remaining_cents=0)) is None
    broker._flush_external_interventions()
    assert reports == []


def _startup_volumes(broker) -> dict:
    return {pid: pending.volume for pid, pending in broker._startup_shrinks.items()}


def _next_process(broker) -> _ReconcileBroker:
    """A new process of the same run: same journal, venue and quarantine sink."""
    process = _ReconcileBroker(broker._recon)
    process._wire = broker._wire
    process.store_ctx = broker.store_ctx
    process.quarantine_sink = broker.quarantine_sink
    return process


def __test_shrink_a_restart_found_is_judged_by_the_next_process_if_it_stopped_first__(
        tmp_path):
    """The process that found and retired the shrink stopped before judging it.
    The next one finds the journal share already at the venue's and still
    judges the pending shrink, once."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker._apply_adoption_baseline(broker._recon)
    process = _next_process(broker)
    process._apply_adoption_baseline(process._recon)
    assert _startup_volumes(process) == {222: pytest.approx(10.0)}

    _run(process)
    process._flush_external_interventions()

    assert len(reports) == 1
    assert reports[0][1]['deal_ids'] == [903]
    assert reports[0][1]['booked_qty'] == pytest.approx(10.0)
    assert _retired(process) == pytest.approx(10.0)
    later = _next_process(process)
    later._apply_adoption_baseline(later._recon)
    assert later._startup_shrinks == {}


def _own_close_of_the_rest(broker) -> None:
    """The bot closes the 10 units the venue still holds (order 888)."""
    broker._settle_own_close(broker._open_own_close(222, 10.0), 888)
    out = broker._translate_exec_event(_foreign_close_push(
        order_id=888, closed_cents=1000, remaining_cents=0))
    assert out is not None and out.fill_qty == pytest.approx(10.0)
    assert 'c3' not in _live_coids(broker)
    broker._wire._deal_res.deal.append(_timed(_close_deal(
        deal_id=889, order_id=888, position_id=222, closed_volume=1000), 1_700_000_004_000))
    broker._recon = _recon()


@pytest.mark.parametrize('restart', [False, True])
def __test_shrink_a_restart_found_is_judged_after_the_bot_closed_the_position__(
        tmp_path, restart):
    """The bot closed the rest of the position before a settled pass judged the
    shrink a restart found: the closed position's deal history judges it."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker._apply_adoption_baseline(broker._recon)
    _own_close_of_the_rest(broker)
    process = broker
    if restart:
        process = _next_process(broker)
        process._apply_adoption_baseline(process._recon)

    _run(process)
    process._flush_external_interventions()

    assert len(reports) == 1
    assert reports[0][1]['deal_ids'] == [903]
    assert reports[0][1]['booked_qty'] == pytest.approx(10.0)
    assert process._startup_shrinks == {}


def __test_pending_shrink_judgment_from_before_an_operator_resume_is_accepted__(tmp_path):
    """A process found the shrink and stopped before judging it; the operator
    then resumed the run: the resume accepted the venue as it stood."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker._apply_adoption_baseline(broker._recon)
    accept_missing_before_resume(broker.store_ctx)
    process = _next_process(broker)
    process.operator_resume_ts_ms = int(epoch_time() * 1000)
    process._apply_adoption_baseline(process._recon)

    for _ in range(3):
        _run(process)
    process._flush_external_interventions()

    assert reports == []
    assert process._startup_shrinks == {}
    assert len(_store_events(process, 'ctrader_close_accepted_by_resume')) == 1


# === HEDGED: a fill taken for an own close of unknown outcome =============

def _claim(order_id, *, fill_qty=10.0, booked=None, share_before=None, share_high=None,
           venue_after=0.0):
    """A closing fill of ``order_id`` taken for the bot's close of unknown outcome."""
    return _ClaimedFill(
        order_id=order_id, deal_id=order_id + 350, fill_qty=fill_qty,
        order_type=int(_model.ProtoOAOrderType.MARKET), client_order_id='',
        from_entry='long', terminal=True, executed_ms=int(epoch_time() * 1000),
        share_before=share_before, share_high=share_high, venue_after=venue_after,
        booked=fill_qty if booked is None else booked)


def _ambiguous_claim_broker(tmp_path, *claims, orders):
    """HEDGED position 222: the bot's close of it has an unknown outcome, and
    ``claims`` were taken for it, each judged the bot's own by the stream;
    ``orders`` describe every closing order the deal history holds."""
    history = _deal_res(*(
        _timed(_close_deal(deal_id=order.orderId + 350, order_id=order.orderId,
                           position_id=222, closed_volume=1000),
               int(epoch_time() * 1000) + index)
        for index, order in enumerate(orders)))
    broker = _ReconcileBroker(_recon())
    broker._hedging_enabled = True
    broker._wire = _VenueWire(deal_res=history, orders=orders)
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    record = broker._open_own_close(222, 10.0)
    broker._settle_own_close(record, None)
    for claim in claims:
        broker._record_claim(record, claim)
        broker._mark_close_judged([claim.deal_id], {claim.deal_id: CloseOrigin.OWN})
    return broker, reports


def __test_hedged_closes_of_other_clients_taken_for_an_own_close_are_reported__(tmp_path):
    """Both orders carry another client's client-order-id: neither is the
    bot's close, and on a position of one owner they took the bot's share."""
    broker, reports = _ambiguous_claim_broker(
        tmp_path, _claim(600), _claim(601), orders=[_closing_order(600), _closing_order(601)])

    _run(broker)
    broker._flush_external_interventions()

    assert len(reports) == 1
    assert reports[0][1]['candidate_order_ids'] == [600, 601]
    assert broker._ambiguous_closes == {}


def __test_hedged_close_of_another_client_taken_for_an_own_close_is_reported__(tmp_path):
    """One close, but it carries another client's client-order-id: it is not
    the bot's close, whatever happened to that one."""
    broker, reports = _ambiguous_claim_broker(
        tmp_path, _claim(600), orders=[_closing_order(600)])

    _run(broker)
    broker._flush_external_interventions()

    assert len(reports) == 1 and reports[0][1]['order_id'] == 600


def __test_hedged_one_close_for_one_own_unknown_close_stays_audited_only__(tmp_path):
    """The one close without a client-order-id may be the bot's own."""
    broker, reports = _ambiguous_claim_broker(
        tmp_path, _claim(600), orders=[_closing_order(600, coid='')])

    _run(broker)
    broker._flush_external_interventions()

    assert reports == []
    assert broker._ambiguous_closes == {}
    assert len(list(broker.store_ctx.iter_events_by_kind_since(
        'ctrader_close_claim_ambiguous', 0))) == 1


@pytest.mark.parametrize('both_taken, reported', [(False, False), (True, True)])
def __test_hedged_closes_without_a_client_order_id_count_only_when_taken_live__(
        tmp_path, both_taken, reported):
    """A closing order without a client-order-id may be a server-initiated
    close; only its fill taken from the stream (no ``isServerEvent``) proves
    it was a client's. Two such closes for one own close of unknown outcome:
    one of them was someone else's."""
    claims = (_claim(600), _claim(601)) if both_taken else (_claim(600),)
    broker, reports = _ambiguous_claim_broker(
        tmp_path, *claims,
        orders=[_closing_order(600, coid=''), _closing_order(601, coid='')])

    _run(broker)
    broker._flush_external_interventions()

    assert len(reports) == (1 if reported else 0)
    assert broker._ambiguous_closes == {}


@pytest.mark.parametrize('foreign_coid', ['', _FOREIGN_COID])
def __test_hedged_close_of_only_merged_foreign_exposure_is_not_reported__(
        tmp_path, foreign_coid):
    """Another client added 5 to the bot's 10-unit leg. While the bot's close
    of 10 has an unknown outcome, the other client closes its own 5
    (15 -> 10), then the bot's close fills (10 -> 0); both fills were taken
    for the bot's close. Whichever order was the bot's close, the other
    client took none of the bot's 10 if the bot's close executed: the
    evidence proves no intervention."""
    broker, reports = _ambiguous_claim_broker(
        tmp_path,
        _claim(600, fill_qty=5.0, booked=5.0, share_before=10.0, share_high=10.0,
               venue_after=10.0),
        _claim(601, fill_qty=10.0, booked=5.0, share_before=5.0, share_high=5.0,
               venue_after=0.0),
        orders=[_closing_order(600, coid=foreign_coid), _closing_order(601, coid='')])

    _run(broker)
    broker._flush_external_interventions()

    assert reports == []
    assert broker._ambiguous_closes == {}


def __test_hedged_close_after_the_own_close_took_the_rest_of_the_share_is_reported__(
        tmp_path):
    """The bot's leg of 10 is its own. Its close of 5 fills (10 -> 5), then
    another client closes the remaining 5 (5 -> 0): whichever order was the
    bot's, someone else took 5 of the bot's share."""
    broker, reports = _ambiguous_claim_broker(
        tmp_path,
        _claim(600, fill_qty=5.0, booked=5.0, share_before=10.0, share_high=10.0,
               venue_after=5.0),
        _claim(601, fill_qty=5.0, booked=0.0, share_before=5.0, share_high=5.0,
               venue_after=0.0),
        orders=[_closing_order(600, coid=''), _closing_order(601, coid='')])

    _run(broker)
    broker._flush_external_interventions()

    assert len(reports) == 1
    assert reports[0][1]['booked_qty'] == pytest.approx(5.0)


# === A recovered close nothing proves someone else's =====================

def _store_events(broker, kind) -> list:
    return list(broker.store_ctx.iter_events_by_kind_since(kind, 0))


def __test_recovered_close_without_a_client_order_id_is_audited_not_reported__(tmp_path):
    """The deal history cannot tell another client's close from a server-initiated
    one (the PUSH's ``isServerEvent`` is not part of the order): ERROR and an
    audit event, the close booked, no report."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(999, coid=''))

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert 900 in broker._seen_deal_ids
    assert reports == []
    audits = _store_events(broker, 'ctrader_close_unattributed')
    assert len(audits) == 1 and audits[0]['deal_ids'] == [900]


def __test_recovered_netting_unattributed_close_below_the_share_is_audited__(tmp_path):
    broker, reports = _netting_recovered_broker(
        tmp_path, _netting_history(foreign_closed=2000, sl_closed=1000))
    broker._wire.orders[600] = _closing_order(600, coid='')
    _seed_position(broker, 'c3', position_id=222, qty=20.0,
                   extras={'missing_pending_since': 0.0})
    broker.store_ctx.add_ref('c3', 'order_id', '111')

    _recover(broker)

    assert reports == []
    audits = _store_events(broker, 'ctrader_close_unattributed')
    assert len(audits) == 1 and audits[0]['booked_qty'] == pytest.approx(10.0)


# === Closes before an operator resume are accepted =======================

def __test_recovered_close_before_an_operator_resume_is_accepted__(tmp_path):
    """The operator closed the position, then resumed: the resume accepted the
    venue as it stands, so the close is never reported."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(999))
    accept_missing_before_resume(broker.store_ctx)

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []
    assert len(_store_events(broker, 'ctrader_close_accepted_by_resume')) == 1


def __test_shrink_a_restart_found_after_an_operator_resume_is_accepted__(tmp_path):
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    accept_missing_before_resume(broker.store_ctx)
    broker._apply_adoption_baseline(broker._recon)
    assert _retired(broker) == pytest.approx(10.0)

    for _ in range(3):
        _run(broker)
    broker._flush_external_interventions()

    assert reports == []
    assert broker._startup_shrinks == {}
    assert _retired(broker) == pytest.approx(10.0)
    assert len(_store_events(broker, 'ctrader_close_accepted_by_resume')) == 1


def __test_shrink_of_a_row_whose_resume_accepted_absence_ended_is_judged__(tmp_path):
    """The resume accepted the absence stamped on the row, but the row came
    back and its stamp was cleared: that acceptance is gone, so a shrink a
    later restart finds is judged as usual."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    row = broker.store_ctx.get_order('c3')
    broker.store_ctx.upsert_order('c3', extras={
        **(row.extras or {}), MISSING_ACCEPTED_BY_RESUME_EXTRA: 1_700_000_000.0})
    broker._apply_adoption_baseline(broker._recon)
    assert _startup_volumes(broker) == {222: pytest.approx(10.0)}

    _run(broker)
    broker._flush_external_interventions()

    assert len(reports) == 1 and reports[0][1]['booked_qty'] == pytest.approx(10.0)
    assert _store_events(broker, 'ctrader_close_accepted_by_resume') == []


def __test_pyramid_shrink_after_an_operator_resume_is_accepted__(tmp_path, monkeypatch):
    """Two entry rows share the shrunk position, so the startup baseline leaves
    it alone; the first pass, ten minutes ago, still accepts the operator's
    close."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker.store_ctx.upsert_order('c3', qty=10.0, filled_qty=10.0)
    _seed_position(broker, 'c4', position_id=222, qty=10.0)
    accept_missing_before_resume(broker.store_ctx)
    broker._apply_adoption_baseline(broker._recon)
    assert _retired(broker) == 0.0

    accepted_at = epoch_time() - 600.0
    monkeypatch.setattr(ctrader_reconcile, 'epoch_time', lambda: accepted_at)
    for _ in range(3):
        _run(broker)
    monkeypatch.undo()
    broker._flush_external_interventions()

    assert reports == []
    assert _retired(broker) + _retired(broker, 'c4') == pytest.approx(10.0)
    assert len(_store_events(broker, 'ctrader_close_accepted_by_resume')) == 1

    # A shrink after that first pass is judged as usual.
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])
    broker._wire._deal_res.deal.append(_timed(_close_deal(
        deal_id=905, order_id=600, position_id=222, closed_volume=500),
        int(epoch_time() * 1000) - 120_000))
    for _ in range(2):
        _run(broker)
    broker._flush_external_interventions()
    assert len(reports) == 1


# === Recovered closes: unknown share, HEDGED share, fired protection ======

@pytest.mark.parametrize('hedged', [False, True])
def __test_recovered_close_of_a_position_whose_bot_share_is_not_known__(tmp_path, hedged):
    """No entry row of the run links the position. NETTING: nothing proves
    another client's close took the bot's part, so ERROR + audit; a HEDGED
    position has one owner, so it is reported."""
    broker, reports = _recovered_close_broker(tmp_path, _closing_order(999), seed=False)
    broker._hedging_enabled = hedged
    deals = [_PositionDeal(deal_id=900, order_id=999, executed_ms=1_700_000_100_000,
                           filled=2000, closed=2000, side='sell', price=1.1, fee=0.0)]

    taken = asyncio.run(broker._judge_recovered_closes(222, deals, from_entry='long'))
    broker._flush_external_interventions()

    assert taken == pytest.approx(20.0)
    audits = _store_events(broker, 'ctrader_close_share_unknown')
    if hedged:
        assert len(reports) == 1 and audits == []
    else:
        assert reports == []
        assert len(audits) == 1 and audits[0]['recovered'] is True


def __test_recovered_hedged_close_of_only_another_client_s_add_is_not_reported__(tmp_path):
    """Another client's order added 10 to the bot's 20-unit leg and closed
    exactly that 10 while the stream was down: the bot's share was intact."""
    broker, reports = _netting_recovered_broker(
        tmp_path, _netting_history(foreign_closed=1000, sl_closed=2000))
    broker._hedging_enabled = True
    _seed_position(broker, 'c3', position_id=222, qty=20.0,
                   extras={'missing_pending_since': 0.0})
    broker.store_ctx.add_ref('c3', 'order_id', '111')

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []


_RULES = _SymbolRules(symbol_id=1, digits=5, min_volume=1000, step_volume=1000,
                      max_volume=10_000_000)


def _sltp_closing_order(order_id, *, stop=None, limit=None):
    """Position 222's own SL / TP order that closed it, as the order details show it."""
    order = _closing_order(order_id, order_type=_SLTP, coid='')
    if stop is not None:
        order.stopPrice = stop
    if limit is not None:
        order.limitPrice = limit
    return order


def _own_protection(broker, levels) -> None:
    """The bot's confirmed protection of position 222, persisted on row ``c3``."""
    guard = broker._protection_guard()
    key = protection_key(222)
    write = guard.expect(key, levels, row_coid='c3')
    guard.settle(key, levels, write=write)


@pytest.mark.parametrize('stop, limit, reported', [
    (1.05, None, False), (None, 1.2, False), (1.2, 1.05, False),
    (1.0, None, True), (1.05, 1.25, True),
])
def __test_recovered_protection_close_is_judged_against_the_bot_s_levels__(
        tmp_path, stop, limit, reported):
    """The position's SL / TP fired while the stream was down: venue mechanics,
    unless it fired at a price the bot never set (either order field)."""
    broker, reports = _recovered_close_broker(
        tmp_path, _sltp_closing_order(999, stop=stop, limit=limit))
    broker._symbol_rules = {'EURUSD': _RULES}
    _own_protection(broker, ProtectionLevels(stop=1.05, limit=1.2))

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert len(reports) == (1 if reported else 0)
    if reported:
        context = reports[0][1]
        assert context['recovered'] is True and context['fired_order_id'] == 999
        assert context['booked_qty'] == pytest.approx(20.0)


def __test_recovered_protection_close_without_levels_after_a_foreign_view_is_audited__(
        tmp_path):
    broker, reports = _recovered_close_broker(tmp_path, _sltp_closing_order(999))
    _own_protection(broker, ProtectionLevels(stop=1.05, limit=1.2))
    broker._last_venue_protection[protection_key(222)] = ProtectionLevels(stop=1.0, limit=1.2)

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []
    assert len(_store_events(broker, 'ctrader_protection_fired_unconfirmed')) == 1


def __test_recovered_protection_close_of_a_position_the_bot_left_bare_is_reported__(tmp_path):
    """The bot left the position bare; a restarted process finds it closed by
    an SL the bot never set."""
    first, _ = _recovered_close_broker(tmp_path, _sltp_closing_order(999, stop=1.0))
    first._mark_bare(222)
    broker = _ReconcileBroker(_recon())
    broker._wire = first._wire
    broker.store_ctx = first.store_ctx
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert len(reports) == 1 and reports[0][1]['bare'] is True


def _own_failsafe(broker, *, stop) -> None:
    """A native fail-safe write of this process landed on position 222."""
    async def landed(_req, **_kwargs):
        return _oa.ProtoOAExecutionEvent(
            executionType=_model.ProtoOAExecutionType.ORDER_ACCEPTED)

    broker._dispatch_order = landed
    asyncio.run(broker._write_unguarded_protection(
        _oa.ProtoOAAmendPositionSLTPReq(ctidTraderAccountId=999, positionId=222,
                                        stopLoss=stop),
        position_id=222, coid='c3', context="native fail-safe SL"))


@pytest.mark.parametrize('stop, reported', [(1.05, False), (1.0, True)])
def __test_recovered_native_failsafe_close_is_judged_against_its_levels__(
        tmp_path, stop, reported):
    """The native fail-safe stop of the position fired while the stream was
    down: the bot's own, unless it fired at a level the bot never set."""
    broker, reports = _recovered_close_broker(tmp_path, _sltp_closing_order(999, stop=stop))
    broker._symbol_rules = {'EURUSD': _RULES}
    _own_failsafe(broker, stop=1.05)

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert len(reports) == (1 if reported else 0)
    if reported:
        assert reports[0][1]['fired_stop'] == pytest.approx(1.0)


def __test_recovered_protection_close_without_levels_after_an_own_view_is_the_venue_s__(
        tmp_path):
    broker, reports = _recovered_close_broker(tmp_path, _sltp_closing_order(999))
    _own_protection(broker, ProtectionLevels(stop=1.05, limit=1.2))
    broker._last_venue_protection[protection_key(222)] = ProtectionLevels(stop=1.05, limit=1.2)

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []
    assert _store_events(broker, 'ctrader_protection_fired_unconfirmed') == []


# === Close coverage across a restart ======================================

def __test_history_close_an_expiry_retired_is_not_booked_after_a_restart__(tmp_path):
    """The first shortfall's wait expired and retired it unbound; the process
    then stops. The next process of the run sees a second shortfall, and the
    deal history shows the first shortfall's close only now: it is a close the
    journal retired already (the coverage watermark the earlier process
    journalled), neither delivered nor debited, and the second shortfall keeps
    waiting for its own close."""
    broker, reports = _unexplained_shrink_broker(tmp_path)
    assert _run(broker) == []
    broker._shrink_seen_since[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S
    assert _run(broker) == [] and _run(broker) == []
    assert _retired(broker) == pytest.approx(10.0)
    process = _next_process(broker)
    process._apply_adoption_baseline(process._recon)
    assert process._startup_shrinks == {}
    process._recon = _recon(positions=[_position(position_id=222, volume=500)])
    assert _run(process) == [] and _run(process) == []
    assert 222 in process._unexplained_shrinks
    process._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               1_700_000_001_000),
        _close_deal_903(price=1.2345))

    assert _run(process) == []

    assert _retired(process) == pytest.approx(10.0)
    assert 222 in process._unexplained_shrinks
    assert reports == []


def _waiting_two_entry_shrink_broker(tmp_path):
    """L1 (c1, order 111) and L2 (c2, order 112) hold 10 each on position
    222; the venue shows 15, the bot's own close of 5 (order 600) executed,
    and the deal history shows no close yet: the shortfall's wait runs."""
    history = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000))
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=1500)]))
    broker._wire = _VenueWire(deal_res=history, orders=[_closing_order(600)])
    broker._wire.honour_window = True
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    for coid, pine_id, order_id, submitted_ms in (
            ('c1', 'L1', '111', 1_700_000_001_000), ('c2', 'L2', '112', 1_700_000_002_000)):
        broker.store_ctx.upsert_order(
            coid, symbol='EURUSD', side='buy', qty=10.0, filled_qty=10.0,
            state='confirmed', pine_entry_id=pine_id, exchange_order_id='222',
            extras={'order_id': order_id, 'position_id': 222,
                    'submitted_at_ms': submitted_ms})
        broker.store_ctx.add_ref(coid, 'order_id', order_id)
    broker._settle_own_close(broker._open_own_close(222, 5.0), 600)
    broker._snapshot_open_positions = {222: _position(position_id=222, volume=1500)}
    assert _run(broker) == [] and _run(broker) == []
    assert 222 in broker._unexplained_shrinks
    return broker, reports


def __test_close_a_wait_spanning_a_restart_waits_for_is_delivered_with_its_deal__(tmp_path):
    """The process stops while a shortfall waits for the close that explains
    it, after its passes moved the coverage watermark past that close's
    execution time. The next process takes the wait over (journalled with
    the watermark it began at): the close the deal history shows then is
    delivered under its own deal id and retires its volume once."""
    broker, reports = _waiting_two_entry_shrink_broker(tmp_path)
    process = _next_process(broker)
    process._apply_adoption_baseline(process._recon)
    process._load_close_evidence()
    assert process._startup_shrinks == {}
    assert 222 in process._unexplained_shrinks
    assert process._closes_covered_until[222] > 1_700_000_003_000
    process._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=903, order_id=600, position_id=222, closed_volume=500,
                           price=1.2345), 1_700_000_003_000))

    events = [event for _ in range(2) for event in _run(process)]

    assert [(e.leg_type, e.fill_id, e.fill_qty, e.fill_price) for e in events] == [
        (LegType.CLOSE, '903', pytest.approx(5.0), pytest.approx(1.2345))]
    assert _retired(process, 'c1') + _retired(process, 'c2') == pytest.approx(5.0)
    assert 222 not in process._unexplained_shrinks
    assert reports == []


def __test_vanished_position_does_not_book_a_close_its_expiry_retired_after_a_restart__(
        tmp_path):
    """:func:`__test_vanished_position_does_not_book_a_close_its_expiry_retired__`
    with a restart between the expiry and the rows' judgment: the next process
    takes the expiry overlap over from the journal, so the close the expiry
    retired is still never delivered."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    process = _next_process(broker)
    process._wire.orders[602] = broker._wire.orders[602]
    process._close_dispatch_pine_by_position[222] = '__pyne_partial__L2'
    process._close_settle_entry_by_position[222] = 'L2'
    process._apply_adoption_baseline(process._recon)
    process._load_close_evidence()
    assert 222 in process._expiry_overlaps
    shift = 120_000
    for record in process._own_close_history:
        record.sent_ms -= shift
        if record.answered_ms is not None:
            record.answered_ms -= shift
    start, end = _shift_expiry(process, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    last_ms = end - shift + 1_000
    process._wire.orders[603] = _closing_order(603)
    process._settle_own_close(process._open_own_close(222, 5.0), 603)
    process._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms),
        _timed(_close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000),
               close_ms),
        _timed(_close_deal(deal_id=909, order_id=603, position_id=222, closed_volume=500),
               last_ms))
    for coid in ('c1', 'c2'):
        row = process.store_ctx.get_order(coid)
        process.store_ctx.upsert_order(
            coid, extras={**(row.extras or {}), 'missing_pending_since': 0.0})
    assert process._own_share(222) == pytest.approx(15.0)

    events, err = _drive_tracker(process)

    assert err is None
    assert sorted((e.fill_id, e.from_entry, e.fill_qty)
                  for e in events if e.fill_id is not None) == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0)),
        ('909', '__pyne_partial__L2', pytest.approx(5.0))]
    assert _live_coids(process) == []
    assert reports == []


@pytest.mark.parametrize('evidence_first', [True, False])
def __test_wait_the_startup_baseline_retired_is_not_taken_over_after_a_restart__(
        tmp_path, evidence_first):
    """The process stops while the shortfall of its one-row position waits;
    the next process's startup baseline retires that shortfall and judges it
    as a startup shrink. The journalled wait ends, whether the startup
    recovery took the earlier processes' close evidence over before the
    baseline or after it: the close's push is a late copy of what the baseline
    retired, never booked again."""
    broker, reports = _unexplained_shrink_broker(tmp_path)
    assert _run(broker) == [] and _run(broker) == []
    assert 222 in broker._unexplained_shrinks
    process = _next_process(broker)
    if evidence_first:
        process._load_close_evidence()
        assert 222 in process._unexplained_shrinks
    process._apply_adoption_baseline(process._recon)
    process._load_close_evidence()

    assert _startup_volumes(process) == {222: pytest.approx(10.0)}
    assert 222 not in process._unexplained_shrinks
    assert process._translate_exec_event(_own_close_903(price=1.2345)) is None
    assert _retired(process) == pytest.approx(10.0)
    assert reports == []


@pytest.mark.parametrize('dies_in_settlement', [False, True])
def __test_confirmed_close_missing_from_history_after_a_restart_lets_no_retired_deal_book__(
        tmp_path, monkeypatch, dies_in_settlement):
    """:func:`__test_confirmed_close_missing_from_history_does_not_let_a_retired_deal_book__`
    with a restart after the answer of the close of L2 confirmed its 10-unit
    fill (deal 907), also when the process died right after journalling the
    answer: the next process takes the close's confirmed execution over from
    the journal, so the 5-unit close the expiry retired (order 600) is still
    never booked in its place, and 907 is booked whole once it shows."""
    broker, reports = _expired_two_entry_shrink_broker(tmp_path)
    record = next(r for r in broker._own_closes if r.order_id == 602)
    if dies_in_settlement:
        def dies(*_args, **_kwargs):
            raise RuntimeError("process died")

        monkeypatch.setattr(broker, '_rejudge_claims', dies)
        with pytest.raises(RuntimeError):
            broker._settle_own_close(record, 602, answer_fill=_AnswerFill(
                deal_id=907, executed_ms=None, closed=10.0), complete=True)
    else:
        broker._settle_own_close(record, 602, answer_fill=_AnswerFill(
            deal_id=907, executed_ms=None, closed=10.0), complete=True)
    process = _next_process(broker)
    process._close_dispatch_pine_by_position[222] = '__pyne_partial__L2'
    process._close_settle_entry_by_position[222] = 'L2'
    process._apply_adoption_baseline(process._recon)
    process._load_close_evidence()
    shift = 120_000
    for restored in process._own_close_history:
        restored.sent_ms -= shift
        if restored.answered_ms is not None:
            restored.answered_ms -= shift
        if restored.order_id == 602:
            restored.sent_ms += 1_000
    start, end = _shift_expiry(process, shift)
    retired_ms = start - shift + 1_000
    close_ms = end - shift - ctrader_reconcile._CLOCK_SKEW_MS + 1_000
    entries = (
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
               1_700_000_001_000),
        _timed(_deal(deal_id=902, order_id=112, position_id=222, filled_volume=1000),
               1_700_000_002_000),
        _timed(_close_deal(deal_id=908, order_id=600, position_id=222, closed_volume=500),
               retired_ms))
    process._wire._deal_res = _deal_res(*entries)
    process._recon = _recon(positions=[_position(position_id=222, volume=500)])

    assert [event for _ in range(3) for event in _run(process)] == []
    assert _retired(process, 'c1') == pytest.approx(5.0)
    assert _retired(process, 'c2') == pytest.approx(0.0)

    process._wire._deal_res = _deal_res(*entries, _timed(
        _close_deal(deal_id=907, order_id=602, position_id=222, closed_volume=1000), close_ms))
    events = [event for _ in range(2) for event in _run(process)]

    assert [(e.fill_id, e.from_entry, e.fill_qty) for e in events] == [
        ('907', '__pyne_partial__L2', pytest.approx(10.0))]
    assert _retired(process, 'c1') == pytest.approx(5.0)
    assert _retired(process, 'c2') == pytest.approx(10.0)
    assert reports == []


# === Closes retired without a judgment stay retired =======================

def _later_foreign_close(process) -> None:
    """Someone closes 5 more of position 222 (order 601), two minutes ago."""
    process._wire.orders[601] = _closing_order(601)
    process._wire._deal_res.deal.append(_timed(_close_deal(
        deal_id=905, order_id=601, position_id=222, closed_volume=500),
        int(epoch_time() * 1000) - 120_000))
    process._recon = _recon(positions=[_position(position_id=222, volume=500)])


@pytest.mark.parametrize('restart', [False, True])
def __test_close_a_startup_resume_accepted_is_neither_booked_nor_reported_later__(
        tmp_path, restart):
    """The startup baseline accepted the operator's close of 10 (deal 903)
    after a resume, from a snapshot read ten minutes ago. Someone closes 5
    more later: only that close is reported and retired; the accepted close
    is neither reported nor taken from the share again."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    accept_missing_before_resume(broker.store_ctx)
    broker._snapshot_read_started_at = epoch_time() - 600.0
    broker._apply_adoption_baseline(broker._recon)
    assert _retired(broker) == pytest.approx(10.0)
    process = broker
    if restart:
        process = _next_process(broker)
        process._apply_adoption_baseline(process._recon)
    assert _run(process) == [] and _run(process) == []
    _later_foreign_close(process)

    for _ in range(3):
        _run(process)
    process._flush_external_interventions()

    assert _retired(process) == pytest.approx(15.0)
    assert [(r[1]['deal_ids'], r[1]['booked_qty']) for r in reports] == [
        ([905], pytest.approx(5.0))]


def __test_pending_shrink_a_later_resume_accepted_covers_its_closes__(tmp_path, monkeypatch):
    """A process found the shrink ten minutes ago and stopped before judging
    it; the operator resumed the run, and the next process accepted it.
    Someone closes 5 more later: only that close is reported and retired."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    found_at = epoch_time() - 600.0
    monkeypatch.setattr(ctrader_reconcile, 'epoch_time', lambda: found_at)
    broker._apply_adoption_baseline(broker._recon)
    monkeypatch.undo()
    accept_missing_before_resume(broker.store_ctx)
    process = _next_process(broker)
    process.operator_resume_ts_ms = int(epoch_time() * 1000)
    process._apply_adoption_baseline(process._recon)
    assert _run(process) == [] and _run(process) == []
    assert len(_store_events(process, 'ctrader_close_accepted_by_resume')) == 1
    _later_foreign_close(process)

    for _ in range(3):
        _run(process)
    process._flush_external_interventions()

    assert _retired(process) == pytest.approx(15.0)
    assert [(r[1]['deal_ids'], r[1]['booked_qty']) for r in reports] == [
        ([905], pytest.approx(5.0))]


def __test_startup_shrink_judgment_does_not_take_a_close_an_expiry_retired__(tmp_path):
    """The first shortfall's wait expired and retired it unbound; the process
    then stops. The next process's startup baseline retires a second
    shortfall of 5, and the deal history shows the first shortfall's close
    only now: the startup shrink's judgment does not take it from the share
    again."""
    broker, reports = _unexplained_shrink_broker(tmp_path)
    assert _run(broker) == []
    broker._shrink_seen_since[222] -= ctrader_reconcile._UNEXPLAINED_SHRINK_WAIT_S
    assert _run(broker) == [] and _run(broker) == []
    assert _retired(broker) == pytest.approx(10.0)
    process = _next_process(broker)
    process._recon = _recon(positions=[_position(position_id=222, volume=500)])
    process._apply_adoption_baseline(process._recon)
    assert _retired(process) == pytest.approx(15.0)
    process._wire._deal_res = _deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               1_700_000_001_000),
        _close_deal_903(price=1.2345))

    events = [event for _ in range(3) for event in _run(process)]

    assert events == []
    assert process._startup_shrinks == {}
    assert _retired(process) == pytest.approx(15.0)
    assert reports == []


def __test_startup_baseline_retires_a_shrink_only_with_its_pending_record__(
        tmp_path, monkeypatch):
    """The process dies while recording the shrink the startup baseline
    found: the retirement is not journalled without it, so the next process
    finds and judges the shrink again."""
    broker, _reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))

    def dies(*_args, **_kwargs):
        raise RuntimeError("process died")

    monkeypatch.setattr(broker, '_record_startup_shrink', dies)
    with pytest.raises(RuntimeError):
        broker._apply_adoption_baseline(broker._recon)

    assert _retired(broker) == 0.0
    process = _next_process(broker)
    process._apply_adoption_baseline(process._recon)
    assert _startup_volumes(process) == {222: pytest.approx(10.0)}


def __test_resume_acceptance_retires_a_shrink_only_with_its_coverage__(
        tmp_path, monkeypatch):
    """Two entries share the shrunk position, so the first settled pass
    accepts the operator's close; the process dies while journalling the
    closes' coverage: the retirement is not journalled without it."""
    broker, _reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker.store_ctx.upsert_order('c3', qty=10.0, filled_qty=10.0)
    _seed_position(broker, 'c4', position_id=222, qty=10.0)
    accept_missing_before_resume(broker.store_ctx)
    broker._apply_adoption_baseline(broker._recon)

    def dies(*_args, **_kwargs):
        raise RuntimeError("process died")

    monkeypatch.setattr(broker, '_journal_close_coverage', dies)
    with pytest.raises(RuntimeError):
        for _ in range(3):
            _run(broker)

    assert _retired(broker) + _retired(broker, 'c4') == 0.0


def __test_resume_acceptance_whose_debit_failed_is_applied_by_the_next_pass__(
        tmp_path, monkeypatch):
    """Two entries share the shrunk position; the first settled pass accepts
    the operator's close, but its debit fails after the closes' coverage was
    written. The pass rolls back whole, in the journal and in this process:
    the next settled pass of the same process applies the acceptance, its
    coverage and the retirement together, with no shortfall wait."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker.store_ctx.upsert_order('c3', qty=10.0, filled_qty=10.0)
    _seed_position(broker, 'c4', position_id=222, qty=10.0)
    accept_missing_before_resume(broker.store_ctx)
    broker._apply_adoption_baseline(broker._recon)

    def debit_fails(*_args, **_kwargs):
        raise RuntimeError('debit failed')

    monkeypatch.setattr(broker, '_retire_partial_close_exposure', debit_fails)
    with pytest.raises(RuntimeError, match='debit failed'):
        for _ in range(3):
            _run(broker)
    monkeypatch.undo()

    assert 222 in broker._resume_accepted_positions
    assert 222 not in broker._closes_covered_until
    assert 222 not in broker._resume_accepted_until
    assert _store_events(broker, 'ctrader_close_accepted_by_resume') == []

    for _ in range(3):
        _run(broker)
    broker._flush_external_interventions()

    assert reports == []
    assert _retired(broker) + _retired(broker, 'c4') == pytest.approx(10.0)
    assert len(_store_events(broker, 'ctrader_close_accepted_by_resume')) == 1
    assert 222 not in broker._unexplained_shrinks
    assert broker._resume_accepted_until[222] == broker._closes_covered_until[222]


def __test_startup_baseline_whose_write_failed_leaves_no_coverage_in_memory__(
        tmp_path, monkeypatch):
    """The startup baseline retires the shrink an operator resume accepted,
    but its audit write fails: the retirement and the closes' coverage roll
    back in the journal, and this process holds no coverage the journal does
    not."""
    broker, _reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    accept_missing_before_resume(broker.store_ctx)

    def audit_fails(*_args, **_kwargs):
        raise RuntimeError('audit failed')

    monkeypatch.setattr(broker, '_note_accepted_by_resume', audit_fails)
    with pytest.raises(RuntimeError, match='audit failed'):
        broker._apply_adoption_baseline(broker._recon)

    assert _retired(broker) == 0.0
    assert 222 not in broker._closes_covered_until
    assert 222 not in broker._resume_accepted_until
    assert broker._coverage_journalled == {}


def __test_close_evidence_merged_inside_a_rolled_back_judgment_stays_merged__(tmp_path):
    """An earlier process journalled its close of position 222 as order 602.
    The next process first merges that evidence inside a judgment that rolls
    back: the merge is kept, so the close still counts as the run's own."""
    broker = _ReconcileBroker(_recon())
    _open(tmp_path, broker)
    record = broker._open_own_close(222, 10.0)
    broker._settle_own_close(record, 602)
    process = _next_process(broker)

    with pytest.raises(RuntimeError, match='judgment failed'):
        with process._judgment_span():
            process._load_close_evidence()
            raise RuntimeError('judgment failed')

    process._load_close_evidence()
    assert [r.order_id for r in process._own_close_history] == [602]


def __test_shared_position_audit_of_a_rolled_back_transfer_is_written_by_its_retry__(
        tmp_path):
    """A second entry linked to position 222 inside a transaction that rolls
    back (an adopted-leg takeover that failed): the audit of the shared
    position is rolled back with it, and the retry writes it once."""
    broker = _ReconcileBroker(_recon())
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=10.0)
    broker.store_ctx.add_ref('c3', 'position_id', '222')
    _seed_position(broker, 'c4', position_id=222, qty=10.0)

    with pytest.raises(RuntimeError, match='transfer failed'):
        with broker._judgment_span():
            broker._link_position_ref('c4', 222)
            raise RuntimeError('transfer failed')
    assert _store_events(broker, 'ctrader_position_id_shared') == []

    broker._link_position_ref('c4', 222)
    assert len(_store_events(broker, 'ctrader_position_id_shared')) == 1


def __test_own_fill_taken_before_the_answer_is_journalled_with_it__(tmp_path):
    """A fill of the close (order 602) arrived before the answer named that
    order: the answer journals it as executed, so a restarted process knows
    the 4 units the close executed."""
    broker = _ReconcileBroker(_recon())
    _open(tmp_path, broker)
    record = broker._open_own_close(222, 10.0)
    broker._record_claim(record, _claim(602, fill_qty=4.0))

    broker._settle_own_close(record, 602)

    assert record.executed == pytest.approx(4.0)
    assert record.complete, "the claimed fill was the order's final one"
    process = _next_process(broker)
    process._load_close_evidence()
    restored = next(r for r in process._own_close_history if r.order_id == 602)
    assert restored.executed == pytest.approx(4.0)
    assert restored.executed_deals == {952}
    assert restored.complete


def __test_close_coverage_journal_keeps_one_record_per_position__(tmp_path):
    # Every rise of a position's watermark is journalled, yet the run keeps
    # only the latest record of each position: the journal and its read at a
    # restart grow with the positions, never with the passes of the run.
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    _open(tmp_path, broker)
    for step in range(50):
        broker._cover_closes(222, 1_700_000_100.0 + step)
        broker._cover_closes(333, 1_700_000_200.0 + step, accepted=True)

    records = list(broker.store_ctx.iter_events_by_kind_for_run_id(
        ctrader_reconcile._CLOSE_COVERAGE_EVENT))
    assert len(records) == 2

    restarted = _restarted_process(tmp_path, broker, _recon(), _deal_res())
    restarted._load_close_evidence()
    assert restarted._closes_covered_until[222] == (
        int((1_700_000_100.0 + 49) * 1000) - ctrader_reconcile._CLOCK_SKEW_MS)
    assert restarted._closes_covered_until[333] == (
        int((1_700_000_200.0 + 49) * 1000) - ctrader_reconcile._CLOCK_SKEW_MS)
    assert restarted._resume_accepted_until[333] == restarted._closes_covered_until[333]
    assert 222 not in restarted._resume_accepted_until


@pytest.mark.parametrize('closed', ['partial', 'full'])
def __test_close_push_whose_judgment_rolled_back_is_booked_by_the_next_pass__(tmp_path, closed):
    """The push of the bot's own close of position 222 (deal 903) is judged in
    a transaction that rolls back, and the stream drops the message. The deal
    is not taken for one the run booked: the next passes judge it from the
    deal history and deliver it once, with its debit in the journal."""
    venue = 1000 if closed == 'partial' else 0
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker._recon = _recon(positions=[_position(position_id=222, volume=venue)] if venue
                           else [])
    broker._wire = _VenueWire(deal_res=_deal_res(
        _timed(_deal(deal_id=901, order_id=111, position_id=222, filled_volume=2000),
               1_700_000_001_000),
        _close_deal_903(price=1.2345)), orders=[_closing_order(600)])
    broker._settle_own_close(broker._open_own_close(222, 10.0), 600)
    broker._snapshot_open_positions = {222: _position(position_id=222, volume=2000)}
    push = _own_close_903(price=1.2345)
    if not venue:
        push.position.positionStatus = _model.ProtoOAPositionStatus.POSITION_STATUS_CLOSED
        push.position.tradeData.volume = 0

    def write_fails(*_args, **_kwargs):
        raise RuntimeError('journal write failed')

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(broker, '_journal_fill_delivery', write_fails)
        with pytest.raises(RuntimeError, match='journal write failed'):
            broker._translate_exec_event(push)

    assert 903 not in broker._seen_deal_ids
    events = [event for _ in range(3) for event in _run(broker)]
    if not venue:
        row = broker.store_ctx.get_order('c3')
        broker.store_ctx.upsert_order(
            'c3', extras={**(row.extras or {}), 'missing_pending_since': 0.0})
        events += _drive_tracker(broker)[0]

    assert [(e.leg_type, e.fill_id, e.fill_qty) for e in events
            if e.leg_type is LegType.CLOSE] == [(LegType.CLOSE, '903', pytest.approx(10.0))]
    assert reports == []


def __test_filled_then_closed_retirement_that_failed_is_delivered_by_the_next_pass__(tmp_path):
    """The pass that retires working order 111 as filled (deal 905) and closed
    (deal 906) fails at its journal write. Its deals are not taken as seen:
    the next pass delivers the whole trade once and retires the row."""
    broker = _filled_then_closed_broker(tmp_path)

    def write_fails(*_args, **_kwargs):
        raise RuntimeError('journal write failed')

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(broker, '_journal_fill_delivery', write_fails)
        with pytest.raises(RuntimeError, match='journal write failed'):
            _run(broker)

    assert not {905, 906} & broker._seen_deal_ids
    assert 'c14' in _live_coids(broker)
    events = _run(broker)

    assert [(e.leg_type, e.fill_id) for e in events if e.leg_type is LegType.CLOSE] == [
        (LegType.CLOSE, '906')]
    assert [e.leg_type for e in events] == [LegType.ENTRY, LegType.CLOSE]
    assert 'c14' not in _live_coids(broker)
    assert _run(broker) == []


def __test_helper_vanished_close_judged_before_its_verdict(tmp_path):
    """Position 222 of row c3 vanished; its close deal 900 was judged, and the
    process died before the tracker applied the CLOSED verdict: the journal
    still owns the row's 20 units. Returns the next process of the run."""
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]),
        deal_res=_deal_res(_close_deal(
            deal_id=900, order_id=111, position_id=222, closed_volume=2000)))
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})
    __test_helper_engine(broker).reconcile()
    broker._recon = _recon()

    def stop_before_apply(*_args, **_kwargs):
        raise RuntimeError('process stopped')

    broker._disappearance_tracker()._apply_confirmation = stop_before_apply
    with pytest.raises(RuntimeError, match='process stopped'):
        _drive_tracker(broker)
    assert _live_coids(broker) == ['c3']
    return _next_process(broker)


@pytest.mark.parametrize('history', ['readable', 'unreadable'])
def __test_vanished_close_judged_before_its_verdict_is_no_startup_alarm__(
        tmp_path, monkeypatch, caplog, history):
    """The restart finds the row the earlier judgment's close explains: the
    startup recovery retires it from that judgment, whether or not the deal
    history can be read again, so the engine's startup comparison finds the
    journal at the flat venue and reports nothing. The judgment's journal of
    the close goes with the row."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    process = __test_helper_vanished_close_judged_before_its_verdict(tmp_path)
    if history == 'unreadable':
        process._wire = _FakeWire(fail=True)
    restarted = __test_helper_engine(process)

    with caplog.at_level(logging.WARNING):
        asyncio.run(process._recover_in_flight_submissions())
        restarted.reconcile()
        events = [*__test_helper_stream_start(process), *_drive_tracker(process)[0]]

    assert [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR] == []
    assert _live_coids(process) == []
    assert restarted._position.size == pytest.approx(0.0)
    assert events == []
    assert process.store_ctx.keyed_events_for_run_id(ctrader_reconcile._VANISHED_CLOSE_EVENT) == {}


def __test_vanished_position_no_judgment_explains_is_a_startup_alarm__(
        tmp_path, monkeypatch, caplog):
    """The same restart without an earlier judgment of the close, and the deal
    history cannot be read: nothing explains the flat venue, so the row stays
    live. While the plugin still recovers the close (its row is live with the
    position gone) the engine's startup comparison waits; once the recovery
    settles without a fill, it reports the difference."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    broker = _ReconcileBroker(_recon())
    broker._wire = _FakeWire(fail=True)
    _open(tmp_path, broker)
    _seed_position(broker, 'c3', position_id=222, qty=volume_to_units(2000),
                   extras={'missing_pending_since': 0.0})
    restarted = __test_helper_engine(broker)

    with caplog.at_level(logging.WARNING):
        asyncio.run(broker._recover_in_flight_submissions())
        restarted.reconcile()

    assert _live_coids(broker) == ['c3']
    assert broker.fill_recovery_pending({'long'})
    assert not any('no fill explains' in r.getMessage() for r in caplog.records
                   if r.levelno >= logging.ERROR)
    monkeypatch.setattr(broker, 'fill_recovery_pending', lambda entry_ids: False)
    with caplog.at_level(logging.WARNING):
        restarted.reconcile()
    assert any('no fill explains' in r.getMessage() for r in caplog.records
               if r.levelno >= logging.ERROR)


def __test_vanished_close_judged_before_its_verdict_short_of_the_journal_is_a_startup_alarm__(
        tmp_path, monkeypatch, caplog):
    """The earlier judgment's close took 20 units, but the journal owns 30 on
    the position and the deal history cannot be read: the judgment does not
    explain the flat venue, so the row stays live. The engine waits while the
    plugin still recovers the close, and reports once that settles."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    process = __test_helper_vanished_close_judged_before_its_verdict(tmp_path)
    process.store_ctx.upsert_order('c3', qty=30.0, filled_qty=30.0)
    process._wire = _FakeWire(fail=True)
    restarted = __test_helper_engine(process)

    with caplog.at_level(logging.WARNING):
        asyncio.run(process._recover_in_flight_submissions())
        restarted.reconcile()

    assert _live_coids(process) == ['c3']
    pending = process.fill_recovery_pending({'long'})
    reported = any('no fill explains' in r.getMessage() for r in caplog.records
                   if r.levelno >= logging.ERROR)
    assert pending and not reported
    monkeypatch.setattr(process, 'fill_recovery_pending', lambda entry_ids: False)
    with caplog.at_level(logging.WARNING):
        restarted.reconcile()
    assert any('no fill explains' in r.getMessage() for r in caplog.records
               if r.levelno >= logging.ERROR)


@pytest.mark.parametrize('restart', [False, True], ids=['same-process', 'restart'])
@pytest.mark.parametrize('listed', ['at-once', 'late'])
def __test_commission_of_a_snapshot_booked_fill_whose_push_never_came_is_booked_once__(
        tmp_path, monkeypatch, listed, restart):
    """The first deal of entry order 111 (920) was booked from the snapshot
    without its commission, and its push never came; the second deal's push
    filled the order to the end, so its row is a settled position no recovery
    reads again. The passes read the order's deal history until it lists the
    first deal (at once, or only on a later pass), and deliver its commission
    as a fee event, once, in this process or after a restart."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    broker = _ReconcileBroker(_recon(orders=[_order(order_id=111, volume=2000)]),
                              deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_working(broker, 'c11', order_id=111, qty=volume_to_units(2000))
    engine = __test_helper_engine(broker)
    engine.reconcile()
    broker._recon = _recon(orders=[_order(order_id=111, volume=2000, executed=1000,
                                          position_id=222, price=1.0)])
    __test_helper_book(engine, _run(broker))
    first = _deal(deal_id=920, order_id=111, position_id=222, filled_volume=1000, price=1.0)
    first.commission = 300
    second = _deal(deal_id=921, order_id=111, position_id=222, filled_volume=1000, price=2.0)
    second.commission = 500
    second.executionTimestamp = first.executionTimestamp + 60_000
    broker._recon = _recon(positions=[_position(position_id=222, volume=2000)])
    pushed = broker._translate_exec_event(__test_helper_entry_push(**__test_helper_DEAL_B))
    assert pushed is not None and pushed.fee == pytest.approx(5.0)
    __test_helper_book(engine, [pushed])
    if restart:
        broker = _next_process(broker)
        engine = __test_helper_engine(broker)
        engine.reconcile()
    events: list = []
    if listed == 'late':
        broker._wire._deal_res = _deal_res(second)
        events += [event for _ in range(2) for event in _run(broker)]
        assert events == []
    broker._wire._deal_res = _deal_res(first, second)

    events += [event for _ in range(3) for event in _run(broker)]
    __test_helper_book(engine, [*events, *events])

    assert [(e.event_type, e.fill_id, e.fee) for e in events] == [
        (FEE_EVENT_TYPE, 'c11:snapshot:1000', pytest.approx(3.0))]
    # After a restart the adopted trade keeps the second deal's receipted 5.0.
    assert engine._position.open_commission == pytest.approx(8.0)
    row = broker.store_ctx.get_order('c11')
    assert row is not None and not (row.extras or {}).get('fee_unbooked')
    assert broker._settle_deliveries() == []
    assert broker.store_ctx.keyed_events_for_run_id(_FEE_DELIVERY_EVENT) == {}


def __test_reconcile_unpriced_deal_in_history_books_nothing_until_priced__(tmp_path):
    """A complete deal history listing the progress without its price proves
    nothing yet: no fill is booked, the cursor stays and the deal is not
    taken as seen, so the priced push of that deal books it."""
    unpriced = _deal(deal_id=930, order_id=111, position_id=222,
                     filled_volume=1000, price=0.0)
    broker = _ReconcileBroker(
        _recon(orders=[_order(order_id=111, volume=2000, executed=1000,
                              position_id=222, price=0.0)]),
        deal_res=_deal_res(unpriced),
    )
    _open(tmp_path, broker)
    _seed_working(broker, 'c11', order_id=111, qty=volume_to_units(2000))

    assert _run(broker) == []
    row = broker.store_ctx.get_order('c11')
    assert row is not None and row.filled_qty == 0.0
    assert 930 not in broker._seen_deal_ids
    assert 111 not in broker._deal_walks, "the window is read whole again"

    push = __test_helper_entry_push(
        deal_id=930, executed=1000, filled=1000, price=1.0, commission=300)
    event = broker._translate_exec_event(push)
    assert event is not None and event.event_type == 'partial'
    assert event.fill_qty == pytest.approx(volume_to_units(1000))
    assert event.fill_price == 1.0 and event.fill_id == '930'
    assert broker.store_ctx.get_order('c11').filled_qty == pytest.approx(10.0)


def __test_helper_unpriced_opening_history(*, close_price):
    """Order 111 filled (deal 905) onto netting position 222, which an older
    deal of another participant (904, order 777) had opened and the history
    lists without a price; the SL (deal 906, order 999) then closed the whole
    position while the stream was down."""
    opening = _deal(deal_id=904, order_id=777, position_id=222, filled_volume=500, price=0.0)
    opening.executionTimestamp = 1_699_999_000_000
    return _deal_res(
        opening,
        _deal(deal_id=905, order_id=111, position_id=222, filled_volume=2000, price=1.2),
        _close_deal(deal_id=906, order_id=999, position_id=222, closed_volume=2500,
                    price=close_price))


def __test_helper_unpriced_opening_broker(tmp_path, *, close_price):
    """The working row of order 111 against :func:`__test_helper_unpriced_opening_history`."""
    broker = _ReconcileBroker(_recon(positions=[]), deal_res=(
        __test_helper_unpriced_opening_history(close_price=close_price)))
    _open(tmp_path, broker)
    _seed_working(broker, 'c14', order_id=111, qty=volume_to_units(2000))
    return broker


def __test_unpriced_unrelated_deal_does_not_block_the_position_s_recovery__(tmp_path):
    """An unpriced deal that needs no fill event (another participant's
    opening) stays volume evidence: the priced entry and close are delivered."""
    broker = __test_helper_unpriced_opening_broker(tmp_path, close_price=1.15)

    events = _run(broker)

    assert [(e.leg_type, e.event_type, e.fill_qty, e.fill_price, e.fill_id)
            for e in events] == [
        (LegType.ENTRY, 'filled', pytest.approx(20.0), pytest.approx(1.2), '905'),
        (LegType.CLOSE, 'filled', pytest.approx(20.0), pytest.approx(1.15), '906'),
    ]
    assert 'c14' not in _live_coids(broker)


def __test_unpriced_close_of_the_share_is_judged_once_priced__(tmp_path):
    """The close of the bot's share listed without a price is not judged: no
    fill, the row stays live and the walk is dropped; once the history prices
    it, the next pass delivers the trade."""
    broker = __test_helper_unpriced_opening_broker(tmp_path, close_price=0.0)

    assert _run(broker) == []
    assert 'c14' in _live_coids(broker)
    assert 906 not in broker._judged_close_deals
    assert 906 not in broker._seen_deal_ids
    assert 111 not in broker._deal_walks, "the window is read whole again"

    broker._wire._deal_res = __test_helper_unpriced_opening_history(close_price=1.15)
    events = _run(broker)

    assert [(e.leg_type, e.fill_price, e.fill_id) for e in events] == [
        (LegType.ENTRY, pytest.approx(1.2), '905'),
        (LegType.CLOSE, pytest.approx(1.15), '906'),
    ]
    assert 'c14' not in _live_coids(broker)


# === Own fill adopted in the run with an earlier unseen execution ============

def _adopted_gap_broker(tmp_path):
    """The stream delivers only the second deal of order 555 of the run.

    Its first deal (10 units at 1.1) was never delivered: the push of the
    second one (10 units at 1.2) shows 20 executed. The order filled and left
    ``order[]``; its position 54302872 holds the 20.
    """
    first = _deal(deal_id=5551, order_id=555, position_id=54302872, filled_volume=1000,
                  price=1.1)
    second = _deal(deal_id=5552, order_id=555, position_id=54302872, filled_volume=1000,
                   price=1.2)
    second.executionTimestamp = first.executionTimestamp + 1000
    deals = _deal_res(first, second)
    broker = _ReconcileBroker(
        _recon(positions=[_position(position_id=54302872, volume=2000)]), deal_res=deals)
    _open(tmp_path, broker)
    broker._symbol_rules = {'EURUSD': _RULES}
    order = _model.ProtoOAOrder(
        orderId=555, orderType=_model.ProtoOAOrderType.MARKET,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED, executedVolume=2000,
        positionId=54302872, clientOrderId=f"{broker.store_ctx.run_tag}-7rxpdapr-0mv9ub05c-e0",
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=2000,
                                          tradeSide=_model.ProtoOATradeSide.BUY))
    push = _oa.ProtoOAExecutionEvent(executionType=_model.ProtoOAExecutionType.ORDER_FILLED)
    push.order.CopyFrom(order)
    push.deal.CopyFrom(_model.ProtoOADeal(dealId=5552, orderId=555, positionId=54302872,
                                          filledVolume=1000, executionPrice=1.2))
    return broker, push, f"{broker.store_ctx.run_tag}-own-555"


def _fills(events) -> list[tuple[str | None, float | None, float | None]]:
    return [(e.fill_id, e.fill_qty, e.fill_price) for e in events
            if e.event_type in ('filled', 'partial')]


def __test_an_adopted_fill_after_an_unseen_execution_books_both_deals_once__(tmp_path):
    # The push of the second deal is held: booking it would move the cursor
    # past the first deal the engine never booked. The reconcile pass reads
    # the order's deals and books both, in execution order at their own
    # prices; a later pass and a late copy of either push book nothing.
    broker, push, coid = _adopted_gap_broker(tmp_path)

    assert broker._translate_exec_event(push) is None

    row = broker.store_ctx.get_order(coid)
    assert row.filled_qty == 0.0
    assert row.pine_entry_id == ADOPTED_STARTUP_ENTRY_ID
    assert 5552 not in broker._seen_deal_ids

    events = _run(broker)

    assert _fills(events) == [('5551', pytest.approx(10.0), pytest.approx(1.1)),
                              ('5552', pytest.approx(10.0), pytest.approx(1.2))]
    assert all(e.pine_id == ADOPTED_STARTUP_ENTRY_ID for e in events)
    assert broker.store_ctx.get_order(coid).filled_qty == pytest.approx(20.0)
    assert coid not in broker._adoption_reads
    assert _fills(_run(broker)) == []
    assert broker._translate_exec_event(push) is None


def __test_an_adopted_fill_whose_deal_read_fails_is_read_again_then_booked__(tmp_path):
    # The deal history cannot be read: nothing is booked, the cursor stays,
    # and the read waits out its backoff before it is asked again. Once it
    # answers, both deals are booked once.
    broker, push, coid = _adopted_gap_broker(tmp_path)
    assert broker._translate_exec_event(push) is None
    broker._wire._fail = True

    assert _fills(_run(broker)) == []

    assert broker.store_ctx.get_order(coid).filled_qty == 0.0
    next_at, delay = broker._adoption_reads[coid]
    assert delay == pytest.approx(10.0)
    requests = len(broker._wire.requests)
    broker._wire._fail = False
    assert _fills(_run(broker)) == []
    assert len(broker._wire.requests) == requests
    broker._adoption_reads[coid] = (0.0, delay)

    assert _fills(_run(broker)) == [('5551', pytest.approx(10.0), pytest.approx(1.1)),
                                    ('5552', pytest.approx(10.0), pytest.approx(1.2))]
    assert broker.store_ctx.get_order(coid).filled_qty == pytest.approx(20.0)
    assert coid not in broker._adoption_reads


# === Executions handed to the engine's held-execution recovery ==============

def _entry_row(broker, coid) -> None:
    """Give a seeded working row the entry shape the engine's hold applies to."""
    row = broker.store_ctx.get_order(coid)
    broker.store_ctx.upsert_order(coid, intent_key='long',
                                  extras={**(row.extras or {}), 'kind': 'working'})


def __test_a_vanished_order_executed_by_its_details_only_is_handed_over__(tmp_path):
    # The complete deal history lists no fill of the vanished order, but its
    # details show it filled: the history cannot deliver the fill, so the
    # entry goes to the engine's held-execution recovery. No cancel, no policy.
    broker, reports = _vanished_working_broker(
        tmp_path, _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
                                   executed=2000))
    _entry_row(broker, 'w1')

    events, err = _drive_tracker(broker)

    assert events == [] and err is None and reports == []
    assert {coid: request['order_id'] for coid, request
            in broker.store_ctx.execution_hold_requests().items()} == {'w1': '600'}


def __test_a_handed_over_order_s_absence_is_no_disappearance__(tmp_path):
    # The engine holds the order: a complete history without its fill stamps
    # nothing, a stamp left from before is cleared, and a grace-expired check
    # concludes nothing.
    broker = _ReconcileBroker(_recon(positions=[]), deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_working(broker, 'c5', order_id=111, qty=volume_to_units(2000),
                  extras={'missing_pending_since': 0.0})
    broker.store_ctx.held_execution_order_ids = lambda: frozenset({'111'})

    events, err = _drive_tracker(broker)

    assert events == [] and err is None
    assert _run(broker) == []
    assert 'missing_pending_since' not in (broker.store_ctx.get_order('c5').extras or {})
    assert _run(broker) == []
    assert 'missing_pending_since' not in (broker.store_ctx.get_order('c5').extras or {})
    assert 'c5' in _live_coids(broker)


def __test_a_vanished_order_whose_closed_position_s_history_is_short_is_handed_over__(
        tmp_path):
    # The order reported 20 executed earlier; its position closed, and the
    # complete deal history shows only 10 of them: the fills cannot be
    # delivered from it, so the entry goes to the engine's recovery and
    # nothing is booked or judged here.
    broker = _ReconcileBroker(_recon(positions=[]), deal_res=_deal_res(
        _deal(deal_id=901, order_id=111, position_id=222, filled_volume=1000),
        _close_deal(deal_id=902, order_id=999, position_id=222, closed_volume=2000)))
    _open(tmp_path, broker)
    _seed_working(broker, 'c7', order_id=111, qty=volume_to_units(2000),
                  extras={'executed_cents': 2000})
    _entry_row(broker, 'c7')

    assert _run(broker) == []

    assert {coid: request['order_id'] for coid, request
            in broker.store_ctx.execution_hold_requests().items()} == {'c7': '111'}
    assert 'c7' in _live_coids(broker)
    assert 902 not in broker._seen_deal_ids


def __test_a_settled_handed_over_entry_on_an_open_position_keeps_its_row__(tmp_path):
    # The engine booked the handed-over order's execution; its position is
    # still open. The row takes the executed volume and the position, and
    # stays live as that position's row.
    filled = _model.ProtoOAOrder(
        orderId=111, orderType=_model.ProtoOAOrderType.LIMIT,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED, executedVolume=2000,
        positionId=222, tradeData=_model.ProtoOATradeData(
            symbolId=1, volume=2000, tradeSide=_model.ProtoOATradeSide.BUY))
    broker = _ReconcileBroker(_recon(positions=[_position(position_id=222, volume=2000)]))
    broker._wire = _VenueWire(deal_res=_deal_res(), orders=[filled])
    broker._symbol_rules = {'EURUSD': _RULES}
    _open(tmp_path, broker)
    _seed_working(broker, 'c8', order_id=111, qty=volume_to_units(2000),
                  extras={'execution_handed_over': '111'})
    _entry_row(broker, 'c8')

    assert _run(broker) == []

    row = broker.store_ctx.get_order('c8')
    assert row.closed_ts_ms is None
    assert row.filled_qty == pytest.approx(volume_to_units(2000))
    assert (row.extras or {}).get('position_id') == 222
    assert 'execution_handed_over' not in (row.extras or {})
    assert broker.store_ctx.execution_hold_requests() == {}


def __test_a_settled_handed_over_entry_whose_close_the_history_lacks_leaves_it_to_the_engine__(
        tmp_path, caplog):
    # The engine booked the handed-over order's execution; its position is
    # closed, but the deal history shows no close of it. No close fill is
    # invented: the row is retired and the close is left to the engine's own
    # fallback, which the log says.
    filled = _model.ProtoOAOrder(
        orderId=111, orderType=_model.ProtoOAOrderType.LIMIT,
        orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_FILLED, executedVolume=2000,
        positionId=222, tradeData=_model.ProtoOATradeData(
            symbolId=1, volume=2000, tradeSide=_model.ProtoOATradeSide.BUY))
    broker = _ReconcileBroker(_recon(positions=[]))
    broker._wire = _VenueWire(deal_res=_deal_res(), orders=[filled])
    _open(tmp_path, broker)
    _seed_working(broker, 'c9', order_id=111, qty=volume_to_units(2000),
                  extras={'execution_handed_over': '111'})
    _entry_row(broker, 'c9')

    with caplog.at_level(logging.WARNING):
        assert _run(broker) == []

    row = broker.store_ctx.get_order('c9')
    assert row.closed_ts_ms is not None
    assert row.filled_qty == pytest.approx(volume_to_units(2000))
    assert "left to the engine's own fallback" in caplog.text
