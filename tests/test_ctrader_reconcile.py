"""
@pyne
"""
import asyncio
from time import time as epoch_time

from pynecore.core.broker.exceptions import (
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
    UnexpectedCancelError,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.helpers import volume_to_units
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model
from pynecore_ctrader.wire import CTraderProtocolError


# === Fakes ================================================================

def _cancelled_event(order_id):
    ev = _oa.ProtoOAExecutionEvent(
        executionType=_model.ProtoOAExecutionType.ORDER_CANCELLED,
    )
    ev.order.orderId = int(order_id)
    return ev


class _FakeWire:
    """Stubbed wire serving the deal-history bridge and cancel requests.

    A ``ProtoOACancelOrderReq`` resolves through ``cancel_responses`` (keyed by
    ``orderId``, value is an execution event to return or an exception to raise);
    unmapped cancels default to a confirmed ``ORDER_CANCELLED`` so the sweep can
    retire the row, mirroring a clean broker cancel. Every other request returns
    the canned ``deal_res``.
    """

    def __init__(self, *, deal_res=None, fail=False):
        self._deal_res = deal_res
        self._fail = fail
        self.requests: list = []
        self.cancel_responses: dict = {}

    async def send_request(self, req):
        self.requests.append(req)
        if self._fail:
            raise RuntimeError("wire down")
        if isinstance(req, _oa.ProtoOACancelOrderReq):
            resp = self.cancel_responses.get(req.orderId)
            if isinstance(resp, Exception):
                raise resp
            if resp is not None:
                return resp
            return _cancelled_event(req.orderId)
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
    # In ``returnProtectionOrders=True`` mode the broker reports a position's
    # live SL/TP as a separate STOP_LOSS_TAKE_PROFIT order linked by positionId.
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
        order.stopLoss = stop_loss
    if take_profit is not None:
        order.takeProfit = take_profit
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


def _close_deal(*, deal_id, order_id, position_id, closed_volume, price=1.1):
    """A FILLED closing deal carrying ``closePositionDetail.closedVolume``."""
    return _model.ProtoOADeal(
        dealId=deal_id, orderId=order_id, positionId=position_id,
        filledVolume=closed_volume, executionPrice=price,
        dealStatus=_model.ProtoOADealStatus.FILLED, moneyDigits=2, commission=0,
        executionTimestamp=1_700_000_100_000,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=closed_volume),
    )


def _deal_res(*deals, has_more=False) -> _oa.ProtoOADealListRes:
    return _oa.ProtoOADealListRes(deal=list(deals), hasMore=has_more)


def _seed_working(broker, coid, *, order_id, qty):
    broker.store_ctx.upsert_order(
        coid, symbol='EURUSD', side='buy', qty=qty, filled_qty=0.0,
        state='confirmed', pine_entry_id='long', exchange_order_id=str(order_id),
        extras={'order_id': str(order_id), 'position_id': None},
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

    assert len(events) == 1
    ev = events[0]
    assert ev.event_type == 'filled'
    assert ev.order.status.name == 'FILLED'
    assert ev.fill_qty == qty
    row = broker.store_ctx.get_order('c9')
    assert row is not None
    assert row.filled_qty == qty


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
    # row is retired through the terminal-close path, NOT promoted to a phantom
    # open position, and no fill is emitted.
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

    assert events == []  # no phantom ENTRY fill
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
    """Drive ``_emit_unexpected_cancellations``; capture events and any halt."""
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


def __test_grace_expired_missing_pending_retires_and_raises__(tmp_path):
    # A bot-owned row stamped missing past the grace window, whose final
    # deal-history re-check conclusively shows no fill and no close, is retired
    # as a synthetic cancel and, under the default 'stop' policy, halts the bot.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_position(broker, 'c1', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert len(events) == 1
    assert events[0].event_type == 'cancelled'
    assert events[0].order.status.name == 'CANCELLED'
    assert events[0].pine_id == 'long'
    assert isinstance(err, UnexpectedCancelError)
    assert 'c1' not in _live_coids(broker)


def __test_grace_not_expired_missing_pending_is_noop__(tmp_path):
    # A freshly-stamped row is still inside the grace window: no cancel, no halt.
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
    # no cancel event, no halt. Supersedes the old (never-written) natural_close_at.
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

    assert events == []                       # no synthetic cancel emitted
    assert err is None                        # no halt
    assert 'c3' not in _live_coids(broker)    # retired as a close
    assert 900 in broker._seen_deal_ids       # close deal recorded on the de-dup channel


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


def __test_grace_expired_working_no_fill_retires_as_cancel__(tmp_path):
    # A zero-fill working order, conclusively never filled at the grace-expiry
    # re-check, is the genuine unexpected cancel: retire + default 'stop' halt.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_working(broker, 'w1', order_id=600, qty=qty)
    row = broker.store_ctx.get_order('w1')
    stamped = dict(row.extras or {})
    stamped['missing_pending_since'] = 0.0
    broker.store_ctx.upsert_order('w1', extras=stamped)

    events, err = _drive_tracker(broker)

    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert isinstance(err, UnexpectedCancelError)
    assert 'w1' not in _live_coids(broker)


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

    assert events == []
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


def __test_policy_stop_with_quarantine_sink_does_not_halt__(tmp_path):
    # With the runner-wired quarantine sink present, the default 'stop' policy
    # latches the engine quarantine instead of raising: the event stream (and
    # the process) stays alive while trading is stopped engine-side.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    latched: list[tuple[str, dict]] = []
    broker.quarantine_sink = lambda reason, context: latched.append(
        (reason, context))
    _open(tmp_path, broker)
    _seed_position(broker, 'q1', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert err is None                      # no halt: the process stays alive
    assert 'q1' not in _live_coids(broker)  # row still retired
    assert len(latched) == 1
    reason, context = latched[0]
    assert 'q1' in reason
    assert context['policy'] == 'stop'


def __test_policy_ignore_retires_without_halting__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    broker.on_unexpected_cancel = 'ignore'
    _open(tmp_path, broker)
    _seed_position(broker, 'c4', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert err is None
    assert 'c4' not in _live_coids(broker)


def __test_policy_re_place_retires_without_halting__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    broker.on_unexpected_cancel = 're_place'
    _open(tmp_path, broker)
    _seed_position(broker, 'c5', position_id=222, qty=qty,
                   extras={'missing_pending_since': 0.0})

    events, err = _drive_tracker(broker)

    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert err is None
    assert 'c5' not in _live_coids(broker)


def __test_policy_stop_and_cancel_sweeps_siblings_then_halts__(tmp_path):
    # stop_and_cancel cancels the OTHER bot-owned working orders in the symbol
    # (ProtoOACancelOrderReq), retires their rows, then raises.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    broker.on_unexpected_cancel = 'stop_and_cancel'
    _open(tmp_path, broker)
    _seed_position(broker, 'gone', position_id=500, qty=qty,
                   extras={'missing_pending_since': 0.0})
    _seed_working(broker, 'sibling', order_id=600, qty=qty)

    events, err = _drive_tracker(broker)

    assert isinstance(err, UnexpectedCancelError)
    cancel_reqs = [r for r in broker._wire.requests
                   if isinstance(r, _oa.ProtoOACancelOrderReq)]
    assert any(r.orderId == 600 for r in cancel_reqs)
    assert _live_coids(broker) == []


def __test_policy_stop_and_cancel_leaves_open_position_sibling__(tmp_path):
    # stop_and_cancel must NOT cancel or retire an OPEN position sibling: a
    # fully-filled position keeps its original extras['order_id'], but it has no
    # working order to cancel and is the live broker exposure the operator is
    # meant to keep. Only the working sibling is swept; the position row survives.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    broker.on_unexpected_cancel = 'stop_and_cancel'
    _open(tmp_path, broker)
    _seed_position(broker, 'gone', position_id=500, qty=qty,
                   extras={'missing_pending_since': 0.0})
    _seed_working(broker, 'sibling', order_id=600, qty=qty)
    _seed_position(broker, 'open_pos', position_id=700, qty=qty)

    events, err = _drive_tracker(broker)

    assert isinstance(err, UnexpectedCancelError)
    cancel_reqs = [r for r in broker._wire.requests
                   if isinstance(r, _oa.ProtoOACancelOrderReq)]
    # The working sibling is cancelled; the open position's order_id is NOT.
    assert any(r.orderId == 600 for r in cancel_reqs)
    assert all(r.orderId != 111 for r in cancel_reqs)
    # The open position row is left intact for the operator.
    assert 'open_pos' in _live_coids(broker)


def __test_policy_stop_and_cancel_leaves_partially_filled_sibling__(tmp_path):
    # stop_and_cancel must NOT cancel or retire a PARTIALLY filled sibling: its
    # filled portion is live broker exposure linked to a position. Terminal-
    # closing its row would strand that filled position untracked after the halt.
    # Only the zero-fill resting sibling is swept; the partial row survives.
    qty = volume_to_units(2000)
    partial = volume_to_units(1000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    broker.on_unexpected_cancel = 'stop_and_cancel'
    _open(tmp_path, broker)
    _seed_position(broker, 'gone', position_id=500, qty=qty,
                   extras={'missing_pending_since': 0.0})
    _seed_working(broker, 'sibling', order_id=600, qty=qty)
    _seed_partial_working(broker, 'partial', order_id=650, qty=qty,
                          filled=partial, position_id=750)

    events, err = _drive_tracker(broker)

    assert isinstance(err, UnexpectedCancelError)
    cancel_reqs = [r for r in broker._wire.requests
                   if isinstance(r, _oa.ProtoOACancelOrderReq)]
    # The zero-fill working sibling is cancelled; the partial-fill order is NOT.
    assert any(r.orderId == 600 for r in cancel_reqs)
    assert all(r.orderId != 650 for r in cancel_reqs)
    # The partially-filled row is left intact for the operator.
    assert 'partial' in _live_coids(broker)
    store_row = broker.store_ctx.get_order('partial')
    assert store_row is not None and store_row.filled_qty == partial


def _fill_exec_event(order_id):
    ev = _oa.ProtoOAExecutionEvent(
        executionType=_model.ProtoOAExecutionType.ORDER_FILLED,
    )
    ev.order.orderId = int(order_id)
    return ev


def _cancel_rejected_event(order_id):
    ev = _oa.ProtoOAExecutionEvent(
        executionType=_model.ProtoOAExecutionType.ORDER_CANCEL_REJECTED,
    )
    ev.order.orderId = int(order_id)
    return ev


def __test_policy_stop_and_cancel_keeps_sibling_when_cancel_races_fill__(tmp_path):
    # The cancel loses a race to a fill: cTrader returns a non-raising
    # ORDER_FILLED execution event. Retiring the row would drop the just-surfaced
    # fill as external activity, stranding live exposure untracked. The row must
    # survive so the fill can still book against it.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    broker.on_unexpected_cancel = 'stop_and_cancel'
    _open(tmp_path, broker)
    _seed_position(broker, 'gone', position_id=500, qty=qty,
                   extras={'missing_pending_since': 0.0})
    _seed_working(broker, 'sibling', order_id=600, qty=qty)
    broker._wire.cancel_responses[600] = _fill_exec_event(600)

    events, err = _drive_tracker(broker)

    assert isinstance(err, UnexpectedCancelError)
    assert 'sibling' in _live_coids(broker)


def __test_policy_stop_and_cancel_keeps_sibling_on_cancel_rejected__(tmp_path):
    # ORDER_CANCEL_REJECTED (cancel/modify race) does NOT confirm the order is
    # gone — it may still be live or fill. The row must survive, not be retired.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    broker.on_unexpected_cancel = 'stop_and_cancel'
    _open(tmp_path, broker)
    _seed_position(broker, 'gone', position_id=500, qty=qty,
                   extras={'missing_pending_since': 0.0})
    _seed_working(broker, 'sibling', order_id=600, qty=qty)
    broker._wire.cancel_responses[600] = _cancel_rejected_event(600)

    events, err = _drive_tracker(broker)

    assert isinstance(err, UnexpectedCancelError)
    assert 'sibling' in _live_coids(broker)


def __test_policy_stop_and_cancel_keeps_sibling_on_ambiguous_cancel__(tmp_path):
    # An ambiguous cancel (timeout / link drop after send -> disposition unknown)
    # leaves the working order possibly live or filled. The row must survive so
    # reconcile resolves it, not be retired out from under that exposure.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    broker.on_unexpected_cancel = 'stop_and_cancel'
    _open(tmp_path, broker)
    _seed_position(broker, 'gone', position_id=500, qty=qty,
                   extras={'missing_pending_since': 0.0})
    _seed_working(broker, 'sibling', order_id=600, qty=qty)
    broker._wire.cancel_responses[600] = OrderDispositionUnknownError(
        "cancel timed out", client_order_id='sibling',
    )

    events, err = _drive_tracker(broker)

    assert isinstance(err, UnexpectedCancelError)
    assert 'sibling' in _live_coids(broker)


def __test_policy_stop_and_cancel_keeps_sibling_on_not_found__(tmp_path):
    # A *_NOT_FOUND cancel race means the working order left the book, but that
    # does NOT distinguish a cancel from a fill: execute_cancel_with_outcome
    # resolves the same race as UNKNOWN. Retiring the row here would delete its
    # refs before a just-surfaced fill can book, stranding exposure. The row must
    # survive, not be retired.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(), deal_res=_deal_res())
    broker.on_unexpected_cancel = 'stop_and_cancel'
    _open(tmp_path, broker)
    _seed_position(broker, 'gone', position_id=500, qty=qty,
                   extras={'missing_pending_since': 0.0})
    _seed_working(broker, 'sibling', order_id=600, qty=qty)
    not_found = ExchangeOrderRejectedError("order gone")
    not_found.__cause__ = CTraderProtocolError('ORDER_NOT_FOUND', '')
    broker._wire.cancel_responses[600] = not_found

    events, err = _drive_tracker(broker)

    assert isinstance(err, UnexpectedCancelError)
    assert 'sibling' in _live_coids(broker)


def __test_reconcile_pass_propagates_unexpected_cancel_halt__(tmp_path):
    # Integration: the snapshot pass does NOT re-clear a still-missing stamp,
    # and the halt from the grace tracker propagates through _run_reconcile_pass
    # (it is NOT swallowed by the transient gap-filler guard).
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(positions=[]), deal_res=_deal_res())
    _open(tmp_path, broker)
    _seed_position(broker, 'c6', position_id=222, qty=qty,
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
    # Each live entry whose broker position is open feeds the observed sink with
    # the absolute stopLoss / takeProfit carried by the position's protection
    # order (returnProtectionOrders=True mode), keyed by the row coid.
    qty = volume_to_units(2000)
    pos = _position(position_id=222, volume=2000)
    prot = _protection_order(position_id=222, stop_loss=1.05, take_profit=1.15)
    broker = _ReconcileBroker(_recon(orders=[prot], positions=[pos]))
    captured = _capture_failsafe(broker)
    _open(tmp_path, broker)
    _seed_position(broker, 'p1', position_id=222, qty=qty)

    _run(broker)

    assert captured == [('p1', prot.stopLoss, prot.takeProfit, None)]


def __test_reconcile_failsafe_trailing_suppresses_stop_level__(tmp_path):
    # cTrader has no relative trailing field: while trailing is active the
    # moving stop is NOT reported as stop_level (it cannot be matched against
    # the engine's relative desired-trailing) and trailing_stop stays None.
    qty = volume_to_units(2000)
    pos = _position(position_id=222, volume=2000)
    prot = _protection_order(position_id=222, stop_loss=1.05,
                             take_profit=1.15, trailing=True)
    broker = _ReconcileBroker(_recon(orders=[prot], positions=[pos]))
    captured = _capture_failsafe(broker)
    _open(tmp_path, broker)
    _seed_position(broker, 'p1', position_id=222, qty=qty)

    _run(broker)

    assert captured == [('p1', None, prot.takeProfit, None)]


def __test_reconcile_failsafe_no_protection_order_degrades__(tmp_path):
    # An open position whose protection order is absent (bracket cleared / never
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
