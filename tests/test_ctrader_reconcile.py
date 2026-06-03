"""
@pyne
"""
import asyncio

from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.helpers import volume_to_units
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model


# === Fakes ================================================================

class _FakeWire:
    """Stubbed wire serving only the deal-history bridge request."""

    def __init__(self, *, deal_res=None, fail=False):
        self._deal_res = deal_res
        self._fail = fail
        self.requests: list = []

    async def send_request(self, req):
        self.requests.append(req)
        if self._fail:
            raise RuntimeError("wire down")
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


def _position(*, position_id, volume, price=1.1, side=_model.ProtoOATradeSide.BUY):
    return _model.ProtoOAPosition(
        positionId=position_id,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        price=price,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=volume, tradeSide=side),
    )


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


def __test_reconcile_natural_close_row_not_stamped__(tmp_path):
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon(positions=[]))  # position gone
    _open(tmp_path, broker)
    _seed_position(broker, 'c2', position_id=222, qty=qty,
                   extras={'natural_close_at': 99.0})

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
