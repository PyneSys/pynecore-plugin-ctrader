"""
@pyne
"""
import asyncio
import logging
from time import time as epoch_time

import pytest

from pynecore.core.broker.exceptions import BrokerManualInterventionError, UnexpectedCancelError
from pynecore.core.broker.disappearance import (
    MISSING_ACCEPTED_BY_RESUME_EXTRA,
    accept_missing_before_resume,
)
from pynecore.core.broker.models import (
    CANCEL_REASON_VENUE_REJECTED,
    VENUE_DRIVEN_CANCEL_REASONS,
    QuarantineKind,
)
from pynecore.core.broker.protection_guard import ProtectionLevels
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader import events as ctrader_events
from pynecore_ctrader import reconcile as ctrader_reconcile
from pynecore_ctrader.helpers import volume_to_units
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model
from pynecore_ctrader.models import _SymbolRules
from pynecore_ctrader.protection import (
    CloseOrigin,
    _ClaimedFill,
    _PositionDeal,
    protection_key,
)
from pynecore_ctrader.reconcile import _DealWalk
from pynecore_ctrader.wire import CTraderProtocolError, CTraderTimeoutError


# === Fakes ================================================================

class _FakeWire:
    """Stubbed wire serving the deal-history bridge: every request returns ``deal_res``.

    A closing order looked up by ``ProtoOAOrderDetailsReq`` is the position's
    own SL/TP order: the venue closed it.
    """

    def __init__(self, *, deal_res=None, fail=False):
        self._deal_res = deal_res
        self._fail = fail
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

    assert len(events) == 1
    ev = events[0]
    assert ev.event_type == 'filled'
    assert ev.order.status.name == 'FILLED'
    assert ev.fill_qty == qty
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


def __test_reconcile_snapshot_booked_partial_commission_recovered_once__(tmp_path):
    # The first half was booked by the snapshot partial-fill path, which knows
    # no deal and books no commission. The recovered residual is still priced
    # from its own (later) deal, and the commissions of BOTH unseen deals are
    # booked now, since neither was booked before.
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
        _recon(positions=[_position(position_id=222, volume=2000)]),
        deal_res=_deal_res(second, first),
    )
    _open(tmp_path, broker)
    _seed_partial_working(broker, 'c11', order_id=111, qty=qty,
                          filled=partial, position_id=222)

    events = _run(broker)

    assert len(events) == 1
    ev = events[0]
    assert ev.fill_price == 2.0
    assert ev.fee == 8.0


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
        from_ms, newer.executionTimestamp - 1)
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


class _OrderStatusWire(_FakeWire):
    """An empty deal history, and the vanished entry order's own status."""

    def __init__(self, *, status, expiry=None, details_error=None, executed=0):
        super().__init__(deal_res=_deal_res())
        self._status = status
        self._expiry = expiry
        self._details_error = details_error
        self._executed = executed

    async def send_request(self, req, *, timeout=None, **kwargs):
        if not isinstance(req, _oa.ProtoOAOrderDetailsReq):
            return await super().send_request(req, timeout=timeout, **kwargs)
        self.requests.append(req)
        if self._details_error is not None:
            raise self._details_error
        order = _order(order_id=req.orderId, volume=2000, executed=self._executed)
        order.orderStatus = self._status
        if self._expiry is not None:
            order.expirationTimestamp = self._expiry
        return _oa.ProtoOAOrderDetailsRes(order=order)


def _foreign_expiry_wire() -> _OrderStatusWire:
    """The vanished entry order expired at an expiry the bot never set: someone
    else edited it, the one gap-path cancel the unexpected-cancel policy handles."""
    return _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED,
                            expiry=1_700_000_900_000)


def _vanished_working_broker(tmp_path, wire) -> tuple[_ReconcileBroker, list]:
    """A zero-fill working entry stamped missing past the grace window, with
    the external-intervention reports captured."""
    broker = _ReconcileBroker(_recon())
    broker._wire = wire
    _open(tmp_path, broker)
    _seed_working(broker, 'w1', order_id=600, qty=volume_to_units(2000),
                  extras={'missing_pending_since': 0.0})
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    return broker, reports


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
    _OrderStatusWire(status=_model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED,
                     details_error=CTraderProtocolError('ORDER_NOT_FOUND', 'no such order')),
], ids=['cancelled', 'not-found'])
def __test_grace_expired_working_order_cancel_nobody_can_be_named_for_is_audited__(
        tmp_path, caplog, wire):
    """The stream missed the cancel of a pending entry; the order's status reads
    cancelled (or the venue no longer knows the order). Only the push carries
    ``isServerEvent``, so a server-initiated cancel and someone else's look the
    same: the cancel is booked, logged as an ERROR and audited, and the
    unexpected-cancel policy never runs."""
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
    # latches the engine's external-intervention quarantine instead of raising:
    # the event stream (and the process) stays alive while the bot leaves the
    # venue untouched, so the other bot-owned working order is not cancelled.
    qty = volume_to_units(2000)
    broker = _ReconcileBroker(_recon())
    broker._wire = _foreign_expiry_wire()
    latched: list[tuple[str, dict, QuarantineKind]] = []
    broker.quarantine_sink = lambda reason, context, *, kind: latched.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    _seed_working(broker, 'q1', order_id=222, qty=qty,
                  extras={'missing_pending_since': 0.0})
    _seed_working(broker, 'q2', order_id=333, qty=qty)

    events, err = _drive_tracker(broker)

    assert len(events) == 1 and events[0].event_type == 'cancelled'
    assert err is None                      # no halt: the process stays alive
    assert 'q1' not in _live_coids(broker)  # row still retired
    assert len(latched) == 1
    reason, context, kind = latched[0]
    assert 'q1' in reason
    assert context['policy'] == 'stop'
    assert kind is QuarantineKind.EXTERNAL_INTERVENTION
    assert 'q2' in _live_coids(broker)
    assert not any(isinstance(r, _oa.ProtoOACancelOrderReq)
                   for r in broker._wire.requests)


def __test_policy_ignore_retires_without_halting__(tmp_path):
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


def __test_policy_re_place_retires_without_halting__(tmp_path):
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


def __test_reconcile_pass_propagates_unexpected_cancel_halt__(tmp_path):
    # Integration: the snapshot pass does NOT re-clear a still-missing stamp,
    # and the halt from the grace tracker propagates through _run_reconcile_pass
    # (it is NOT swallowed by the transient gap-filler guard).
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


def _recover(broker) -> None:
    """One grace-expiry pass, then the report flush ``watch_orders`` runs after it."""
    events, err = _drive_tracker(broker)
    assert events == [] and err is None
    broker._flush_external_interventions()


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
    """The run's close whose answer was lost may be the one that executed."""
    broker, reports = _recovered_close_broker(
        tmp_path, _closing_order(999), executed_ms=int(epoch_time() * 1000))
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
                           filled=2000, closed=2000)]

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
    monkeypatch.setattr(ctrader_events, '_RECONCILE_PASS_BUDGET_S', 0.05)
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
    """The earlier process's close never got its answer: it may be the one that executed."""
    first, _ = _recovered_close_broker(
        tmp_path, _closing_order(999), executed_ms=int(epoch_time() * 1000))
    first._open_own_close(222)

    restarted, reports = _restarted(tmp_path, first, _closing_order(999))
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
    assert events == [] and err is None
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


def __test_recovered_report_that_halted_is_not_replayed__(tmp_path):
    first, _ = _recovered_close_broker(tmp_path, _closing_order(999))
    first.on_unexpected_cancel = 'halt'
    _drive_tracker(first)
    with pytest.raises(UnexpectedCancelError):
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

    assert _run(broker) == []
    broker._flush_external_interventions()

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
    """Without a quarantine sink the policy halts: the halt leaves the stream
    after the pass that retired the position."""
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

    _recover(broker)

    assert 'c3' not in _live_coids(broker)
    assert reports == []
    assert {903, 904} <= broker._judged_close_deals


def __test_recovered_netting_close_below_the_share_is_reported__(tmp_path):
    broker, reports = _netting_recovered_broker(
        tmp_path, _netting_history(foreign_closed=2000, sl_closed=1000))
    _seed_position(broker, 'c3', position_id=222, qty=20.0,
                   extras={'missing_pending_since': 0.0})
    broker.store_ctx.add_ref('c3', 'order_id', '111')

    _recover(broker)

    assert len(reports) == 1
    context = reports[0][1]
    assert context['deal_ids'] == [903]
    assert context['booked_qty'] == pytest.approx(10.0)


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

    _run(broker)
    broker._flush_external_interventions()
    assert reports == [] and _retired(broker) == 0.0, "one pass is not proof"

    _run(broker)
    broker._flush_external_interventions()
    assert len(reports) == 1
    assert reports[0][1]['deal_ids'] == [903]
    assert reports[0][1]['booked_qty'] == pytest.approx(10.0)
    assert _retired(broker) == pytest.approx(10.0)

    _run(broker)
    broker._flush_external_interventions()
    assert len(reports) == 1


def __test_missed_venue_partial_close_retires_without_a_report__(tmp_path):
    broker, reports = _shrunk_position_broker(
        tmp_path, closing_order=_closing_order(600, stop_out=True))

    _run(broker)
    _run(broker)
    broker._flush_external_interventions()

    assert reports == []
    assert _retired(broker) == pytest.approx(10.0)


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


def __test_pyramid_shrink_after_an_operator_resume_is_accepted__(tmp_path):
    """Two entry rows share the shrunk position, so the startup baseline leaves
    it alone; the first pass still accepts the operator's close."""
    broker, reports = _shrunk_position_broker(tmp_path, closing_order=_closing_order(600))
    broker.store_ctx.upsert_order('c3', qty=10.0, filled_qty=10.0)
    _seed_position(broker, 'c4', position_id=222, qty=10.0)
    accept_missing_before_resume(broker.store_ctx)
    broker._apply_adoption_baseline(broker._recon)
    assert _retired(broker) == 0.0

    for _ in range(3):
        _run(broker)
    broker._flush_external_interventions()

    assert reports == []
    assert _retired(broker) + _retired(broker, 'c4') == pytest.approx(10.0)
    assert len(_store_events(broker, 'ctrader_close_accepted_by_resume')) == 1

    # A shrink after that first pass is judged as usual.
    broker._recon = _recon(positions=[_position(position_id=222, volume=500)])
    broker._wire._deal_res.deal.append(_timed(_close_deal(
        deal_id=905, order_id=600, position_id=222, closed_volume=500), 1_700_000_005_000))
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
                           filled=2000, closed=2000)]

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
