"""
@pyne
"""
import asyncio
import time

import pytest

import pynecore_ctrader.recovery as _recovery
from pynecore.core.broker.disappearance import accept_missing_before_resume
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import ENTRY_KIND_WORKING, create_entry_order_row

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.helpers import volume_to_units
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model
from pynecore_ctrader.models import _SymbolRules

_FILLED = _model.ProtoOAOrderStatus.ORDER_STATUS_FILLED
_REJECTED = _model.ProtoOAOrderStatus.ORDER_STATUS_REJECTED
_ACCEPTED = _model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED
_CANCELLED = _model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED


# === Fakes ================================================================

class _RecoveryWire:
    """Wire stub dispatching order-history vs deal-history requests by type.

    ``orders`` answers ``ProtoOAOrderDetailsReq`` for the closing orders of a
    position closed while the bot was down; the lookup of any other order
    times out.
    """

    def __init__(self, *, order_res=None, deal_res=None, order_fail=False,
                 deal_fail=False, orders=()):
        self._order_res = order_res
        self._deal_res = deal_res
        self._order_fail = order_fail
        self._deal_fail = deal_fail
        self.orders = {order.orderId: order for order in orders}
        self.requests: list = []

    async def send_request(self, req, *, timeout=None, **_kwargs):
        self.requests.append(req)
        if isinstance(req, _oa.ProtoOAOrderListReq):
            if self._order_fail:
                raise RuntimeError("order history down")
            return self._order_res
        if isinstance(req, _oa.ProtoOADealListReq):
            if self._deal_fail:
                raise RuntimeError("deal history down")
            return self._deal_res
        if isinstance(req, _oa.ProtoOAOrderDetailsReq):
            if req.orderId not in self.orders:
                raise TimeoutError("order details down")
            return _oa.ProtoOAOrderDetailsRes(order=self.orders[req.orderId])
        raise AssertionError(f"unexpected request {type(req).__name__}")


class _RecoveryBroker(CTrader):
    """cTrader broker with ``_reconcile`` canned and the wire stubbed."""

    def __init__(self, recon, *, order_res=None, deal_res=None, order_fail=False,
                 deal_fail=False, orders=()):
        super().__init__(symbol=None, config=_make_config())
        self._live_account_id = 999
        self._symbols_by_name = {'EURUSD': 1}
        self._symbols_by_id = {1: 'EURUSD'}
        self._recon = recon
        self._wire = _RecoveryWire(
            order_res=order_res, deal_res=deal_res, order_fail=order_fail,
            deal_fail=deal_fail, orders=orders)

    async def _reconcile(self, *, return_protection_orders=False):
        return self._recon


def _make_config(**overrides) -> CTraderConfig:
    defaults = dict(demo=True, client_id="cid", client_secret="sec", account_id="999")
    defaults.update(overrides)
    return CTraderConfig(**defaults)


def _open(tmp_path, broker) -> None:
    store = BrokerStore(tmp_path / "broker.sqlite", plugin_name=broker.plugin_name)
    identity = RunIdentity(
        strategy_id="recov", symbol="EURUSD", timeframe="60",
        account_id="recov-account",
    )
    broker.store_ctx = store.open_run(identity, script_source="// recov")


def _recon(*, orders=(), positions=()) -> _oa.ProtoOAReconcileRes:
    return _oa.ProtoOAReconcileRes(order=list(orders), position=list(positions))


def _order(*, order_id, coid, volume, executed=0, position_id=0, price=0.0,
           status=_ACCEPTED):
    return _model.ProtoOAOrder(
        orderId=order_id, clientOrderId=coid,
        orderType=_model.ProtoOAOrderType.LIMIT, orderStatus=status,
        executedVolume=executed, executionPrice=price, positionId=position_id,
        utcLastUpdateTimestamp=1_700_000_000_000,
        tradeData=_model.ProtoOATradeData(
            symbolId=1, volume=volume, tradeSide=_model.ProtoOATradeSide.BUY),
    )


def _position(*, position_id, volume, price=1.1):
    return _model.ProtoOAPosition(
        positionId=position_id,
        positionStatus=_model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
        price=price,
        tradeData=_model.ProtoOATradeData(
            symbolId=1, volume=volume, tradeSide=_model.ProtoOATradeSide.BUY),
    )


def _deal(*, deal_id, order_id, position_id, filled_volume, price=1.1):
    return _model.ProtoOADeal(
        dealId=deal_id, orderId=order_id, positionId=position_id,
        filledVolume=filled_volume, executionPrice=price,
        dealStatus=_model.ProtoOADealStatus.FILLED, moneyDigits=2, commission=0,
        executionTimestamp=1_700_000_000_000,
    )


def _close_deal(*, deal_id, order_id, position_id, closed_volume, price=1.1):
    return _model.ProtoOADeal(
        dealId=deal_id, orderId=order_id, positionId=position_id,
        filledVolume=closed_volume, executionPrice=price,
        dealStatus=_model.ProtoOADealStatus.FILLED, moneyDigits=2, commission=0,
        executionTimestamp=1_700_000_100_000,
        closePositionDetail=_model.ProtoOAClosePositionDetail(closedVolume=closed_volume),
    )


def _order_res(*orders, has_more=False) -> _oa.ProtoOAOrderListRes:
    return _oa.ProtoOAOrderListRes(order=list(orders), hasMore=has_more)


def _deal_res(*deals, has_more=False) -> _oa.ProtoOADealListRes:
    return _oa.ProtoOADealListRes(deal=list(deals), hasMore=has_more)


def _seed_submitted(broker, coid, *, qty) -> None:
    """Seed a persist-first ``submitted`` row (crash before the post-ack confirm)."""
    create_entry_order_row(
        broker.store_ctx, coid=coid, symbol='EURUSD', side='buy', qty=qty,
        intent_key=coid, pine_entry_id='long', kind=ENTRY_KIND_WORKING,
        order_type='limit',
    )


def _recover(broker) -> None:
    asyncio.run(broker._recover_in_flight_submissions())


def _live_coids(broker) -> set:
    return {r.client_order_id for r in broker.store_ctx.iter_live_orders()}


# === Source 1: live reconcile order[] coid match ==========================

def __test_recover_send_before_ack_resting_order_confirms__(tmp_path):
    # Crash between wire-send and post-ack persist: the order DID land and is
    # resting in order[] carrying the row's clientOrderId. The deterministic coid
    # match confirms it and records the broker order_id alias.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(orders=[_order(order_id=111, coid='c1', volume=2000, executed=0)]))
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c1', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c1')
    assert row is not None
    assert row.state == 'confirmed'
    assert (row.extras or {}).get('order_id') == '111'


def __test_recover_live_position_alone_does_not_confirm__(tmp_path):
    # Counterpoint live-position rule: ProtoOAPosition carries no clientOrderId,
    # so a live position with no order[] coid match (and an empty conclusive
    # order history) NEVER confirms the row — it stays pending.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]),
        order_res=_order_res(),  # conclusive empty
    )
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c2', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c2')
    assert row is not None
    assert row.state == 'submitted'


def __test_recover_live_partial_fill_seeds_dedup__(tmp_path):
    # The order is still resting in order[] (coid match) but already PARTIALLY
    # filled (executedVolume > 0). The Source 1 branch must read the fill deals
    # and seed _seen_deal_ids before confirming, so a reconnect PUSH replay of
    # that same partial-fill execution is de-duped instead of double-applied.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(orders=[_order(order_id=111, coid='c2b', volume=2000,
                              executed=1000, position_id=222)]),
        deal_res=_deal_res(_deal(deal_id=902, order_id=111, position_id=222,
                                 filled_volume=1000)),
    )
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c2b', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c2b')
    assert row is not None
    assert row.state == 'confirmed'
    assert row.filled_qty == volume_to_units(1000)
    assert 902 in broker._seen_deal_ids
    # A live-order confirm persists the broker-clock submitted_at_ms since-anchor,
    # exactly as _persist_entry does, so a later offline fill of this working order
    # is recovered from its own submit time (never the fixed-window fallback).
    assert (row.extras or {}).get('submitted_at_ms') == 1_700_000_000_000


def __test_recover_live_partial_fill_inconclusive_deals_stays_pending__(tmp_path):
    # The order is still resting in order[] (coid match) with positive
    # executedVolume, but the deal-history read FAILS (inconclusive, no dealId
    # surfaced). Confirming filled_qty from executedVolume here would advance the
    # cursor with NO de-dup anchor, so a reconnect PUSH replay of that same
    # partial-fill execution would be double-applied. The row must stay pending
    # for the runtime reconcile re-entry, which recovers it with the deal ids.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(orders=[_order(order_id=111, coid='c2c', volume=2000,
                              executed=1000, position_id=222)]),
        deal_fail=True,  # deal-history read fails -> inconclusive, no deal ids
    )
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c2c', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c2c')
    assert row is not None
    assert row.state == 'submitted'
    assert 'c2c' in _live_coids(broker)
    assert not broker._seen_deal_ids
    # The row stays pending but the broker refs matched by coid ARE recorded, so a
    # fill PUSH arriving before the next reconcile re-entry reverse-maps to this row
    # (resolved by order_id / position_id) instead of being treated as external.
    assert (row.extras or {}).get('order_id') == '111'
    assert (row.extras or {}).get('position_id') == 222
    assert broker.store_ctx.find_by_ref('order_id', '111') is not None
    assert broker.store_ctx.find_by_ref('position_id', '222') is not None


# === Source 2: order + deal history bridge ================================

def __test_recover_fill_only_in_deal_history_confirms_filled__(tmp_path):
    # The order filled while the stream was down; it has left order[] but the
    # order history echoes the coid (status FILLED) and the deal history carries
    # the fill. The row is confirmed at the recovered size + de-dup seeded.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]),
        order_res=_order_res(_order(
            order_id=111, coid='c3', volume=2000, executed=2000,
            position_id=222, status=_FILLED)),
        deal_res=_deal_res(_deal(deal_id=900, order_id=111, position_id=222,
                                 filled_volume=2000)),
    )
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c3', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c3')
    assert row is not None
    assert row.state == 'confirmed'
    assert row.filled_qty == qty
    assert (row.extras or {}).get('position_id') == 222
    assert (row.extras or {}).get('order_id') == '111'
    assert (row.extras or {}).get('submitted_at_ms') == 1_700_000_000_000
    assert 900 in broker._seen_deal_ids


def _filled_then_closed_broker(tmp_path, *, closing_order=None):
    """A pending row ``c4`` whose entry filled, then whose position 222 was
    closed by order 999 while the bot was down; ``closing_order`` answers the
    lookup of that closing order (none: the lookup fails)."""
    broker = _RecoveryBroker(
        _recon(positions=[]),
        order_res=_order_res(_order(
            order_id=111, coid='c4', volume=2000, executed=2000,
            position_id=222, status=_FILLED)),
        deal_res=_deal_res(
            _deal(deal_id=905, order_id=111, position_id=222, filled_volume=2000),
            _close_deal(deal_id=906, order_id=999, position_id=222,
                        closed_volume=2000)),
        orders=[closing_order] if closing_order is not None else (),
    )
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c4', qty=volume_to_units(2000))
    return broker, reports


def __test_recover_filled_then_closed_retires_not_phantom__(tmp_path):
    # The entry filled then its position closed while down. The OPEN snapshot has
    # no position, but the deal history carries the opening deal AND a closing
    # deal (closedVolume). The row is retired (confirmed != live exposure), not
    # promoted to a phantom open position.
    broker, reports = _filled_then_closed_broker(tmp_path, closing_order=_closing_order(
        999, order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT))

    _recover(broker)
    broker._flush_external_interventions()

    row = broker.store_ctx.get_order('c4')
    assert row is not None
    assert row.state == 'closed'
    assert 'c4' not in _live_coids(broker)
    assert reports == []
    assert {905, 906} <= broker._seen_deal_ids  # de-dup seeded before retiring


def __test_recover_filled_then_closed_by_someone_else_is_reported__(tmp_path):
    """A retired row leaves every later scan: its close is judged before it goes."""
    broker, reports = _filled_then_closed_broker(
        tmp_path, closing_order=_closing_order(999))

    _recover(broker)
    broker._flush_external_interventions()

    assert 'c4' not in _live_coids(broker)
    assert len(reports) == 1
    assert reports[0][1]['deal_ids'] == [906] and reports[0][1]['recovered'] is True


def __test_recover_filled_then_closed_before_an_operator_resume_is_accepted__(tmp_path):
    broker, reports = _filled_then_closed_broker(
        tmp_path, closing_order=_closing_order(999))
    accept_missing_before_resume(broker.store_ctx)

    _recover(broker)
    broker._flush_external_interventions()

    assert 'c4' not in _live_coids(broker)
    assert reports == []
    assert len(list(broker.store_ctx.iter_events_by_kind_since(
        'ctrader_close_accepted_by_resume', 0))) == 1


def __test_recover_filled_then_closed_unjudged_close_stays_pending__(tmp_path):
    """The closing order cannot be looked up yet: the row stays pending with its
    refs, for the runtime reconcile to judge and retire."""
    broker, reports = _filled_then_closed_broker(tmp_path)

    _recover(broker)
    broker._flush_external_interventions()

    row = broker.store_ctx.get_order('c4')
    assert row is not None
    assert 'c4' in _live_coids(broker)
    assert row.state == 'submitted'
    assert (row.extras or {}).get('order_id') is not None
    assert reports == []


def __test_recover_rejected_order_lands_rejected__(tmp_path):
    # The order history echoes the coid with a terminal REJECTED status: the
    # dispatch was refused. The row is retired rejected through the terminal
    # writer, never re-issued.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(),
        order_res=_order_res(_order(
            order_id=111, coid='c5', volume=2000, status=_REJECTED)),
    )
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c5', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c5')
    assert row is not None
    assert row.state == 'rejected'
    assert 'c5' not in _live_coids(broker)


def __test_recover_partial_then_cancelled_keeps_fill__(tmp_path):
    # A LIMIT order PARTIALLY filled before its residual was cancelled while the
    # bot was down: the order history echoes the coid with a terminal CANCELLED
    # status BUT a positive executedVolume, and the position is still open at the
    # filled size. The terminal status must NOT zero-fill the row away — the
    # partial fill is a live position, so the deal bridge confirms it.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(positions=[_position(position_id=222, volume=1000)]),
        order_res=_order_res(_order(
            order_id=111, coid='c5b', volume=2000, executed=1000,
            position_id=222, status=_CANCELLED)),
        deal_res=_deal_res(_deal(deal_id=901, order_id=111, position_id=222,
                                 filled_volume=1000)),
    )
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c5b', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c5b')
    assert row is not None
    assert row.state == 'confirmed'
    assert row.filled_qty == volume_to_units(1000)
    assert 'c5b' in _live_coids(broker)
    assert 901 in broker._seen_deal_ids


def __test_recover_position_gone_inconclusive_stays_pending__(tmp_path):
    # The order filled (terminal CANCELLED, positive executedVolume) and its
    # position is gone from the OPEN snapshot, but the deal-history read FAILS
    # (inconclusive). A missing closedVolume is a transport gap, not proof the
    # position is still open, so confirming would record a closed position as a
    # live row — the row must stay pending for the runtime reconcile re-entry.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(positions=[]),  # position gone from the open snapshot
        order_res=_order_res(_order(
            order_id=111, coid='c5c', volume=2000, executed=2000,
            position_id=222, status=_CANCELLED)),
        deal_fail=True,  # deal-history read fails -> inconclusive bridge
    )
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c5c', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c5c')
    assert row is not None
    assert row.state == 'submitted'
    assert 'c5c' in _live_coids(broker)
    # Broker refs matched by coid are recorded so a fill PUSH reverse-maps here.
    assert (row.extras or {}).get('order_id') == '111'
    assert broker.store_ctx.find_by_ref('order_id', '111') is not None


def __test_recover_filled_position_open_inconclusive_deals_stays_pending__(tmp_path):
    # The order history echoes the coid FILLED with positive executedVolume and
    # the position IS still open, but the deal-history read FAILS (inconclusive,
    # no dealId surfaced). Confirming from order.executedVolume here would advance
    # filled_qty with NO de-dup anchor, so a reconnect PUSH replay of the fill
    # would be double-applied. The row must stay pending for the runtime reconcile
    # re-entry, which recovers it with the deal ids.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]),
        order_res=_order_res(_order(
            order_id=111, coid='c5d', volume=2000, executed=2000,
            position_id=222, status=_FILLED)),
        deal_fail=True,  # deal-history read fails -> inconclusive, no deal ids
    )
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c5d', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c5d')
    assert row is not None
    assert row.state == 'submitted'
    assert 'c5d' in _live_coids(broker)
    assert not broker._seen_deal_ids
    # Broker refs matched by coid are recorded so a fill PUSH reverse-maps here.
    assert (row.extras or {}).get('order_id') == '111'
    assert (row.extras or {}).get('position_id') == 222
    assert broker.store_ctx.find_by_ref('order_id', '111') is not None
    assert broker.store_ctx.find_by_ref('position_id', '222') is not None


# === Still-unknown + evidence-gated TTL ===================================

def __test_recover_unknown_young_stays_pending__(tmp_path):
    # Conclusive empty order history (no coid match) but the row is young: it
    # stays still_unknown (pending) — never abandoned before the TTL, never
    # re-dispatched.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c6', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c6')
    assert row is not None
    assert row.state == 'submitted'


def __test_recover_unknown_past_ttl_abandons__(tmp_path):
    # Same conclusive-empty read, but past the evidence-gated TTL: the row is
    # retired abandoned_unknown so the strategy is not blocked forever.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c7', qty=qty)

    saved = _recovery._ABANDON_TTL_S
    _recovery._ABANDON_TTL_S = -1.0  # any row is past the TTL
    try:
        _recover(broker)
    finally:
        _recovery._ABANDON_TTL_S = saved

    row = broker.store_ctx.get_order('c7')
    assert row is not None
    assert row.state == 'rejected'
    assert 'c7' not in _live_coids(broker)


def __test_recover_inconclusive_history_never_abandons__(tmp_path):
    # A FAILED order-history read is non-evidence: even past the TTL the row is
    # never abandoned (a truncated read must not conclude absence).
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(_recon(), order_fail=True)
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c8', qty=qty)

    saved = _recovery._ABANDON_TTL_S
    _recovery._ABANDON_TTL_S = -1.0
    try:
        _recover(broker)
    finally:
        _recovery._ABANDON_TTL_S = saved

    row = broker.store_ctx.get_order('c8')
    assert row is not None
    assert row.state == 'submitted'


# === Startup-orphan retirement ============================================

def _closing_order(order_id, *, order_type=_model.ProtoOAOrderType.MARKET,
                   coid='intruder-1'):
    """A closing order; by default another API client's order."""
    return _model.ProtoOAOrder(
        orderId=order_id, orderType=order_type, orderStatus=_FILLED, closingOrder=True,
        positionId=222, executedVolume=2000, clientOrderId=coid,
        tradeData=_model.ProtoOATradeData(symbolId=1, volume=2000,
                                          tradeSide=_model.ProtoOATradeSide.SELL))


def _orphan_broker(tmp_path, *, closing_order=None, deal_fail=False):
    """A prior-run confirmed row whose position 222 closed while the bot was
    down; the deal history holds its fill and, with ``closing_order``, the close."""
    deals = [_deal(deal_id=910, order_id=111, position_id=222, filled_volume=2000)]
    if closing_order is not None:
        deals.append(_close_deal(deal_id=911, order_id=closing_order.orderId,
                                 position_id=222, closed_volume=2000))
    broker = _RecoveryBroker(
        _recon(), deal_res=_deal_res(*deals), deal_fail=deal_fail,
        orders=[closing_order] if closing_order is not None else ())
    reports: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: reports.append(
        (reason, context, kind))
    _open(tmp_path, broker)
    qty = volume_to_units(2000)
    broker.store_ctx.upsert_order(
        'c9', symbol='EURUSD', side='buy', qty=qty, filled_qty=qty,
        state='confirmed', pine_entry_id='long', intent_key='ik9',
        exchange_order_id='222', extras={'order_id': '111', 'position_id': 222})
    broker.store_ctx.add_ref('c9', 'order_id', '111')
    return broker, reports


def __test_recover_retires_startup_orphan__(tmp_path):
    # No pending rows; a prior-run confirmed row whose position the venue's own
    # SL closed while the bot was down. Its order_id / position_id are absent
    # from both snapshots, so it is retired (close + envelope-anchor cleanup) —
    # else the runtime tracker would later raise a false UnexpectedCancelError.
    broker, reports = _orphan_broker(tmp_path, closing_order=_closing_order(
        999, order_type=_model.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT))

    _recover(broker)
    broker._flush_external_interventions()

    assert 'c9' not in _live_coids(broker)
    assert reports == []
    assert 911 in broker._seen_deal_ids


def __test_recover_orphan_closed_by_someone_else_is_reported__(tmp_path):
    broker, reports = _orphan_broker(tmp_path, closing_order=_closing_order(999))

    _recover(broker)
    broker._flush_external_interventions()

    assert 'c9' not in _live_coids(broker)
    assert len(reports) == 1
    assert reports[0][1]['deal_ids'] == [911] and reports[0][1]['recovered'] is True


def __test_recover_orphan_closed_without_a_client_order_id_is_audited_not_reported__(
        tmp_path):
    """Nothing tells another client's close from a server-initiated one here."""
    broker, reports = _orphan_broker(tmp_path, closing_order=_closing_order(999, coid=''))

    _recover(broker)
    broker._flush_external_interventions()

    assert 'c9' not in _live_coids(broker)
    assert reports == []
    assert len(list(broker.store_ctx.iter_events_by_kind_since(
        'ctrader_close_unattributed', 0))) == 1


def __test_recover_orphan_closed_before_an_operator_resume_is_accepted__(tmp_path):
    """The operator closed the stopped bot's position and resumed it: the next
    start accepts the venue as it stands instead of quarantining again."""
    broker, reports = _orphan_broker(tmp_path, closing_order=_closing_order(999))
    accept_missing_before_resume(broker.store_ctx)

    _recover(broker)
    broker._flush_external_interventions()

    assert 'c9' not in _live_coids(broker)
    assert reports == []
    assert len(list(broker.store_ctx.iter_events_by_kind_since(
        'ctrader_close_accepted_by_resume', 0))) == 1


def __test_recover_orphan_without_a_close_in_its_history_is_retired__(tmp_path):
    broker, reports = _orphan_broker(tmp_path)

    _recover(broker)
    broker._flush_external_interventions()

    assert 'c9' not in _live_coids(broker)
    assert reports == []


def __test_recover_orphan_with_an_unreadable_history_stays_live__(tmp_path):
    """Who closed it is not known: the runtime reconcile retires it later,
    judging the same evidence."""
    broker, reports = _orphan_broker(
        tmp_path, closing_order=_closing_order(999), deal_fail=True)

    _recover(broker)
    broker._flush_external_interventions()

    assert 'c9' in _live_coids(broker)
    assert reports == []


def __test_recover_orphan_with_live_position_kept__(tmp_path):
    # A confirmed row whose position is STILL open must not be retired.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]))
    _open(tmp_path, broker)
    broker.store_ctx.upsert_order(
        'c10', symbol='EURUSD', side='buy', qty=qty, filled_qty=qty,
        state='confirmed', pine_entry_id='long', intent_key='ik10',
        exchange_order_id='222', extras={'order_id': '111', 'position_id': 222})

    _recover(broker)

    assert 'c10' in _live_coids(broker)


def __test_recover_promoted_row_not_orphan_retired__(tmp_path):
    # A row confirmed by THIS recovery pass (filled, position not yet in the
    # snapshot) must be skipped by the orphan pass via promoted_coids — else the
    # absent-from-snapshot position would be mistaken for an orphan and closed.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(positions=[]),  # snapshot has no positions yet
        order_res=_order_res(_order(
            order_id=111, coid='c11', volume=2000, executed=2000,
            position_id=222, status=_FILLED)),
        deal_res=_deal_res(_deal(deal_id=910, order_id=111, position_id=222,
                                 filled_volume=2000)),  # filled, NO close deal
    )
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c11', qty=qty)

    _recover(broker)

    row = broker.store_ctx.get_order('c11')
    assert row is not None
    assert row.state == 'confirmed'
    assert 'c11' in _live_coids(broker)


def __test_recover_working_order_filled_offline_not_orphan_retired__(tmp_path):
    # A confirmed LIMIT/STOP working order (only an order_id handle, position_id
    # None, unfilled) that FILLED while the bot was offline: cTrader sheds the
    # order from order[] and the new position[] entry carries no coid/orderId
    # link, so the order_id is absent from both snapshots. The orphan pass must
    # NOT retire it — a freshly filled position would otherwise be closed before
    # the runtime reconcile deal-history bridge can promote it.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]))
    _open(tmp_path, broker)
    broker.store_ctx.upsert_order(
        'c13', symbol='EURUSD', side='buy', qty=qty, filled_qty=0.0,
        state='confirmed', pine_entry_id='long', intent_key='ik13',
        exchange_order_id='111', extras={'order_id': '111', 'position_id': None})

    _recover(broker)

    row = broker.store_ctx.get_order('c13')
    assert row is not None
    assert row.state == 'confirmed'
    assert 'c13' in _live_coids(broker)


# === History window lower bound (dual-timestamp) ==========================

def __test_recover_lower_bound_uses_created_ts_anchor__(tmp_path):
    # A crash-before-ack submitted row carries no submitted_at_ms yet, so the
    # history window reaches back to the row's created_ts_ms (the dispatch-start
    # instant) minus the skew margin.
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c12', qty=qty)
    created_ms = broker.store_ctx.get_order('c12').created_ts_ms

    _recover(broker)

    order_reqs = [r for r in broker._wire.requests
                  if isinstance(r, _oa.ProtoOAOrderListReq)]
    assert order_reqs
    assert order_reqs[-1].fromTimestamp == created_ms - 60_000


# === Protection of a position a recovered fill opened =====================

def _bare_recovered_fill(tmp_path, *, hedged):
    """A MARKET entry filled while its answer was lost to a crash; the restart
    recovers the fill on a position that carries no protection."""
    qty = volume_to_units(2000)
    broker = _RecoveryBroker(
        _recon(positions=[_position(position_id=222, volume=2000)]),
        order_res=_order_res(_order(
            order_id=111, coid='c20', volume=2000, executed=2000,
            position_id=222, status=_FILLED)),
        deal_res=_deal_res(_deal(deal_id=920, order_id=111, position_id=222,
                                 filled_volume=2000)),
    )
    broker._hedging_enabled = hedged
    broker._symbol_rules = {'EURUSD': _SymbolRules(
        symbol_id=1, digits=5, min_volume=1000, step_volume=1000, max_volume=10_000_000)}
    _open(tmp_path, broker)
    _seed_submitted(broker, 'c20', qty=qty)
    _recover(broker)
    return broker


def _observe(broker, position) -> None:
    asyncio.run(broker._observe_protection_snapshot(
        {position.positionId: position}, read_started_at=time.time(),
        filled_positions=set()))


@pytest.mark.parametrize('hedged', [False, True])
def __test_recover_protection_added_to_a_bare_recovered_fill_is_reported__(tmp_path, hedged):
    """The bot left the position its recovered fill opened without protection,
    so a stop-loss someone adds to it afterwards is an intervention."""
    broker = _bare_recovered_fill(tmp_path, hedged=hedged)
    calls: list = []
    broker.quarantine_sink = lambda reason, context, *, kind: calls.append(
        (reason, context, kind))

    for _ in range(2):
        _observe(broker, _position(position_id=222, volume=2000))
    broker._flush_external_interventions()
    assert calls == []

    stopped = _position(position_id=222, volume=2000)
    stopped.stopLoss = 1.0
    for _ in range(2):
        _observe(broker, stopped)
    broker._flush_external_interventions()

    assert len(calls) == 1
    assert calls[0][1]['protection_key'].endswith(':222')

