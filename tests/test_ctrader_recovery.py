"""
@pyne
"""
import asyncio
import time
from types import SimpleNamespace

import pytest

import pynecore_ctrader.recovery as _recovery
from pynecore import lib
from pynecore.core.broker.disappearance import accept_missing_before_resume
from pynecore.core.broker.idempotency import KIND_ENTRY
from pynecore.core.broker.models import DispatchEnvelope, EntryIntent, OrderType
from pynecore.core.broker.position import BrokerPosition
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    create_entry_order_row,
)
from pynecore.core.broker.sync_engine import OrderSyncEngine
from pynecore.lib.strategy import Order, _order_type_entry
from pynecore.types.strategy import JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY

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



# === The bot's own venue state its journal lost ===========================

class _CancelWire(_RecoveryWire):
    """Answers ``ProtoOACancelOrderReq`` with the order's cancel, after the
    venue-write gate the real wire runs right before the send."""

    async def send_request(self, req, *, timeout=None, write_gate=None, **_kwargs):
        if isinstance(req, _oa.ProtoOACancelOrderReq):
            if write_gate is not None:
                write_gate()
            self.requests.append(req)
            return _oa.ProtoOAExecutionEvent(
                ctidTraderAccountId=999,
                executionType=_model.ProtoOAExecutionType.ORDER_CANCELLED,
                order=_model.ProtoOAOrder(
                    orderId=req.orderId,
                    orderStatus=_CANCELLED,
                    tradeData=_model.ProtoOATradeData(
                        symbolId=1, volume=2000, tradeSide=_model.ProtoOATradeSide.SELL)))
        return await super().send_request(req, timeout=timeout)


def _labelled_position(label, *, position_id=58367450, volume=2000):
    position = _position(position_id=position_id, volume=volume)
    position.tradeData.label = label
    return position


def _labelled_order(label, *, coid, order_id=777):
    order = _order(order_id=order_id, coid=coid, volume=2000)
    order.tradeData.label = label
    return order


def _events(broker, kind) -> list:
    return list(broker.store_ctx.iter_events_by_kind_since(kind, 0))


def _lost_journal_broker(tmp_path, *, positions=(), orders=()):
    broker = _RecoveryBroker(_recon())
    broker._wire = _CancelWire()
    broker.symbol = 'EURUSD'
    _open(tmp_path, broker)
    broker._recon = _recon(positions=positions, orders=orders)
    return broker


def __test_recover_untracked_position_with_the_run_s_label_is_adopted_as_own__(tmp_path):
    """The bot's LIMIT filled while it was down and its journal row was lost:
    the position carries the run's label, so it is the bot's own exposure."""
    broker = _lost_journal_broker(tmp_path)
    broker._recon = _recon(positions=[_labelled_position(broker.store_ctx.run_tag)])

    _recover(broker)

    assert 58367450 in broker._owned_position_ids()
    rows = list(broker.store_ctx.iter_live_orders())
    assert len(rows) == 1
    assert rows[0].filled_qty == pytest.approx(volume_to_units(2000))
    assert not (rows[0].extras or {}).get('adopted_startup')
    position = asyncio.run(broker.get_position('EURUSD'))
    assert position is not None and position.size == pytest.approx(volume_to_units(2000))
    assert 58367450 in broker._bare_positions
    assert len(_events(broker, 'ctrader_own_leg_recovered')) == 1


@pytest.mark.parametrize('label', ['other-run', ''])
def __test_recover_untracked_position_of_another_label_stays_another_party_s__(
        tmp_path, label):
    """Another label is another run's; no label is no run's, whose it is
    cannot be told: ERROR + audit, once per position."""
    broker = _lost_journal_broker(tmp_path, positions=[_labelled_position(label)])

    _recover(broker)
    _recover(broker)

    assert broker._owned_position_ids() == set()
    assert list(broker.store_ctx.iter_live_orders()) == []
    assert asyncio.run(broker.get_position('EURUSD')) is None
    assert len(_events(broker, 'ctrader_untracked_leg_unattributed')) == (0 if label else 1)


@pytest.mark.parametrize('by', ['label', 'coid'])
def __test_recover_untracked_working_order_of_the_run_is_cancelled__(tmp_path, by):
    """A working entry order of the run no journal row tracks would fill into
    exposure no intent stands behind: cancelled as the bot's own debris."""
    broker = _lost_journal_broker(tmp_path)
    run_tag = broker.store_ctx.run_tag
    order = (_labelled_order(run_tag, coid='') if by == 'label'
             else _labelled_order('', coid=f"{run_tag}-7rxpdapr-0mv9ub05c-e0"))
    broker._recon = _recon(orders=[order])

    _recover(broker)

    cancels = [r for r in broker._wire.requests if isinstance(r, _oa.ProtoOACancelOrderReq)]
    assert [r.orderId for r in cancels] == [777]
    audits = _events(broker, 'ctrader_own_order_debris')
    assert len(audits) == 1 and audits[0]['cancelled'] is True


def __test_recover_working_order_of_another_party_or_tracked_is_left_alone__(tmp_path):
    broker = _lost_journal_broker(tmp_path)
    run_tag = broker.store_ctx.run_tag
    _seed_submitted(broker, f"{run_tag}-tracked", qty=volume_to_units(2000))
    broker._recon = _recon(orders=[
        _labelled_order('other-run', coid='other-run-1', order_id=778),
        _labelled_order(run_tag, coid=f"{run_tag}-tracked", order_id=779)])

    _recover(broker)

    assert not [r for r in broker._wire.requests if isinstance(r, _oa.ProtoOACancelOrderReq)]
    assert _events(broker, 'ctrader_own_order_debris') == []


def __test_recover_own_debris_cancel_the_quarantine_refuses_is_audited__(tmp_path):
    """No write leaves while the quarantine holds: the order stays, ERROR + audit."""
    broker = _lost_journal_broker(tmp_path)
    broker.venue_writes_blocked = lambda: True
    broker._recon = _recon(orders=[_labelled_order(broker.store_ctx.run_tag, coid='')])

    _recover(broker)

    assert not [r for r in broker._wire.requests if isinstance(r, _oa.ProtoOACancelOrderReq)]
    audits = _events(broker, 'ctrader_own_order_debris')
    assert len(audits) == 1 and audits[0]['cancelled'] is False


# === In-run parked dispatch resolution =====================================

def _park_unknown(broker, coid, *, age_s) -> None:
    """Seed a ``disposition_unknown`` row + its engine park, ``age_s`` old."""
    _seed_submitted(broker, coid, qty=volume_to_units(2000))
    broker.store_ctx.set_order_state(coid, 'disposition_unknown')
    broker.store_ctx.record_park(coid, coid)
    broker.store_ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = created_ts_ms - ? WHERE client_order_id = ?",
        (int(age_s * 1000), coid),
    )
    broker.store_ctx._store._conn.commit()


def _resolve_parked(broker, *, passes=1) -> None:
    """Run resolver passes, each one lookup interval after the previous."""
    for _ in range(passes):
        asyncio.run(broker._resolve_parked_dispatches())
        real = broker._parked_resolver._clock
        broker._parked_resolver._clock = lambda clock=real: clock() + 31.0


def _resolution_of(broker, coid):
    for rec in broker.store_ctx.iter_pending_resolutions():
        if rec.coid == coid:
            return rec.resolution
    return None


def _order_list_requests(broker) -> list:
    return [r for r in broker._wire.requests if isinstance(r, _oa.ProtoOAOrderListReq)]


def __test_parked_absent_past_ttl_is_rejected_after_consecutive_misses__(tmp_path):
    # The send timed out and the venue never processed the order: neither the
    # live snapshot nor a complete order-history read carries the coid. Two
    # misses keep it parked, the third rejects it and tells the engine, which
    # then dispatches the intent again.
    broker = _RecoveryBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    _park_unknown(broker, 'k1', age_s=700)

    _resolve_parked(broker, passes=2)
    assert broker.store_ctx.get_order('k1').state == 'disposition_unknown'
    assert broker._parked_resolver.misses == {'k1': 2}
    assert _resolution_of(broker, 'k1') is None

    _resolve_parked(broker)
    row = broker.store_ctx.get_order('k1')
    assert row.state == 'rejected'
    assert 'k1' not in _live_coids(broker)
    assert _resolution_of(broker, 'k1') == 'rejected'
    assert broker._parked_resolver.misses == {}


def __test_parked_absent_younger_than_ttl_stays_parked__(tmp_path):
    # The order history indexes an order late: an absence is no verdict before
    # the TTL, however many lookups agree.
    broker = _RecoveryBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    _park_unknown(broker, 'k2', age_s=100)

    _resolve_parked(broker, passes=4)

    assert broker.store_ctx.get_order('k2').state == 'disposition_unknown'
    assert _resolution_of(broker, 'k2') is None


def __test_parked_within_grace_is_not_looked_up__(tmp_path):
    broker = _RecoveryBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    _park_unknown(broker, 'k3', age_s=5)

    asyncio.run(broker._resolve_parked_dispatches())

    assert _order_list_requests(broker) == []


def __test_parked_lookups_are_spaced_out__(tmp_path):
    # Every lookup costs an order-history request of the rolling quota: passes
    # five seconds apart ask once per interval, not once per pass.
    broker = _RecoveryBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    _park_unknown(broker, 'k4', age_s=700)

    for _ in range(3):
        asyncio.run(broker._resolve_parked_dispatches())

    assert len(_order_list_requests(broker)) == 1


def __test_parked_refused_by_the_venue_is_rejected_at_once__(tmp_path):
    # The order reached the venue and was rejected with nothing executed: no
    # miss streak and no TTL are needed.
    refused = _order(order_id=41, coid='k5', volume=2000, status=_REJECTED)
    broker = _RecoveryBroker(_recon(), order_res=_order_res(refused))
    _open(tmp_path, broker)
    _park_unknown(broker, 'k5', age_s=60)

    _resolve_parked(broker)

    assert broker.store_ctx.get_order('k5').state == 'rejected'
    assert _resolution_of(broker, 'k5') == 'rejected'


def __test_parked_filled_order_gets_its_refs_and_stays_parked__(tmp_path):
    # The order DID fill and the PUSH was lost: the resolver never rejects it.
    # It records the broker refs, so the reconcile snapshot recovers the fill.
    filled = _order(order_id=42, coid='k6', volume=2000, executed=2000,
                    position_id=222, status=_FILLED)
    broker = _RecoveryBroker(_recon(), order_res=_order_res(filled))
    _open(tmp_path, broker)
    _park_unknown(broker, 'k6', age_s=700)

    _resolve_parked(broker, passes=4)

    row = broker.store_ctx.get_order('k6')
    assert row.state == 'disposition_unknown'
    assert row.filled_qty == 0
    assert row.extras['order_id'] == '42'
    assert row.extras['position_id'] == 222
    assert 'recovered_inconclusive' not in row.extras
    assert _resolution_of(broker, 'k6') is None
    # Once the row carries its order id nothing is asked again.
    assert len(_order_list_requests(broker)) == 1


def __test_parked_resting_order_is_found_in_the_live_snapshot__(tmp_path):
    # A LIMIT that landed and rests in order[]: the live snapshot answers, the
    # order history is not read.
    resting = _order(order_id=43, coid='k7', volume=2000)
    broker = _RecoveryBroker(_recon(orders=[resting]), order_res=_order_res())
    _open(tmp_path, broker)
    _park_unknown(broker, 'k7', age_s=700)

    _resolve_parked(broker)

    row = broker.store_ctx.get_order('k7')
    assert row.state == 'disposition_unknown'
    assert row.extras['order_id'] == '43'
    assert _order_list_requests(broker) == []


def __test_parked_failed_history_read_is_no_evidence__(tmp_path):
    # A failed read neither advances the miss streak nor rejects, even past
    # the TTL.
    broker = _RecoveryBroker(_recon(), order_fail=True)
    _open(tmp_path, broker)
    _park_unknown(broker, 'k8', age_s=700)

    _resolve_parked(broker, passes=5)

    assert broker.store_ctx.get_order('k8').state == 'disposition_unknown'
    assert broker._parked_resolver.misses == {}
    assert _resolution_of(broker, 'k8') is None


def __test_submitted_row_is_mid_flight_and_untouched_in_run__(tmp_path):
    broker = _RecoveryBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    _seed_submitted(broker, 'k9', qty=volume_to_units(2000))

    _resolve_parked(broker, passes=3)

    assert _order_list_requests(broker) == []
    assert broker.store_ctx.get_order('k9').state == 'submitted'


def __test_reconcile_pass_resolves_parked_dispatches_first__(tmp_path, monkeypatch):
    broker = _RecoveryBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    steps: list[str] = []

    async def _resolver():
        steps.append('parked')

    async def _snapshot():
        steps.append('snapshot')
        return
        yield

    monkeypatch.setattr(broker, '_resolve_parked_dispatches', _resolver)
    monkeypatch.setattr(broker, '_reconcile_snapshot', _snapshot)

    async def _run():
        return [event async for event in broker._run_reconcile_pass()]

    assert asyncio.run(_run()) == []
    assert steps == ['parked', 'snapshot']


def __test_parked_landed_order_anchors_its_deal_window_at_the_dispatch__(tmp_path):
    # The order history reports the order's LAST change: a fill spread over
    # several executions changed last long after its first one. The deal
    # window the reconcile pass reads is anchored at the dispatch, so no
    # earlier execution falls out of it.
    filled = _order(order_id=44, coid='k10', volume=2000, executed=2000,
                    position_id=223, status=_FILLED)
    filled.utcLastUpdateTimestamp = int(time.time() * 1000)
    broker = _RecoveryBroker(_recon(), order_res=_order_res(filled))
    _open(tmp_path, broker)
    _park_unknown(broker, 'k10', age_s=700)

    _resolve_parked(broker)

    row = broker.store_ctx.get_order('k10')
    assert row.extras['order_id'] == '44'
    assert row.extras['submitted_at_ms'] <= row.created_ts_ms


def __test_parked_history_page_announcing_more_but_empty_is_no_evidence__(tmp_path):
    # ``hasMore`` on an empty page leaves the rest of the window unread: no
    # miss, however old the row.
    broker = _RecoveryBroker(_recon(), order_res=_order_res(has_more=True))
    _open(tmp_path, broker)
    _park_unknown(broker, 'k11', age_s=700)

    _resolve_parked(broker, passes=4)

    assert broker.store_ctx.get_order('k11').state == 'disposition_unknown'
    assert broker._parked_resolver.misses == {}
    assert _resolution_of(broker, 'k11') is None


def __test_parked_history_page_that_cannot_narrow_the_window_is_no_evidence__(tmp_path):
    # Every order of a truncated page shares its newest timestamp: the next
    # page asks the same window again, so the walk cannot reach the older
    # orders and proves nothing about them.
    other = _order(order_id=45, coid='other', volume=2000)
    other.utcLastUpdateTimestamp = int(time.time() * 1000) - 10_000
    broker = _RecoveryBroker(_recon(), order_res=_order_res(other, has_more=True))
    _open(tmp_path, broker)
    _park_unknown(broker, 'k12', age_s=700)

    _resolve_parked(broker, passes=4)

    assert broker.store_ctx.get_order('k12').state == 'disposition_unknown'
    assert broker._parked_resolver.misses == {}
    assert len(_order_list_requests(broker)) == 4 * 2


def __test_parked_history_page_cut_at_the_window_start_is_no_evidence__(tmp_path):
    # A truncated page whose oldest order sits exactly at the window's lower
    # bound: the orders sharing that timestamp that the page cut off stay
    # unread, so the walk proves nothing about them.
    broker = _RecoveryBroker(_recon())
    _open(tmp_path, broker)
    _park_unknown(broker, 'k13', age_s=700)
    other = _order(order_id=46, coid='other', volume=2000)
    other.utcLastUpdateTimestamp = broker._recovery_lower_bound_ms(
        broker.store_ctx.get_order('k13'))
    broker._wire._order_res = _order_res(other, has_more=True)

    _resolve_parked(broker, passes=4)

    assert broker.store_ctx.get_order('k13').state == 'disposition_unknown'
    assert broker._parked_resolver.misses == {}
    assert _resolution_of(broker, 'k13') is None


# === Restart: an inherited dispatch holds its intent until resolved ========

_RESTART_BAR_TS = 1_700_000_000_000


class _RestartBroker(_RecoveryBroker):
    """Recovery broker whose order sends are recorded and accepted."""

    def __init__(self, recon, **kwargs):
        super().__init__(recon, **kwargs)
        self._symbol_rules = {'EURUSD': _SymbolRules(
            symbol_id=1, digits=5, min_volume=1000, step_volume=1000,
            max_volume=10_000_000)}
        self.sent: list = []

    async def _get_symbol_rules(self, symbol):
        return self._symbol_rules['EURUSD']

    async def _dispatch_order(self, req, *, coid, context, predecessor_cancel_ids=None):
        self.sent.append(req)
        event = _oa.ProtoOAExecutionEvent(
            executionType=_model.ProtoOAExecutionType.ORDER_ACCEPTED)
        event.order.CopyFrom(_model.ProtoOAOrder(
            orderId=900 + len(self.sent), clientOrderId=req.clientOrderId,
            orderType=req.orderType,
            orderStatus=_model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED,
            tradeData=_model.ProtoOATradeData(
                symbolId=1, volume=req.volume, tradeSide=req.tradeSide)))
        return event


def _crashed_entry_send(tmp_path, *, limit=None, state='submitted') -> str:
    """An earlier process wrote the entry's persist-first row, then crashed.

    Its envelope anchor stays and the row stays pending. In ``submitted``
    no park was written: the crash came before the venue's answer. In
    ``disposition_unknown`` the answer was lost and the dispatch parked.

    :return: The client order id of the crashed send.
    """
    store = BrokerStore(tmp_path / "broker.sqlite", plugin_name='ctrader')
    ctx = store.open_run(RunIdentity(strategy_id="recov", symbol="EURUSD", timeframe="60",
                                     account_id="recov-account"),
                         script_source="// recov")
    intent = EntryIntent(pine_id='L', symbol='EURUSD', side='buy', qty=20.0,
                         order_type=OrderType.LIMIT if limit else OrderType.MARKET,
                         limit=limit)
    coid = DispatchEnvelope(intent=intent, run_tag=ctx.run_tag, bar_ts_ms=_RESTART_BAR_TS,
                            retry_seq=0).client_order_id(KIND_ENTRY)
    ctx.record_envelope('L', _RESTART_BAR_TS, 0)
    create_entry_order_row(
        ctx, coid=coid, symbol='EURUSD', side='buy', qty=20.0, intent_key='L',
        pine_entry_id='L', kind=ENTRY_KIND_WORKING if limit else ENTRY_KIND_POSITION,
        order_type='limit' if limit else 'market')
    if state != 'submitted':
        ctx.set_order_state(coid, state)
        ctx.record_park(coid, 'L')
    store.close()
    return coid


def _restarted(tmp_path, monkeypatch, recon, *, limit=None):
    """The restarted process: startup recovery, then the engine, the entry re-emitted."""
    monkeypatch.setattr(lib, '_script', SimpleNamespace(initial_capital=1_000_000.0))
    broker = _RestartBroker(recon, order_res=_order_res())
    _open(tmp_path, broker)
    _recover(broker)
    engine = OrderSyncEngine(broker=broker, position=BrokerPosition(), symbol='EURUSD',
                             run_tag=broker.store_ctx.run_tag, mintick=0.00001,
                             store_ctx=broker.store_ctx)
    engine._position.entry_orders['L'] = Order('L', 20.0, order_type=_order_type_entry,
                                               limit=limit)
    return broker, engine


def _new_orders(broker) -> list:
    return [req for req in broker.sent if isinstance(req, _oa.ProtoOANewOrderReq)]


def __test_restart_holds_an_entry_whose_inherited_send_is_unresolved__(tmp_path, monkeypatch):
    # The send may have landed (the order history indexes it late), so the
    # entry the script emits again must not go out a second time under the
    # same clientOrderId: cTrader would open a second position. Once the
    # venue conclusively never had the order, the entry goes out under a
    # fresh id.
    coid = _crashed_entry_send(tmp_path)
    broker, engine = _restarted(tmp_path, monkeypatch, _recon())
    assert broker.store_ctx.get_order(coid).state == 'submitted'

    engine.sync(_RESTART_BAR_TS + 3_600_000)
    engine.sync(_RESTART_BAR_TS + 7_200_000)

    assert _new_orders(broker) == []
    assert coid in {rec.coid for rec in broker.store_ctx.replay()[1].values()}

    broker.store_ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = created_ts_ms - 700000 WHERE client_order_id = ?",
        (coid,))
    broker.store_ctx._store._conn.commit()
    _resolve_parked(broker, passes=3)
    assert broker.store_ctx.get_order(coid).state == 'rejected'

    engine.sync(_RESTART_BAR_TS + 10_800_000)

    sent = _new_orders(broker)
    assert len(sent) == 1
    assert sent[0].clientOrderId != coid


def __test_restart_adopts_an_inherited_send_found_working__(tmp_path, monkeypatch):
    # The crashed send did land and rests on the venue, but neither venue view
    # listed it at startup. The resolver finds it later; the engine adopts it
    # from its park instead of sending the entry again.
    coid = _crashed_entry_send(tmp_path, limit=1.1)
    broker, engine = _restarted(tmp_path, monkeypatch, _recon(), limit=1.1)

    engine.sync(_RESTART_BAR_TS + 3_600_000)
    assert _new_orders(broker) == []

    broker._recon = _recon(orders=[_order(order_id=77, coid=coid, volume=2000)])
    broker.store_ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = created_ts_ms - 60000 WHERE client_order_id = ?",
        (coid,))
    broker.store_ctx._store._conn.commit()
    _resolve_parked(broker)
    assert broker.store_ctx.get_order(coid).extras['order_id'] == '77'

    engine.sync(_RESTART_BAR_TS + 7_200_000)
    engine.sync(_RESTART_BAR_TS + 10_800_000)

    assert _new_orders(broker) == []
    assert engine.order_mapping['L'] == ['77']
    assert coid not in broker.store_ctx.replay()[1]


def _reconcile_events(broker) -> list:
    async def _run():
        broker._reconcile_pass_deadline = asyncio.get_running_loop().time() + 30.0
        return [event async for event in broker._reconcile_snapshot()]
    return asyncio.run(_run())


@pytest.mark.parametrize('state', ['submitted', 'disposition_unknown'])
def __test_an_inherited_send_found_filled_on_an_adopted_position_is_booked_once__(
        tmp_path, state):
    # The crashed MARKET send did fill, but neither venue view had indexed the
    # order at startup: the open position carries the run's label, so startup
    # adopted it as the bot's own leg. When the resolver later finds the order,
    # that position already stands in the journal and in the engine's book:
    # the inherited row takes it over, and no second fill is emitted.
    coid = _crashed_entry_send(tmp_path, state=state)
    broker = _RestartBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    broker.symbol = 'EURUSD'
    broker._recon = _recon(positions=[_labelled_position(broker.store_ctx.run_tag)])
    _recover(broker)
    broker._apply_adoption_baseline(broker._recon)
    assert broker.store_ctx.get_order(coid).state == state
    assert len(list(broker.store_ctx.iter_live_orders())) == 2

    filled = _order(order_id=55, coid=coid, volume=2000, executed=2000,
                    position_id=58367450, status=_FILLED)
    filled.orderType = _model.ProtoOAOrderType.MARKET
    broker._wire._order_res = _order_res(filled)
    broker._wire._deal_res = _deal_res(_deal(deal_id=5501, order_id=55,
                                             position_id=58367450, filled_volume=2000))
    broker.store_ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = created_ts_ms - 60000 WHERE client_order_id = ?",
        (coid,))
    broker.store_ctx._store._conn.commit()
    _resolve_parked(broker)
    events = _reconcile_events(broker)

    fills = [e for e in events if e.event_type in ('filled', 'partial')]
    assert fills == []
    live = list(broker.store_ctx.iter_live_orders())
    owners = [r for r in live if str((r.extras or {}).get('position_id')) == '58367450'
              or r.exchange_order_id == '58367450']
    assert sum(r.filled_qty for r in owners) == pytest.approx(volume_to_units(2000))
    assert broker.store_ctx.find_by_ref('position_id', '58367450').client_order_id in {
        r.client_order_id for r in owners}


def _adopted_and_inherited(tmp_path, *, position_volume=2000, position_ts=0,
                           adopted_volume=None, before_adoption=None, short=False):
    """An inherited MARKET buy send and the labelled position startup adopted.

    :param short: The position is a short one.
    :param position_ts: The position's last venue change in the startup
        snapshot; ``0`` sends none.
    :param adopted_volume: The position's volume in the snapshot the engine
        adopts, when it grew after the startup recovery's snapshot.
    :param before_adoption: Called with the broker between the startup
        recovery and the engine's adoption.
    :return: The broker after startup recovery and the engine's adoption,
        the inherited coid and the adopted row's coid.
    """
    coid = _crashed_entry_send(tmp_path, state='disposition_unknown')
    broker = _RestartBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    broker.symbol = 'EURUSD'
    position = _labelled_position(broker.store_ctx.run_tag, volume=position_volume)
    position.utcLastUpdateTimestamp = position_ts
    if short:
        position.tradeData.tradeSide = _model.ProtoOATradeSide.SELL
    broker._recon = _recon(positions=[position])
    _recover(broker)
    if before_adoption is not None:
        before_adoption(broker)
    if adopted_volume is not None:
        grown = _labelled_position(broker.store_ctx.run_tag, volume=adopted_volume)
        grown.utcLastUpdateTimestamp = position_ts
        broker._recon = _recon(orders=broker._recon.order, positions=[grown])
    broker._apply_adoption_baseline(broker._recon)
    adopted = next(r.client_order_id for r in broker.store_ctx.iter_live_orders()
                   if (r.extras or {}).get('recovered_own_leg'))
    return broker, coid, adopted


def _filled_into_adopted(broker, coid) -> None:
    """The venue now lists ``coid``'s MARKET order filled into the adopted position."""
    filled = _order(order_id=55, coid=coid, volume=2000, executed=2000,
                    position_id=58367450, status=_FILLED)
    filled.orderType = _model.ProtoOAOrderType.MARKET
    broker._wire._order_res = _order_res(filled)
    broker._wire._deal_res = _deal_res(_deal(deal_id=5501, order_id=55,
                                             position_id=58367450, filled_volume=2000))
    broker.store_ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = created_ts_ms - 60000 WHERE client_order_id = ?",
        (coid,))
    broker.store_ctx._store._conn.commit()


def _assert_takeover_held(broker, coid, adopted, adopted_qty) -> None:
    """The inherited dispatch stays parked and the adopted leg keeps its size."""
    assert broker.store_ctx.get_order(coid).state == 'disposition_unknown'
    assert broker.store_ctx.get_order(coid).filled_qty == 0.0
    donor = broker.store_ctx.get_order(adopted)
    assert donor.closed_ts_ms is None
    assert donor.filled_qty == pytest.approx(adopted_qty)
    assert not {5501, 5502} & broker._seen_deal_ids
    assert _events(broker, 'ctrader_adopted_leg_taken_over') == []
    assert len(_events(broker, 'ctrader_adopted_leg_takeover_held')) == 1


def __test_an_inherited_send_filled_beyond_the_adopted_leg_stays_parked__(tmp_path):
    # The inherited order filled more before the adoption than the adopted leg
    # holds. Part of it may have been closed before the restart, but the leg's
    # shortfall does not prove which deals it covers: the dispatch stays
    # parked, its entry held, and nothing is moved, retired or booked. The
    # held takeover is reported once, not on every lookup.
    broker, coid, adopted = _adopted_and_inherited(tmp_path, position_volume=1000)
    _filled_into_adopted(broker, coid)

    _resolve_parked(broker, passes=2)

    _assert_takeover_held(broker, coid, adopted, volume_to_units(1000))


def __test_an_inherited_send_s_fill_after_the_adoption_is_booked_as_a_fill__(tmp_path):
    # The inherited order filled in two executions, one before the snapshot the
    # engine adopted and one after it. Only the first is in the adopted size:
    # the row takes that over without an event, and the reconcile pass books
    # the second as the fill the engine never saw.
    adopted_ts = 1_700_000_000_000
    broker, coid, adopted = _adopted_and_inherited(
        tmp_path, position_volume=1000, position_ts=adopted_ts)
    _filled_into_adopted(broker, coid)
    after = _deal(deal_id=5502, order_id=55, position_id=58367450, filled_volume=1000,
                  price=1.2)
    after.executionTimestamp = adopted_ts + 50_000
    broker._wire._deal_res = _deal_res(
        _deal(deal_id=5501, order_id=55, position_id=58367450, filled_volume=1000), after)

    _resolve_parked(broker)

    row = broker.store_ctx.get_order(coid)
    assert row.state == 'confirmed'
    assert row.filled_qty == pytest.approx(volume_to_units(1000))
    assert broker._row_exposure(row) == pytest.approx(volume_to_units(1000))
    assert broker.store_ctx.get_order(adopted).closed_ts_ms is not None
    assert 5501 in broker._seen_deal_ids
    assert 5502 not in broker._seen_deal_ids

    position = _labelled_position(broker.store_ctx.run_tag, volume=2000)
    broker._recon = _recon(positions=[position])
    events = _reconcile_events(broker)

    fills = [e for e in events if e.event_type in ('filled', 'partial')]
    assert len(fills) == 1
    assert fills[0].fill_qty == pytest.approx(volume_to_units(1000))
    assert fills[0].fill_price == pytest.approx(1.2)
    row = broker.store_ctx.get_order(coid)
    assert row.filled_qty == pytest.approx(volume_to_units(2000))
    assert 5502 in broker._seen_deal_ids
    assert _reconcile_events(broker) == []


def __test_an_inherited_send_filled_only_after_the_adoption_takes_nothing_over__(tmp_path):
    # Every execution of the inherited order came after the snapshot the
    # engine adopted: the adopted leg keeps its size and the fill is booked.
    adopted_ts = 1_700_000_000_000
    broker, coid, adopted = _adopted_and_inherited(
        tmp_path, position_volume=1000, position_ts=adopted_ts - 1)
    _filled_into_adopted(broker, coid)

    _resolve_parked(broker)

    donor = broker.store_ctx.get_order(adopted)
    assert donor.closed_ts_ms is None
    assert donor.filled_qty == pytest.approx(volume_to_units(1000))
    assert broker.store_ctx.get_order(coid).extras['order_id'] == '55'
    assert 5501 not in broker._seen_deal_ids
    assert _events(broker, 'ctrader_adopted_leg_taken_over') == []

    broker._recon = _recon(positions=[_labelled_position(broker.store_ctx.run_tag,
                                                         volume=3000)])
    fills = [e for e in _reconcile_events(broker) if e.event_type in ('filled', 'partial')]
    assert len(fills) == 1
    assert fills[0].fill_qty == pytest.approx(volume_to_units(2000))


def _filled_in_two_deals(broker, coid) -> None:
    """``coid``'s MARKET order filled into the adopted position in two deals."""
    _filled_into_adopted(broker, coid)
    broker._wire._deal_res = _deal_res(
        _deal(deal_id=5501, order_id=55, position_id=58367450, filled_volume=1000),
        _deal(deal_id=5502, order_id=55, position_id=58367450, filled_volume=1000))


def __test_an_inherited_send_s_fill_between_recovery_and_adoption_is_owned_once__(tmp_path):
    # The startup recovery saw the position with the order's first deal only;
    # the second came before the snapshot the engine adopted. The adopted leg
    # is raised to the adopted size, so the inherited row takes both deals
    # over: nothing is retired as closed, and no pass books either again.
    broker, coid, adopted = _adopted_and_inherited(
        tmp_path, position_volume=1000, adopted_volume=2000)
    assert broker.store_ctx.get_order(adopted).filled_qty == pytest.approx(
        volume_to_units(2000))
    assert len(_events(broker, 'ctrader_own_leg_grown')) == 1
    _filled_in_two_deals(broker, coid)

    _resolve_parked(broker)

    row = broker.store_ctx.get_order(coid)
    assert row.state == 'confirmed'
    assert row.filled_qty == pytest.approx(volume_to_units(2000))
    assert broker._row_exposure(row) == pytest.approx(volume_to_units(2000))
    assert broker.store_ctx.get_order(adopted).closed_ts_ms is not None
    assert {5501, 5502} <= broker._seen_deal_ids

    broker._recon = _recon(positions=[_labelled_position(broker.store_ctx.run_tag,
                                                         volume=2000)])
    events = _reconcile_events(broker)

    assert [e for e in events if e.event_type in ('filled', 'partial', 'closed')] == []
    assert broker._row_exposure(broker.store_ctx.get_order(coid)) == pytest.approx(
        volume_to_units(2000))


def __test_an_unattributed_growth_keeps_the_inherited_send_parked__(tmp_path):
    # Another entry row of the symbol carries an order neither the adoption
    # snapshot nor any position link accounts for: the growth may be its fill,
    # so the adopted leg keeps its size. Its shortfall against the inherited
    # order's adopted deals then proves no close: the dispatch stays parked
    # and nothing is retired, booked or seeded.
    def other_entry(broker):
        create_entry_order_row(
            broker.store_ctx, coid='other-entry', symbol='EURUSD', side='buy', qty=10.0,
            intent_key='S', pine_entry_id='S', kind=ENTRY_KIND_WORKING, order_type='limit')
        broker.store_ctx.upsert_order('other-entry', state='confirmed',
                                      extras={'kind': ENTRY_KIND_WORKING, 'order_id': '991'})

    broker, coid, adopted = _adopted_and_inherited(
        tmp_path, position_volume=1000, adopted_volume=2000, before_adoption=other_entry)
    assert broker.store_ctx.get_order(adopted).filled_qty == pytest.approx(
        volume_to_units(1000))
    assert len(_events(broker, 'ctrader_own_leg_growth_unattributed')) == 1
    _filled_in_two_deals(broker, coid)

    _resolve_parked(broker)

    _assert_takeover_held(broker, coid, adopted, volume_to_units(1000))


def __test_a_growth_beside_an_opposite_side_row_keeps_the_inherited_send_parked__(tmp_path):
    # On a NETTING account a sell row of the run is linked to the adopted
    # long position, and the position grew between the startup recovery and
    # the adoption. How much of the long side the adopted leg stands for
    # cannot be told from the two snapshots: the leg keeps its size, and the
    # inherited buy, which filled more than the leg holds, stays parked
    # instead of retiring the difference as closed.
    def opposite_entry(broker):
        create_entry_order_row(
            broker.store_ctx, coid='short-entry', symbol='EURUSD', side='sell', qty=5.0,
            intent_key='S', pine_entry_id='S', kind=ENTRY_KIND_POSITION, order_type='market')
        broker.store_ctx.upsert_order('short-entry', state='confirmed', filled_qty=5.0,
                                      extras={'kind': ENTRY_KIND_POSITION, 'order_id': '992',
                                              'position_id': '58367450'})

    broker, coid, adopted = _adopted_and_inherited(
        tmp_path, position_volume=1000, adopted_volume=1500, before_adoption=opposite_entry)
    assert broker.store_ctx.get_order(adopted).filled_qty == pytest.approx(
        volume_to_units(1000))
    assert _events(broker, 'ctrader_own_leg_grown') == []
    assert len(_events(broker, 'ctrader_own_leg_growth_unattributed')) == 1
    _filled_in_two_deals(broker, coid)

    _resolve_parked(broker)

    _assert_takeover_held(broker, coid, adopted, volume_to_units(1000))
    row = broker.store_ctx.get_order(coid)
    assert JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY not in (row.extras or {})


def __test_an_inherited_send_against_the_adopted_leg_s_side_stays_parked__(tmp_path):
    # The inherited buy filled into a position startup adopted as a short leg.
    # Moving the leg's exposure to a buy row would flip its sign: the dispatch
    # stays parked and the leg keeps its size.
    broker, coid, adopted = _adopted_and_inherited(tmp_path, short=True)
    assert broker.store_ctx.get_order(adopted).side == 'sell'
    _filled_into_adopted(broker, coid)
    broker._wire._deal_res = _deal_res(_deal(deal_id=5501, order_id=55,
                                             position_id=58367450, filled_volume=1000))

    _resolve_parked(broker)

    _assert_takeover_held(broker, coid, adopted, volume_to_units(2000))


def __test_a_parked_send_of_this_process_never_takes_an_adopted_leg_over__(tmp_path):
    # On a NETTING account a send of the running process fills into the
    # position startup adopted. Its fill was never part of that adoption, so
    # the adopted leg keeps its size and the send's fill is left to the fill
    # recovery that books it.
    broker = _RestartBroker(_recon(), order_res=_order_res())
    _open(tmp_path, broker)
    broker.symbol = 'EURUSD'
    broker._recon = _recon(positions=[_labelled_position(broker.store_ctx.run_tag)])
    _recover(broker)
    adopted = next(r for r in broker.store_ctx.iter_live_orders()
                   if (r.extras or {}).get('recovered_own_leg'))
    intent = EntryIntent(pine_id='L', symbol='EURUSD', side='buy', qty=20.0,
                         order_type=OrderType.MARKET)
    coid = DispatchEnvelope(intent=intent, run_tag=broker.store_ctx.run_tag,
                            bar_ts_ms=_RESTART_BAR_TS, retry_seq=0).client_order_id(KIND_ENTRY)
    create_entry_order_row(
        broker.store_ctx, coid=coid, symbol='EURUSD', side='buy', qty=20.0, intent_key='L',
        pine_entry_id='L', kind=ENTRY_KIND_POSITION, order_type='market')
    broker.store_ctx.set_order_state(coid, 'disposition_unknown')
    assert not broker.store_ctx.is_inherited_dispatch(coid)
    _filled_into_adopted(broker, coid)

    _resolve_parked(broker)

    still = broker.store_ctx.get_order(adopted.client_order_id)
    assert still.closed_ts_ms is None
    assert still.filled_qty == adopted.filled_qty
    assert broker.store_ctx.get_order(coid).extras['order_id'] == '55'
    assert broker.store_ctx.get_order(coid).filled_qty == 0.0
    assert 5501 not in broker._seen_deal_ids
    assert _events(broker, 'ctrader_adopted_leg_taken_over') == []


def __test_a_failed_adopted_leg_takeover_changes_nothing_and_is_retried__(
        tmp_path, monkeypatch):
    # The transfer fails after the donor's reduction was written: the whole
    # transfer rolls back, the order's deals stay unseeded, and the next
    # lookup repeats it once, so the adopted size is given up exactly once.
    broker, coid, adopted = _adopted_and_inherited(tmp_path)
    _filled_into_adopted(broker, coid)
    original = broker._confirm_recovered_entry
    calls = []

    def failing_once(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("store write failed")
        return original(*args, **kwargs)

    monkeypatch.setattr(broker, '_confirm_recovered_entry', failing_once)

    _resolve_parked(broker)

    donor = broker.store_ctx.get_order(adopted)
    assert donor.closed_ts_ms is None
    assert donor.filled_qty == pytest.approx(volume_to_units(2000))
    assert broker.store_ctx.get_order(coid).state == 'disposition_unknown'
    assert 5501 not in broker._seen_deal_ids

    _resolve_parked(broker)

    assert broker.store_ctx.get_order(adopted).closed_ts_ms is not None
    row = broker.store_ctx.get_order(coid)
    assert row.state == 'confirmed'
    assert row.filled_qty == pytest.approx(volume_to_units(2000))
    assert 5501 in broker._seen_deal_ids
    assert len(_events(broker, 'ctrader_adopted_leg_taken_over')) == 1
