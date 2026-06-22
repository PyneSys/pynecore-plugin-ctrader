"""Live order-event stream mix-in for the cTrader Open API plugin.

Implements :meth:`watch_orders` (``watch_orders = NATIVE``): the persistent
connection's ``ProtoOAExecutionEvent`` PUSH channel, demultiplexed onto
``_exec_events`` by the base event router, is translated into PyneCore
:class:`~pynecore.core.broker.models.OrderEvent` objects with their Pine
identity reverse-mapped from the BrokerStore order-ref index.

The PUSH channel is the primary order-event source; the periodic
``ProtoOAReconcileReq`` snapshot diff (:class:`~pynecore_ctrader.reconcile._ReconcileMixin`)
that gap-fills events lost during a disconnect / restart is driven from this
same loop — :meth:`watch_orders` piggybacks the reconcile cadence onto its
``execq.get()`` wait rather than running a separate background task. Events are
never silently dropped (the queue is unbounded; a backlog watermark only warns).
"""
import asyncio
import logging
from time import time as epoch_time
from typing import AsyncIterator

from pynecore.core.broker.exceptions import BrokerManualInterventionError
from pynecore.core.broker.models import (
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
)

from ._base import _CTraderBase
from .helpers import money_value, volume_to_units
from .messages import OpenApiMessages_pb2 as _oa
from .messages import OpenApiModelMessages_pb2 as _model
from .state import _ORDER_STATUS_MAP, _ORDER_TYPE_MAP
from .wire import CTraderConnectionError

logger = logging.getLogger(__name__)

#: ``ProtoOAExecutionType`` -> :class:`OrderEvent` ``event_type``.
_EXEC_TYPE_TO_EVENT = {
    _model.ProtoOAExecutionType.ORDER_ACCEPTED: 'created',
    _model.ProtoOAExecutionType.ORDER_FILLED: 'filled',
    _model.ProtoOAExecutionType.ORDER_PARTIAL_FILL: 'partial',
    _model.ProtoOAExecutionType.ORDER_CANCELLED: 'cancelled',
    _model.ProtoOAExecutionType.ORDER_REJECTED: 'rejected',
    _model.ProtoOAExecutionType.ORDER_EXPIRED: 'cancelled',
}

#: ``qsize`` above which a consumer backlog is warned about (never dropped).
_BACKLOG_WATERMARK = 1000

#: Reconcile-snapshot cadence (seconds). The PUSH stream is the primary source;
#: the reconcile pass is a gap-filler, so the cadence is decoupled from any
#: disappearance grace window. Engine-ordered: the pass runs when the engine
#: next resumes the ``watch_orders`` iterator, not on a hard wall clock.
_RECONCILE_CADENCE_S = 5.0


class _EventStreamMixin(_CTraderBase):
    """Order-event PUSH stream: ``ProtoOAExecutionEvent`` -> ``OrderEvent``."""

    async def watch_orders(self) -> AsyncIterator[OrderEvent]:
        """Stream order status updates, driving the reconcile cadence inline.

        Yields one :class:`OrderEvent` per relevant ``ProtoOAExecutionEvent``,
        with Pine identity filled in from the BrokerStore ref index. Execution
        types that carry no order-lifecycle meaning (swaps, deposits) are
        skipped; nothing is dropped silently.

        The reconcile-snapshot pass is piggybacked onto this loop instead of a
        separate background task: each iteration blocks on ``execq.get()`` only
        until the next reconcile deadline (``asyncio.wait_for`` — cancellation
        on timeout does NOT consume a queued event), then runs the pass. The
        deadline is checked at the TOP of the loop before blocking, so a busy
        PUSH stream cannot starve the reconcile; and the next deadline is set
        AFTER the pass completes (no catch-up burst when one pass runs long).
        """
        execq = self._exec_events
        if execq is None:
            raise CTraderConnectionError("live event router not started")
        warned = False
        loop = asyncio.get_running_loop()
        next_reconcile_at = loop.time() + _RECONCILE_CADENCE_S
        while True:
            now = loop.time()
            if now >= next_reconcile_at:
                async for event in self._run_reconcile_pass():
                    yield event
                next_reconcile_at = loop.time() + _RECONCILE_CADENCE_S
                continue
            try:
                message = await asyncio.wait_for(
                    execq.get(), timeout=next_reconcile_at - now,
                )
            except asyncio.TimeoutError:
                continue
            if not warned and execq.qsize() > _BACKLOG_WATERMARK:
                logger.warning(
                    "cTrader order-event backlog above %d; consumer is lagging",
                    _BACKLOG_WATERMARK,
                )
                warned = True
            event = self._translate_exec_event(message)
            if event is not None:
                yield event

    async def _run_reconcile_pass(self) -> AsyncIterator[OrderEvent]:
        """Run one reconcile-snapshot pass, isolating transient failures.

        Two sub-passes in order: :meth:`_reconcile_snapshot` (gap-fill missed
        fills, stamp / clear ``missing_pending_since``), then
        :meth:`_emit_unexpected_cancellations` (retire rows missing past the
        grace window). A recoverable reconcile error — a transport hiccup on
        the snapshot or the deal-history bridge request — is logged and
        swallowed so the PUSH stream (the primary order-event source) is never
        torn down by the gap-filler. A deliberate halt from the
        disappearance-grace tracker (``on_unexpected_cancel='stop'`` ->
        :class:`BrokerManualInterventionError`) is NOT a transient failure: it
        propagates so the engine performs its graceful stop — swallowing it
        would strand the strategy out of sync. ``asyncio.CancelledError`` is a
        ``BaseException`` and so still propagates for a clean teardown.

        The transient-failure warning is rate-limited on the failure streak:
        the pass runs every ~5 s, so a plain network outage would otherwise
        repeat the same line 12×/minute for its whole duration. The first
        failure of a streak warns, then one reminder every 60 failures
        (~5 minutes); the rest log at DEBUG. The line that closes the streak
        ("recovered after N failed passes") is WARNING again so the recovery
        is visible at the same console level as the failure it answers.
        """
        try:
            async for event in self._reconcile_snapshot():
                yield event
            async for event in self._emit_unexpected_cancellations():
                yield event
        except BrokerManualInterventionError:
            raise
        except Exception as exc:  # noqa: BLE001 - gap-filler must not kill the PUSH stream
            self._reconcile_fail_streak += 1
            streak = self._reconcile_fail_streak
            log = (logger.warning if streak == 1 or streak % 60 == 0
                   else logger.debug)
            log("cTrader reconcile pass failed (transient, %d consecutive): %s",
                streak, exc)
        else:
            if self._reconcile_fail_streak:
                logger.warning(
                    "cTrader reconcile recovered after %d failed pass(es)",
                    self._reconcile_fail_streak,
                )
                self._reconcile_fail_streak = 0

    def _translate_exec_event(self, message) -> OrderEvent | None:
        """Translate one execution / order-error message into an OrderEvent.

        :param message: A ``ProtoOAExecutionEvent`` or ``ProtoOAOrderErrorEvent``
            taken off the execution queue.
        :return: The mapped :class:`OrderEvent`, or ``None`` when the message
            carries no order-lifecycle meaning.
        """
        if isinstance(message, _oa.ProtoOAOrderErrorEvent):
            return self._order_error_to_event(message)
        if not isinstance(message, _oa.ProtoOAExecutionEvent):
            return None
        event_type = _EXEC_TYPE_TO_EVENT.get(message.executionType)
        if event_type is None:
            return None

        order = message.order
        # Snapshot the entry fill cursor BEFORE recovery / fill processing
        # mutates it (e.g. _recover_parked_entry_by_coid sets filled_qty as a
        # side effect), so the ENTRY-fill suppress below can tell a reconcile
        # pass that already counted this cumulative — cursor ahead at event
        # entry — from this event's own first-time recording.
        prior_entry_filled = self._entry_fill_cursor(order)
        deal = message.deal if message.HasField('deal') else None
        if deal is not None and deal.dealId in self._seen_deal_ids:
            # The dispatch path re-injects the correlated fill that
            # ``send_request`` consumed; cTrader may ALSO push an uncorrelated
            # copy. Both carry the same ``dealId`` — drop the second. The id is
            # recorded below, only for a fill we will actually apply.
            return None
        exch_order = self._order_from_proto(order)

        fill_price: float | None = None
        fill_qty: float | None = None
        fill_id: str | None = None
        fee = 0.0
        fee_currency = ''
        timestamp = epoch_time()
        if deal is not None:
            fill_price = deal.executionPrice or None
            fill_qty = volume_to_units(deal.filledVolume)
            # ``dealId`` is the broker-native per-execution id. It is canonical
            # across the PUSH copy and the correlated dispatch-response copy of
            # the same fill (both carry it; ``_seen_deal_ids`` above drops the
            # second locally), so the engine's duplicate-fill gate keys on it as
            # a final backstop if the local dedup ever misses (e.g. ``deal``
            # delivered without passing this branch). The cumulative-only
            # reconcile/bridge paths cannot reproduce a single dealId per emitted
            # slice and intentionally leave ``fill_id`` unset, relying on the
            # persisted ``filled_qty`` cursor instead.
            fill_id = str(deal.dealId)
            # Remember the dealId only for a fill we will actually apply, so a
            # malformed delivery (no volume / price, which record_fill ignores)
            # does not burn the id and drop a corrected redelivery carrying the
            # same dealId. Mirrors the engine's _is_duplicate_fill gate.
            if (fill_qty or 0.0) > 0.0 and (fill_price or 0.0) > 0.0:
                self._seen_deal_ids.add(deal.dealId)
            fee = money_value(deal.commission, deal.moneyDigits)
            if deal.executionTimestamp:
                timestamp = deal.executionTimestamp / 1000.0
        elif order.executionPrice:
            fill_price = order.executionPrice

        pine_id, from_entry, leg_type = self._resolve_identity(order, deal)
        if (event_type in ('filled', 'partial') and leg_type is None
                and self.store_ctx is not None):
            # The order/position ref index could not reverse-map this fill.
            # Before treating it as external, try the deterministic
            # ``clientOrderId`` echo: a MARKET entry whose dispatch parked on an
            # ambiguous timeout never recorded its refs, so :meth:`_resolve_identity`
            # misses even though the order filled and the PUSH echoes our coid.
            recovered_pine_id = self._recover_parked_entry_by_coid(order)
            if recovered_pine_id is not None:
                pine_id, from_entry, leg_type = recovered_pine_id, None, LegType.ENTRY
            else:
                # A fill that reverse-maps to no order this run placed is external
                # activity (manual broker action, or another bot on the same
                # account). The sync engine applies every fill-like event to
                # ``BrokerPosition`` regardless of ``pine_id``, so emitting it would
                # corrupt this strategy's local position. Drop it — mirroring
                # :meth:`_order_error_to_event` and the reference-plugin
                # external-order policy (see broker-plugin-responsibility-review.md).
                # ``store_ctx is None`` (tests / persistence off) keeps the legacy
                # emit path so identity-less fixtures still observe the event.
                self.store_ctx.log_event(
                    'external_activity_ignored',
                    exchange_order_id=str(order.orderId) or None,
                    payload={
                        'execution_type': event_type,
                        'order_id': order.orderId,
                        'position_id': order.positionId,
                    },
                )
                return None
        if (event_type in ('filled', 'partial') and leg_type is LegType.ENTRY
                and (fill_qty or 0.0) > 0.0 and (fill_price or 0.0) > 0.0):
            # Only a fill record_fill will actually apply touches the cursor:
            # a malformed slice (no volume / price) must neither advance the
            # cursor (it would desync the watermark and suppress a corrected
            # redelivery) nor be suppressed by it.
            if (prior_entry_filled is not None
                    and volume_to_units(order.executedVolume)
                    <= prior_entry_filled + 1e-9):
                # A reconcile working-row pass already counted this entry-fill
                # cumulative straight from ``order.executedVolume`` (the cursor
                # was ahead at event entry). That path cannot enumerate deals, so
                # it never seeded ``_seen_deal_ids`` — and the dealId guard above
                # only catches a PUSH-vs-PUSH replay. Re-emitting the slice would
                # double-apply it in ``record_fill``. The dealId is now in
                # ``_seen_deal_ids`` (added above), so any later copy of this
                # same deal is caught by that guard too.
                return None
            self._advance_fill_cursor(order)
        if (event_type == 'filled' and order.closingOrder
                and self._position_is_flat(message)):
            self._mark_position_closed(order.positionId)
        elif event_type in ('filled', 'partial') and order.positionId:
            self._link_position(order, exch_order.client_order_id)

        return OrderEvent(
            order=exch_order,
            event_type=event_type,
            fill_price=fill_price,
            fill_qty=fill_qty,
            timestamp=timestamp,
            pine_id=pine_id,
            from_entry=from_entry,
            leg_type=leg_type,
            fee=fee,
            fee_currency=fee_currency,
            fill_id=fill_id,
        )

    def _order_from_proto(self, order) -> ExchangeOrder:
        """Build an :class:`ExchangeOrder` snapshot from a ``ProtoOAOrder``."""
        symbol_id = order.tradeData.symbolId
        qty = volume_to_units(order.tradeData.volume)
        filled = volume_to_units(order.executedVolume)
        side = ('buy' if order.tradeData.tradeSide == _model.ProtoOATradeSide.BUY
                else 'sell')
        return ExchangeOrder(
            id=str(order.orderId),
            symbol=self._symbol_name_for(symbol_id),
            side=side,
            order_type=_ORDER_TYPE_MAP.get(order.orderType, OrderType.MARKET),
            qty=qty,
            filled_qty=filled,
            remaining_qty=max(0.0, qty - filled),
            price=order.limitPrice or None,
            stop_price=order.stopPrice or None,
            average_fill_price=order.executionPrice or None,
            status=_ORDER_STATUS_MAP.get(order.orderStatus, OrderStatus.OPEN),
            timestamp=order.utcLastUpdateTimestamp / 1000.0,
            fee=0.0,
            fee_currency='',
            reduce_only=order.closingOrder,
            client_order_id=order.clientOrderId or self._coid_for_order(order),
        )

    def _resolve_identity(
            self, order, deal,
    ) -> tuple[str | None, str | None, LegType | None]:
        """Reverse-map an execution event to its Pine identity.

        Resolves the originating entry via the BrokerStore order-ref index
        (``order_id`` then ``position_id``). A closing-order fill is reported as
        the entry's exit; a non-closing fill as the entry itself. Precise
        TP-vs-SL leg attribution and disconnect-replay are M3.
        """
        if self.store_ctx is None:
            return None, None, None
        row = None
        if order.orderId:
            row = self.store_ctx.find_by_ref('order_id', str(order.orderId))
        if row is None and order.positionId:
            row = self.store_ctx.find_by_ref('position_id', str(order.positionId))
        if row is None:
            return None, None, None
        if order.closingOrder:
            return None, row.pine_entry_id, LegType.CLOSE
        return row.pine_entry_id, None, LegType.ENTRY

    def _recover_parked_entry_by_coid(self, order) -> str | None:
        """Reverse-map a parked entry fill the order/position ref index missed.

        A MARKET entry whose dispatch ended in an ambiguous timeout is parked
        with only its persist-first ``submitted`` -> ``disposition_unknown`` row:
        the success-path ref recording (:meth:`_persist_entry`) never ran, so the
        row carries no ``order_id`` / ``position_id`` alias and
        :meth:`_resolve_identity` cannot reverse-map a later fill. When the order
        in fact filled, cTrader echoes our deterministic ``clientOrderId`` on the
        PUSH execution event (``ProtoOAOrder``), so the row is still recoverable by
        its coid. This is the in-session counterpart of the startup
        :meth:`~pynecore_ctrader.recovery._RecoveryMixin._confirm_recovered_entry`
        path: mirror :meth:`_persist_entry`'s ref recording (so
        :meth:`_advance_fill_cursor`, :meth:`_link_position` and the reconcile
        snapshot reverse-map every later event), drop the now-resolved
        ``pending_verifications`` park the sync engine left on the timeout (a
        filled MARKET never re-surfaces in ``get_open_orders``, so
        ``_verify_pending_dispatches`` would replay it forever), and return the
        entry's Pine id so the fill is emitted instead of mis-dropped as external
        — which would otherwise strand an unmanaged open position until the next
        restart's recovery re-entry.

        Entry path only: a close fill carries no ``clientOrderId``
        (``ProtoOAClosePositionReq`` has no such field), so the ``closingOrder``
        guard keeps a coid match from ever reclassifying a close as an entry.
        """
        if (self.store_ctx is None or not order.clientOrderId
                or order.closingOrder):
            return None
        row = self.store_ctx.get_order(order.clientOrderId)
        if row is None or row.pine_entry_id is None:
            return None
        coid = row.client_order_id
        order_id = str(order.orderId)
        position_id = order.positionId or 0
        # Broker-clock since-anchor for the M3 deal-history bridge, exactly as
        # :meth:`_persist_entry`; fall back to the client clock when the PUSH
        # carried no order timestamp.
        submitted_at_ms = order.utcLastUpdateTimestamp or int(epoch_time() * 1000)
        extras = dict(row.extras or {})
        extras['order_id'] = order_id
        extras['position_id'] = position_id or None
        extras['submitted_at_ms'] = submitted_at_ms
        self.store_ctx.upsert_order(
            coid,
            state='confirmed',
            filled_qty=volume_to_units(order.executedVolume),
            exchange_order_id=(str(position_id) if position_id else order_id),
            extras=extras,
        )
        self.store_ctx.add_ref(coid, 'order_id', order_id)
        self._link_position_ref(coid, position_id)
        self.store_ctx.record_unpark(coid)
        self.store_ctx.log_event(
            'recovered_parked_entry_by_coid', client_order_id=coid,
            exchange_order_id=order_id, intent_key=row.intent_key,
            payload={'position_id': position_id or None},
        )
        return row.pine_entry_id

    def _coid_for_order(self, order) -> str | None:
        """Best-effort BrokerStore lookup of the client-order-id for an order."""
        if self.store_ctx is None or not order.orderId:
            return None
        row = self.store_ctx.find_by_ref('order_id', str(order.orderId))
        return row.client_order_id if row is not None else None

    def _entry_fill_cursor(self, order) -> float | None:
        """Snapshot the entry row's durable ``filled_qty`` cursor by order id.

        Returns ``None`` when no cursor is resolvable (store off, no order id,
        or the row is not reverse-mapped by ``order_id`` yet — e.g. a parked
        entry recovered by ``clientOrderId`` only). Read BEFORE this event
        mutates any row state so the caller can tell a reconcile pass that
        already counted this cumulative (cursor ahead) from this event's own
        first-time recording.
        """
        if self.store_ctx is None or not order.orderId:
            return None
        row = self.store_ctx.find_by_ref('order_id', str(order.orderId))
        return row.filled_qty if row is not None else None

    def _advance_fill_cursor(self, order) -> None:
        """Advance the entry row's durable ``filled_qty`` to the PUSH cumulative.

        The reconcile snapshot diffs ``order.executedVolume`` against the row's
        ``filled_qty`` cursor; if the PUSH path left the cursor stale, a
        reconcile pass in the same ``watch_orders`` cycle would re-derive — and
        re-emit — the slice the PUSH just reported (the engine defers the
        matching ``record_fill`` to the next bar, so the store cursor is the only
        in-flight de-dup signal). Writing the cumulative here, BEFORE the
        OrderEvent is yielded / enqueued, makes the cursor the single shared
        de-dup channel for the PUSH and reconcile paths and keeps it
        restart-consistent. Monotonic: the cursor only grows and is clamped to
        the order's own size (``set_filled`` writes the absolute value with no
        max of its own).
        """
        if self.store_ctx is None or not order.orderId:
            return
        row = self.store_ctx.find_by_ref('order_id', str(order.orderId))
        if row is None:
            return
        cumulative = min(
            row.qty, max(row.filled_qty, volume_to_units(order.executedVolume)),
        )
        if cumulative > row.filled_qty + 1e-9:
            self.store_ctx.set_filled(row.client_order_id, cumulative)

    def _link_position(self, order, coid: str | None) -> None:
        """Link an entry to its netted ``positionId`` once its order fills.

        Mirrors the ``positionId`` into the entry row's ``extras`` (so a full
        close can flatten every pyramid row — :meth:`_mark_position_closed`) and
        FIFO-pins the public reverse-map alias (:meth:`_link_position_ref`). Runs
        when a working order fills and first carries a ``positionId``.
        """
        if self.store_ctx is None or not order.positionId:
            return
        row = self.store_ctx.find_by_ref('order_id', str(order.orderId)) if order.orderId else None
        target_coid = coid or (row.client_order_id if row is not None else None)
        if target_coid is None:
            return
        # ``upsert_order`` REPLACES the extras blob, so read-merge to preserve
        # the existing ``order_id`` alias mirror.
        existing = self.store_ctx.get_order(target_coid)
        extras = dict(existing.extras or {}) if existing is not None else {}
        extras['position_id'] = order.positionId
        self.store_ctx.upsert_order(target_coid, extras=extras)
        self._link_position_ref(target_coid, order.positionId)

    def _position_is_flat(self, message) -> bool:
        """Report whether a closing fill left the net position flat.

        A partial ``ProtoOAClosePositionReq`` (engine-driven SOFTWARE partial
        bracket or a script partial close) fills the closing order while the
        position stays OPEN with a reduced volume. The execution event carries
        the post-execution ``position`` snapshot, so the entry row must only be
        closed once that snapshot reports ``POSITION_STATUS_CLOSED`` (or a zero
        remaining volume); otherwise later fills for the remaining position can
        no longer reverse-map to Pine. An absent ``position`` field cannot
        indicate a partial reduce, so it is treated as flat.
        """
        if not message.HasField('position'):
            return True
        position = message.position
        if position.positionStatus == _model.ProtoOAPositionStatus.POSITION_STATUS_CLOSED:
            return True
        return position.tradeData.volume == 0

    def _mark_position_closed(self, position_id: int) -> None:
        """Close every BrokerStore entry row of a now-flat netted position.

        NETTING merges pyramid entries onto one ``positionId``, so a full close
        must flatten ALL of them — the FIFO-pinned ``position_id`` alias only
        resolves to the oldest, leaving the other pyramid rows live. Match on the
        per-entry ``extras['position_id']`` mirror, with ``exchange_order_id`` as
        a compatibility fallback, and the alias as a last resort for older rows
        that predate the extras mirror. Targets are materialised before closing
        so the live-order scan is not mutated mid-iteration.
        """
        if self.store_ctx is None or not position_id:
            return
        pid_str = str(position_id)
        targets = [
            row.client_order_id
            for row in self.store_ctx.iter_live_orders()
            if (row.extras or {}).get('position_id') == position_id
            or row.exchange_order_id == pid_str
        ]
        if not targets:
            row = self.store_ctx.find_by_ref('position_id', pid_str)
            if row is not None:
                targets.append(row.client_order_id)
        for coid in targets:
            self.store_ctx.close_order(coid)

    def _order_error_to_event(self, error) -> OrderEvent | None:
        """Translate a ``ProtoOAOrderErrorEvent`` into a rejected OrderEvent."""
        if self.store_ctx is None or not error.orderId:
            return None
        row = self.store_ctx.find_by_ref('order_id', str(error.orderId))
        if row is None:
            return None
        exch = ExchangeOrder(
            id=str(error.orderId), symbol=row.symbol, side=row.side,
            order_type=OrderType.MARKET, qty=row.qty, filled_qty=row.filled_qty,
            remaining_qty=max(0.0, row.qty - row.filled_qty), price=None,
            stop_price=None, average_fill_price=None, status=OrderStatus.REJECTED,
            timestamp=epoch_time(), fee=0.0, fee_currency='', reduce_only=False,
            client_order_id=row.client_order_id,
        )
        return OrderEvent(
            order=exch, event_type='rejected', fill_price=None, fill_qty=None,
            timestamp=epoch_time(), pine_id=row.pine_entry_id, from_entry=None,
            leg_type=None,
        )
