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
from abc import ABC
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
from pynecore.core.broker.store_helpers import (
    EXTRAS_KEY_BRACKET_OWN_LEG_ID,
    STATE_CANCEL_PENDING,
    STATE_CLOSING,
    STATE_CONFIRMED,
    STATE_DISPOSITION_UNKNOWN,
    STATE_SERVER_REF_SEEN,
    iter_active_bracket_ownerships,
)
from pynecore.types.strategy import JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY

from ._base import _CTraderBase
from .helpers import money_value, parse_protocol_id, volume_to_units
from .messages import OpenApiMessages_pb2 as OpenApiMessages
from .messages import OpenApiModelMessages_pb2 as OpenApiModelMessages
from .state import _ORDER_STATUS_MAP, _ORDER_TYPE_MAP
from .wire import CTraderConnectionError

logger = logging.getLogger(__name__)

#: ``ProtoOAExecutionType`` -> :class:`OrderEvent` ``event_type``.
_EXEC_TYPE_TO_EVENT = {
    OpenApiModelMessages.ProtoOAExecutionType.ORDER_ACCEPTED: 'created',
    OpenApiModelMessages.ProtoOAExecutionType.ORDER_FILLED: 'filled',
    OpenApiModelMessages.ProtoOAExecutionType.ORDER_PARTIAL_FILL: 'partial',
    OpenApiModelMessages.ProtoOAExecutionType.ORDER_CANCELLED: 'cancelled',
    OpenApiModelMessages.ProtoOAExecutionType.ORDER_REJECTED: 'rejected',
    OpenApiModelMessages.ProtoOAExecutionType.ORDER_EXPIRED: 'cancelled',
}

#: ``orders.state`` values of a real ENTRY dispatch row (as opposed to the
#: book-keeping markers — bracket ownership, close legs, entry-stop watches —
#: that carry no exchange order). Used by the venue-close attribution scan.
_ENTRY_ROW_STATES = frozenset({
    STATE_CONFIRMED,
    STATE_CLOSING,
    STATE_CANCEL_PENDING,
    STATE_SERVER_REF_SEEN,
    STATE_DISPOSITION_UNKNOWN,
})

#: ``qsize`` above which a consumer backlog is warned about (never dropped).
_BACKLOG_WATERMARK = 1000

#: Reconcile-snapshot cadence (seconds). The PUSH stream is the primary source;
#: the reconcile pass is a gap-filler, so the cadence is decoupled from any
#: disappearance grace window. Engine-ordered: the pass runs when the engine
#: next resumes the ``watch_orders`` iterator, not on a hard wall clock.
_RECONCILE_CADENCE_S = 5.0


class _EventStreamMixin(_CTraderBase, ABC):
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
            try:
                event = self._translate_exec_event(message)
            except BrokerManualInterventionError:
                # A deliberate halt must reach the engine's graceful stop.
                raise
            except Exception as exc:  # noqa: BLE001 - one message must not kill the stream
                # A translation failure on one PUSH message is recoverable:
                # tearing the stream down would leave the live strategy
                # stale on an open position, which is far worse than
                # degrading to the reconcile-snapshot gap-filler (it
                # re-derives missed fills from the working/position
                # snapshots on its ~5 s cadence). Log + audit, keep going.
                logger.exception(
                    "cTrader execution-event translation failed; message "
                    "dropped (reconcile gap-filler covers missed fills): %s",
                    exc,
                )
                if self.store_ctx is not None:
                    self.store_ctx.log_event(
                        'exec_event_translation_failed',
                        payload={'error': str(exc)},
                    )
                continue
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
        disappearance-grace tracker (``on_unexpected_cancel='halt'``, or a
        quarantining policy with no engine sink wired ->
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
        if isinstance(message, OpenApiMessages.ProtoOAOrderErrorEvent):
            return self._order_error_to_event(message)
        if not isinstance(message, OpenApiMessages.ProtoOAExecutionEvent):
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
        elif (event_type in ('filled', 'partial') and order.closingOrder
              and leg_type is LegType.CLOSE and (fill_qty or 0.0) > 0.0):
            # A run-owned close that left the position OPEN (a partial reduce).
            # The close executes under the venue's close ``orderId`` — never a
            # journal row of this run — so without this the durable book keeps
            # the entry's full exposure and startup ownership / cycle-end
            # reconciliation reads a position the venue no longer holds.
            self._retire_partial_close_exposure(
                order.positionId, fill_qty or 0.0)
        elif (event_type in ('filled', 'partial') and order.positionId
              and not order.closingOrder):
            # Position-linking mirrors ``positionId`` onto the ENTRY row a fill
            # belongs to. A closing-order fill (e.g. a partial close that
            # leaves the position open) has no entry row of its own: its
            # ``orderId`` is the venue's close order and its ``clientOrderId``
            # is not one of ours — linking would try to upsert a foreign coid.
            self._link_position(order, exch_order.client_order_id)
        elif (event_type == 'cancelled' and not order.closingOrder
              and order.orderId):
            # An external / expiry cancel that reaches the PUSH stream (rather
            # than being consumed by the dispatch confirmed-cancel path) must
            # also retire its working-order row, or the venue-cancelled order
            # stays live in the store. The helper leaves a partially filled
            # entry's row live (its position side is still open exposure).
            self._retire_cancelled_working_order(order.orderId)

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
        side = ('buy' if order.tradeData.tradeSide == OpenApiModelMessages.ProtoOATradeSide.BUY
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

        Run-ownership isolation is the governing rule here. On a one-way
        (netting) account the venue keeps a SINGLE net position per symbol, so
        every run trading that account+symbol attaches its entries to — and
        records a ``position_id`` ref for — the SAME ``positionId``. That alias
        is therefore NOT run-unique: reverse-mapping a fill through it would let
        one run adopt another run's entry (position grows past the run's own
        slice) or book another run's close as its own exit. The only run-unique
        venue handle is the entry ``orderId`` this run placed (recorded as an
        ``order_id`` ref); a close carries only the venue's own close
        ``orderId`` (never in the ref index), so a close is ours only when THIS
        run dispatched it (``_close_dispatch_pine_by_position``, keyed by the
        shared ``positionId`` but only ever populated by our own
        ``execute_close`` / ``close_leg``).

        A close the venue itself fired from the protective bracket armed on the
        position (server-side SL / TP / trailing) is dispatched by neither side,
        so it carries no run-unique handle at all; it is attributed from the
        run's own exposure record instead — see
        :meth:`_run_owned_position_entry`.

        A closing-order fill is reported as the entry's exit; a non-closing
        fill as the entry itself. A fill that maps to neither run-owned handle
        is another run's / external activity and returns ``(None, None, None)``
        so the caller drops it.
        """
        if self.store_ctx is None:
            return None, None, None
        # ``order_id`` — the run-unique handle — resolves an entry this run
        # placed. It never matches a close order (a close order's id is never
        # journaled), so a matched row is always an ENTRY; the ``closingOrder``
        # guard is defensive.
        row = (self.store_ctx.find_by_ref('order_id', str(order.orderId))
               if order.orderId else None)
        if row is not None:
            if order.closingOrder:
                return None, row.pine_entry_id, LegType.CLOSE
            return row.pine_entry_id, None, LegType.ENTRY
        # No run-owned entry order matched. A closing fill is ours only when
        # this run dispatched the close against that position — the shared
        # ``position_id`` ref must NOT stand in for that dispatch, or a
        # concurrent run's close of the netted position would be mis-booked as
        # this run's exit. This same map also covers a close THIS run
        # dispatched against startup-adopted exposure that no entry row links.
        if order.closingOrder and order.positionId in (
                self._close_dispatch_pine_by_position):
            return (
                None,
                self._close_dispatch_pine_by_position[order.positionId],
                LegType.CLOSE,
            )
        # This run dispatched no close against the position — but the venue can
        # close it on its OWN initiative through the protective bracket we
        # armed on it (position-attribute SL / TP / trailing). That fill carries
        # the venue's close ``orderId`` (never journaled) and no coid of ours,
        # so both handles above miss and the fill used to be dropped as
        # external: the engine's book then kept the stale open size for the rest
        # of the cycle. The bracket is OURS whenever this run still holds the
        # position, so the exposure record is the attribution proof.
        if order.closingOrder and order.positionId:
            owner = self._run_owned_position_entry(order.positionId)
            if owner is not None:
                self.store_ctx.log_event(
                    'venue_close_attributed_to_entry',
                    exchange_order_id=str(order.orderId) or None,
                    payload={
                        'order_id': order.orderId,
                        'position_id': order.positionId,
                        'order_type': int(order.orderType),
                        'from_entry': owner,
                    },
                )
                return None, owner, LegType.CLOSE
        return None, None, None

    def _run_owned_position_entry(self, position_id: int) -> str | None:
        """Pine entry id of the run-owned exposure a venue close just flattened.

        Two proofs of ownership, both scoped to THIS run instance by
        ``RunContext`` (``iter_live_orders`` filters on ``run_instance_id``), so
        neither can adopt another run's book:

        * a live bracket-ownership row (``bo:<intent_key>:<leg_id>``, state
          ``bracket_own``) whose ``bracket_own_leg_id`` is this ``positionId``
          — the core one-way emulator wrote it BEFORE amending our exit's
          bracket onto that leg, so a protective fill on the leg is that exit's;
        * a live ENTRY row of this run whose ``extras['position_id']`` mirror is
          this ``positionId`` — the run holds open exposure in the netted
          position, whose protective levels are the ones this run last amended.

        Deliberately last in :meth:`_resolve_identity`: the run-unique handles
        (``order_id`` ref, own close dispatch) are tried first, and a
        ``positionId`` matching NOTHING this run owns still falls through to
        external activity. A malformed persisted id is skipped rather than
        raised on — one bad extras blob must not veto the attribution of the
        remaining rows (or kill the PUSH stream).

        :param position_id: The closing execution's ``positionId``.
        :return: The owning entry's Pine id, or ``None`` when unowned.
        """
        if self.store_ctx is None:
            return None
        for row in iter_active_bracket_ownerships(self.store_ctx):
            leg_id = (row.extras or {}).get(EXTRAS_KEY_BRACKET_OWN_LEG_ID)
            if leg_id is None or not row.from_entry:
                continue
            try:
                if parse_protocol_id(leg_id, field='bracket_own_leg_id') == position_id:
                    return row.from_entry
            except ValueError:
                continue
        for row in self.store_ctx.iter_live_orders():
            if row.state not in _ENTRY_ROW_STATES or not row.pine_entry_id:
                continue
            raw = (row.extras or {}).get('position_id')
            if raw is None:
                continue
            try:
                if parse_protocol_id(raw, field='position_id') == position_id:
                    return row.pine_entry_id
            except ValueError:
                continue
        return None

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
        # the existing ``order_id`` alias mirror. Linking only ever UPDATES an
        # entry row this run recorded: a ``target_coid`` with no row means the
        # id is not ours (cTrader can echo a foreign ``clientOrderId`` on
        # orders we did not place via that field) — upserting it would insert
        # an under-specified row and ``upsert_order`` raises ``ValueError`` on
        # the missing required fields, killing the event stream.
        existing = self.store_ctx.get_order(target_coid)
        if existing is None:
            return
        extras = dict(existing.extras or {})
        extras['position_id'] = order.positionId
        self.store_ctx.upsert_order(target_coid, extras=extras)
        self._link_position_ref(target_coid, order.positionId)

    @staticmethod
    def _position_is_flat(message) -> bool:
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
        if position.positionStatus == OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_CLOSED:
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
        targets: list[str] = []
        for row in self.store_ctx.iter_live_orders():
            position_id_raw = (row.extras or {}).get('position_id')
            matches_position = (
                position_id_raw is not None
                and parse_protocol_id(position_id_raw, field='position_id') == position_id
            )
            if matches_position or row.exchange_order_id == pid_str:
                targets.append(row.client_order_id)
        if not targets:
            row = self.store_ctx.find_by_ref('position_id', pid_str)
            if row is not None:
                targets.append(row.client_order_id)
        for coid in targets:
            self.store_ctx.close_order(coid)

    def _retire_partial_close_exposure(
            self, position_id: int, closed_qty: float,
    ) -> None:
        """Book a run-owned partial close's quantity as retired entry exposure.

        A cTrader close executes under the venue's own close ``orderId`` and is
        never a journal row of this run, so nothing in the durable book records
        that the position shrank. The entry row's ``filled_qty`` cannot absorb
        it either: that cursor is the MONOTONE cumulative-execution watermark
        the PUSH / reconcile / recovery de-dup paths compare the venue's
        ``executedVolume`` against — decrementing it would let those paths
        re-emit an already-applied entry slice as fresh. Accumulate the closed
        quantity in the entry row's
        :data:`JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY` counter instead; startup
        ownership reconstruction and the cycle-end audit subtract it, so the
        owned net keeps tracking the venue's remaining exposure. A FULL close
        never reaches this — the flat branch retires the rows wholesale via
        :meth:`_mark_position_closed`.

        Rows are matched exactly as :meth:`_mark_position_closed`
        (``extras['position_id']`` mirror, ``exchange_order_id`` fallback) and
        reduced in journal order — insertion order, i.e. FIFO oldest-first,
        matching the netting reduce order the ``position_id`` alias pin also
        assumes. Each row absorbs at most its own unretired remainder; an
        overshoot (venue closed more than the journal attributes to this run)
        is clamped rather than driven negative.

        :param position_id: The netted ``positionId`` the close reduced.
        :param closed_qty: The close fill's quantity in Pine units.
        """
        if self.store_ctx is None or not position_id or closed_qty <= 0.0:
            return
        pid_str = str(position_id)
        remaining = closed_qty
        for row in list(self.store_ctx.iter_live_orders()):
            if remaining <= 1e-9:
                break
            position_id_raw = (row.extras or {}).get('position_id')
            matches_position = (
                position_id_raw is not None
                and parse_protocol_id(position_id_raw, field='position_id') == position_id
            )
            if not matches_position and row.exchange_order_id != pid_str:
                continue
            extras = dict(row.extras or {})
            already_raw = extras.get(JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY)
            already = (float(already_raw)
                       if isinstance(already_raw, (int, float)) else 0.0)
            take = min(max(0.0, row.filled_qty - already), remaining)
            if take <= 1e-9:
                continue
            extras[JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY] = already + take
            self.store_ctx.upsert_order(row.client_order_id, extras=extras)
            self.store_ctx.log_event(
                'journal_exposure_retired',
                client_order_id=row.client_order_id,
                exchange_order_id=pid_str,
                payload={'retired': take, 'total_retired': already + take,
                         'source': 'partial_close_fill'},
            )
            remaining -= take

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
