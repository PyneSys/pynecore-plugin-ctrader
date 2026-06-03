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

        A recoverable reconcile error — a transport hiccup on the snapshot or
        the deal-history bridge request — is logged and swallowed so the PUSH
        stream (the primary order-event source) is never torn down by the
        gap-filler. ``asyncio.CancelledError`` is a ``BaseException`` and so
        still propagates for a clean teardown.
        """
        try:
            async for event in self._reconcile_snapshot():
                yield event
        except Exception as exc:  # noqa: BLE001 - gap-filler must not kill the PUSH stream
            logger.warning("cTrader reconcile pass failed (transient): %s", exc)

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
        deal = message.deal if message.HasField('deal') else None
        if deal is not None:
            # The dispatch path re-injects the correlated fill that
            # ``send_request`` consumed; cTrader may ALSO push an uncorrelated
            # copy. Both carry the same ``dealId`` — record the fill once.
            if deal.dealId in self._seen_deal_ids:
                return None
            self._seen_deal_ids.add(deal.dealId)
        exch_order = self._order_from_proto(order)

        fill_price: float | None = None
        fill_qty: float | None = None
        fee = 0.0
        fee_currency = ''
        timestamp = epoch_time()
        if deal is not None:
            fill_price = deal.executionPrice or None
            fill_qty = volume_to_units(deal.filledVolume)
            fee = money_value(deal.commission, deal.moneyDigits)
            if deal.executionTimestamp:
                timestamp = deal.executionTimestamp / 1000.0
        elif order.executionPrice:
            fill_price = order.executionPrice

        pine_id, from_entry, leg_type = self._resolve_identity(order, deal)
        if (event_type in ('filled', 'partial') and leg_type is None
                and self.store_ctx is not None):
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
        if event_type in ('filled', 'partial') and leg_type is LegType.ENTRY:
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

    def _coid_for_order(self, order) -> str | None:
        """Best-effort BrokerStore lookup of the client-order-id for an order."""
        if self.store_ctx is None or not order.orderId:
            return None
        row = self.store_ctx.find_by_ref('order_id', str(order.orderId))
        return row.client_order_id if row is not None else None

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
