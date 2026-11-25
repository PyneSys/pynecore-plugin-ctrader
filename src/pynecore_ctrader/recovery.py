"""Persist-first crash recovery + startup-orphan retirement for cTrader.

Runs once inside :meth:`connect`, after the account probe and BEFORE the
engine's startup reconcile (which adopts the broker net position). Resolves
every persist-first dispatch row a crash left pending between the entry
wire-send and its post-ack confirm — the rows :meth:`execute_entry` now writes
BEFORE the wire call, in states ``submitted`` / ``disposition_unknown`` /
``server_ref_seen``.

Architecture B — deterministic reconcile-recovery, NO ``DispatchJournal`` fuzzy
resume. cTrader echoes ``clientOrderId`` on ``ProtoOAOrder``, so a pending row's
outcome is read deterministically from two authoritative sources:

* the live reconcile snapshot (``ProtoOAReconcileReq``) — an order still resting
  in ``order[]`` carries ``clientOrderId == row.coid`` → ``confirmed`` live;
* the order history (``ProtoOAOrderListReq``) — for an order the live snapshot
  has shed (filled-then-…, rejected, cancelled, expired) the terminal
  disposition is read by the same ``clientOrderId`` echo, then the deal history
  (``ProtoOADealListReq``, reused from :class:`_ReconcileMixin`) supplies the
  precise fill / closure detail.

The deterministic coid↔order match replaces Capital.com's fuzzy ``/confirms``
heuristic; the Capital.com ``recovery.py`` is the STRUCTURAL pattern
(``promoted_coids`` skip, envelope-anchor cleanup) only — NOT its fuzzy
matching, and NOT its two-pass bracket retirement (cTrader brackets are position
attributes, so there are no separate leg rows to retire).

Resolution rules:

* **Live-position rule.** ``ProtoOAPosition`` carries no ``clientOrderId``, so a
  live position alone NEVER confirms a row — only an ``order[]`` coid-match or
  the order-history bridge does. No net-position inference by symbol; an
  untracked position carrying the run's own label is adopted as a new row of
  its own (``_recover_untracked_venue_state``), never matched to a pending one.
* **Never re-issue.** A row whose outcome cannot be read from either source
  stays pending (``still_unknown``); the recoverer never dispatches a new order.
* **Evidence-gated TTL.** A ``still_unknown`` row is retired
  (``recovered_abandoned_unknown``) only when the elapsed time passed a
  minutes-scale TTL AND the order-history read was complete (paginated to
  exhaustion, no transport error) and carried no match. A truncated / failed
  read is non-evidence → the row stays pending. A running bot applies the same
  rule to a dispatch parked with an unknown disposition and to every row this
  pass left pending (:meth:`_RecoveryMixin._resolve_parked_dispatches`); the
  engine holds the entry such a row belongs to until it is resolved, because
  cTrader would execute a second send under the same ``clientOrderId`` as a
  second order.
* **``confirmed`` ≠ live exposure.** A filled-then-closed row lands ``confirmed``
  but is retired (``close_row``) — the engine's own startup adoption owns the
  position size; recovery never re-opens a closed position.
* **Dual-timestamp lower bound.** The history window reaches back to the EARLIER
  of the persist-first row's ``created_ts_ms`` (the dispatch-start instant) and
  any broker-clock ``submitted_at_ms``, minus a skew margin.
* **De-dup seed.** A fill recovered from the history seeds the shared
  :attr:`_seen_deal_ids`, so a reconnect PUSH replay of the same deal is not
  double-applied after the event stream opens.
"""
import asyncio
import logging
from abc import ABC
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from time import time as epoch_time
from typing import cast

from pynecore.core.broker.exceptions import BrokerError
from pynecore.core.broker.journal import DispatchJournal, ReconcileOutcome
from pynecore.core.broker.parked_dispatch import ParkedOutcome, ParkedVerdict
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    PENDING_DISPATCH_STATES,
    find_pending_dispatch,
)
from pynecore.types.strategy import ADOPTED_STARTUP_ENTRY_ID

from ._base import _UNKNOWN_DISPATCH_TTL_S
from .helpers import REQUEST_TIMEOUT, parse_protocol_id, volume_to_units
from .messages import OpenApiMessages_pb2 as OpenApiMessages
from .messages import OpenApiModelMessages_pb2 as OpenApiModelMessages
from .protection import _PositionDeal, _accepted_by_resume
from .reconcile import _SINCE_ANCHOR_SKEW_MS, _ReconcileMixin
from .wire import CTraderWireError

logger = logging.getLogger(__name__)

#: Liveness escape for a ``still_unknown`` pending row at startup.
_ABANDON_TTL_S = _UNKNOWN_DISPATCH_TTL_S

#: Audit event: an open position carrying the run's label that no journal row
#: of the run tracked was adopted as the bot's own.
_OWN_LEG_RECOVERED_EVENT = 'ctrader_own_leg_recovered'
#: Audit event: a dispatch the startup recovery left pending was found filled
#: into a position startup had adopted as an untracked own leg; the dispatch
#: row took that position over.
_ADOPTED_LEG_TAKEN_OVER_EVENT = 'ctrader_adopted_leg_taken_over'
#: Audit event, once per dispatch: such a dispatch's order filled into the
#: adopted position, but the evidence does not prove which part of it the
#: adopted row holds; the dispatch stays parked and its entry held.
_ADOPTED_LEG_TAKEOVER_HELD_EVENT = 'ctrader_adopted_leg_takeover_held'
#: Audit event: a working order carrying the run's label or client-order-id
#: prefix that no journal row of the run tracked was cancelled (or could not be).
_OWN_ORDER_DEBRIS_EVENT = 'ctrader_own_order_debris'
#: Audit event: an open position on the run's symbol that carries no label at
#: all and no journal row tracks; written once per position.
_UNLABELLED_LEG_EVENT = 'ctrader_untracked_leg_unattributed'
#: Order types of the bot's own entry orders.
_ENTRY_ORDER_TYPES = frozenset({
    OpenApiModelMessages.ProtoOAOrderType.MARKET,
    OpenApiModelMessages.ProtoOAOrderType.LIMIT,
    OpenApiModelMessages.ProtoOAOrderType.STOP,
})


@dataclass(frozen=True, slots=True)
class _OrderLookupResult:
    """Order-history lookup result for one pending row's coid.

    :ivar order: The matching ``ProtoOAOrder`` (carrying the coid echo), or
        ``None`` when nothing matched.
    :ivar conclusive: ``True`` only when the whole window was paginated without
        a transport error — a not-found result is then deterministic absence,
        never an inference from a truncated read.
    """
    order: 'OpenApiModelMessages.ProtoOAOrder | None'
    conclusive: bool


_TERMINAL_ORDER_STATUSES = frozenset({
    OpenApiModelMessages.ProtoOAOrderStatus.ORDER_STATUS_REJECTED,
    OpenApiModelMessages.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED,
    OpenApiModelMessages.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED,
})


class _RecoveryMixin(_ReconcileMixin, ABC):
    """Persist-first crash recovery + startup-orphan retirement (architecture B)."""

    async def _recover_in_flight_submissions(self) -> None:
        """Resolve every pending persist-first dispatch row at startup.

        Walks the BrokerStore's :data:`PENDING_DISPATCH_STATES` rows, resolves
        each against the live reconcile snapshot + order/deal history, then runs
        the single-pass orphan retirement. A no-op when persistence is off (the
        test path) or there is nothing to recover.
        """
        if self.store_ctx is None:
            return
        pending = list(find_pending_dispatch(self.store_ctx))
        res = await self._reconcile(return_protection_orders=True)
        orders_by_coid = {o.clientOrderId: o for o in res.order if o.clientOrderId}
        open_positions = {
            p.positionId: p for p in res.position
            if p.positionStatus == OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN
        }
        promoted_coids: set[str] = set()
        for row in pending:
            if await self._recover_one_pending_row(row, orders_by_coid, open_positions):
                promoted_coids.add(row.client_order_id)
        await self._retire_startup_orphans(res, promoted_coids)
        await self._recover_untracked_venue_state(res)

    async def _recover_untracked_venue_state(
            self, res: 'OpenApiMessages.ProtoOAReconcileRes',
    ) -> None:
        """Recognise the bot's own venue state its journal lost.

        Every order the run sends carries its run tag as ``label`` (and its
        client-order-ids start with it), and an open position keeps the label
        of the order that opened it. A journal write lost to a crash (or a row
        an earlier process closed while its order still lived) leaves the
        run's exposure on the venue with nothing of the run tracking it:

        * An untracked open position carrying the run's label is the bot's
          own. It is adopted (:meth:`_adopt_own_leg`): the engine's startup
          adoption, which follows, takes it up like any position the run
          holds, and its protection is guarded from the venue's view. ERROR +
          audit: the bot's journal lost a fill.
        * An untracked working entry order carrying the run's label or
          client-order-id prefix is cancelled (:meth:`_cancel_own_debris`).
          No intent of the engine stands behind it, so nothing would manage
          it: left working, it could fill into exposure the strategy never
          asked for. The strategy places the order again if it still wants it.
        * An untracked open position on the run's symbol without any label
          was opened by none of the runs (each stamps its label): another
          party's, but nothing tells whose (ERROR + audit, once per
          position). One carrying another label is another run's.

        Never raises for a venue failure: what it cannot do stays as it was,
        logged and audited.
        """
        if self.store_ctx is None:
            return
        run_tag = self.store_ctx.run_tag
        owned = self._owned_position_ids() or set()
        symbol_id = self._symbols_by_name.get(self.symbol) if self.symbol else None
        for position in res.position:
            if (position.positionStatus
                    != OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN
                    or position.positionId in owned):
                continue
            label = position.tradeData.label
            if label == run_tag:
                self._adopt_own_leg(position)
            elif not label and position.tradeData.symbolId == symbol_id:
                self._note_unlabelled_leg(position)
        for order in res.order:
            if (order.closingOrder or order.orderType not in _ENTRY_ORDER_TYPES
                    or order.orderStatus != OpenApiModelMessages.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED
                    or not (order.tradeData.label == run_tag
                            or order.clientOrderId.startswith(f"{run_tag}-"))
                    or self.store_ctx.find_by_ref('order_id', str(order.orderId)) is not None
                    or (order.clientOrderId
                        and self.store_ctx.get_order(order.clientOrderId) is not None)):
                continue
            await self._cancel_own_debris(order)

    def _adopt_own_leg(self, position: 'OpenApiModelMessages.ProtoOAPosition') -> None:
        """Journal an open position of the bot's that no row of the run tracks.

        The row counts as the run's own exposure (not ``adopted_startup``):
        filled, linked to the position, under the synthetic entry id the
        engine's adoption uses for exposure no entry names. Its protection is
        taken from the venue's view (:meth:`_take_own_fill_protection`).
        """
        assert self.store_ctx is not None
        position_id = position.positionId
        coid = f"{self.store_ctx.run_tag}-recovered-{position_id}"
        qty = volume_to_units(position.tradeData.volume)
        side = ('buy' if position.tradeData.tradeSide == OpenApiModelMessages.ProtoOATradeSide.BUY
                else 'sell')
        symbol = self._symbol_name_for(position.tradeData.symbolId)
        self.store_ctx.upsert_order(
            coid, symbol=symbol, side=side, qty=qty, filled_qty=qty, state='confirmed',
            pine_entry_id=ADOPTED_STARTUP_ENTRY_ID, exchange_order_id=str(position_id),
            extras={'kind': ENTRY_KIND_POSITION, 'entry_filled_at': epoch_time(),
                    'position_id': str(position_id), 'recovered_own_leg': True},
        )
        self._link_position_ref(coid, position_id)
        logger.error(
            "cTrader startup: open position %d (%s %s units of %s) carries this run's "
            "label %r but no journal row of the run tracked it; adopted as the bot's own",
            position_id, side, qty, symbol, self.store_ctx.run_tag)
        self.store_ctx.log_event(
            _OWN_LEG_RECOVERED_EVENT, client_order_id=coid,
            exchange_order_id=str(position_id),
            payload={'position_id': position_id, 'symbol': symbol, 'side': side,
                     'qty': qty, 'label': position.tradeData.label})
        self._take_own_fill_protection(position)

    async def _cancel_own_debris(self, order: 'OpenApiModelMessages.ProtoOAOrder') -> None:
        """Cancel a working order of the bot's that no row of the run tracks."""
        assert self.store_ctx is not None
        payload = {'order_id': order.orderId, 'client_order_id': order.clientOrderId,
                   'label': order.tradeData.label, 'volume': order.tradeData.volume,
                   'executed_volume': order.executedVolume}
        try:
            await self._dispatch_order(
                OpenApiMessages.ProtoOACancelOrderReq(
                    ctidTraderAccountId=self._live_account_id, orderId=order.orderId),
                coid=order.clientOrderId or str(order.orderId), context="own debris cancel")
        except (BrokerError, CTraderWireError, TimeoutError) as exc:
            logger.error(
                "cTrader startup: working order %d (%s) carries this run's tag but no journal "
                "row of the run tracks it; its cancel failed, it stays working: %s",
                order.orderId, order.clientOrderId, exc)
            self.store_ctx.log_event(
                _OWN_ORDER_DEBRIS_EVENT, exchange_order_id=str(order.orderId),
                payload={**payload, 'cancelled': False, 'error': str(exc)})
            return
        logger.error(
            "cTrader startup: working order %d (%s) carries this run's tag but no journal "
            "row of the run tracks it; cancelled", order.orderId, order.clientOrderId)
        self.store_ctx.log_event(
            _OWN_ORDER_DEBRIS_EVENT, exchange_order_id=str(order.orderId),
            payload={**payload, 'cancelled': True})

    def _note_unlabelled_leg(self, position: 'OpenApiModelMessages.ProtoOAPosition') -> None:
        """An untracked, unlabelled position on the run's symbol: ERROR + audit once."""
        assert self.store_ctx is not None
        ref = str(position.positionId)
        if any(xoid == ref for _intent, _coid, xoid, _payload
               in self.store_ctx.iter_events_by_kind_for_run_id(_UNLABELLED_LEG_EVENT)):
            return
        logger.error(
            "cTrader startup: open position %d on the run's symbol carries no label and no "
            "journal row of the run tracks it; none of the runs opened it, so it is not the "
            "bot's, but whose it is cannot be told", position.positionId)
        self.store_ctx.log_event(
            _UNLABELLED_LEG_EVENT, exchange_order_id=ref,
            payload={'position_id': position.positionId,
                     'volume': position.tradeData.volume,
                     'side': int(position.tradeData.tradeSide)})

    async def _recover_one_pending_row(
            self, row, orders_by_coid: 'dict[str, OpenApiModelMessages.ProtoOAOrder]',
            open_positions: 'dict[int, OpenApiModelMessages.ProtoOAPosition]',
    ) -> bool:
        """Resolve one pending row from the two authoritative sources.

        :return: ``True`` when the row was promoted to a live ``confirmed`` row
            (so the orphan pass skips it); ``False`` for a terminally-retired or
            still-unknown row.
        """
        if self.store_ctx is None:
            return False
        coid = row.client_order_id
        # Source 1 — live reconcile ``order[]``: deterministic coid match. A live
        # ``position`` alone NEVER confirms a row (``ProtoOAPosition`` carries no
        # clientOrderId); only a resting ``order`` coid-match or the order-history
        # bridge below does.
        live_order = orders_by_coid.get(coid)
        if live_order is not None:
            avg_price = live_order.executionPrice or None
            if live_order.executedVolume:
                # The resting order has already partially filled. Seed the shared
                # de-dup with that partial fill's ``dealId``(s) BEFORE confirming,
                # so a reconnect PUSH replay of the same partial-fill execution is
                # not double-applied once the event stream opens.
                from_ms = self._recovery_lower_bound_ms(row)
                partial = await self._find_fill_deal(live_order.orderId, from_ms)
                if not partial.filled_cents and not partial.conclusive:
                    # The order reports ``executedVolume`` but the deal-history
                    # read failed / was truncated, so it surfaced no ``dealId`` to
                    # seed the shared de-dup. Confirming ``filled_qty`` here with NO
                    # anchor would let a reconnect PUSH replay of this same partial
                    # fill pass the deal-id filter and be applied a second time
                    # (the runtime ``_reconcile_working_row`` only de-dups against
                    # ``order[]`` progress, not the PUSH stream). Leave the row
                    # pending: the runtime reconcile re-entry recovers it with the
                    # deal ids once the history read completes — exactly as the
                    # Source 2 order-history branch below. Still record the broker
                    # refs we already matched by coid, so a fill PUSH that lands
                    # before that re-entry reverse-maps to this row (the PUSH
                    # identity path resolves by ``order_id`` / ``position_id``, NOT
                    # coid) instead of being mis-classified as external activity.
                    self._record_recovered_refs(
                        row, order_id=live_order.orderId,
                        position_id=live_order.positionId or None,
                        submitted_at_ms=(live_order.utcLastUpdateTimestamp
                                         or int(epoch_time() * 1000)),
                        executed_cents=live_order.executedVolume,
                    )
                    return False
                self._seen_deal_ids.update(partial.deal_ids)
                avg_price = partial.avg_price or avg_price
            self._confirm_recovered_entry(
                row, order_id=live_order.orderId,
                position_id=live_order.positionId or None,
                filled=volume_to_units(live_order.executedVolume),
                avg_price=avg_price,
                submitted_at_ms=(live_order.utcLastUpdateTimestamp
                                 or int(epoch_time() * 1000)),
                source='live_order',
                position=open_positions.get(live_order.positionId),
            )
            return True
        # Source 2 — order history by coid. A crash-before-ack row carries no
        # ``order_id`` yet, so the ``clientOrderId`` echoed on ``ProtoOAOrder`` is
        # the only deterministic key; the order history surfaces every order the
        # live snapshot has shed (filled, rejected, cancelled, expired).
        from_ms = self._recovery_lower_bound_ms(row)
        found = await self._find_order_by_coid(coid, from_ms)
        if found.order is not None:
            return await self._resolve_recovered_order(
                row, found.order, open_positions, from_ms, orders_by_coid=orders_by_coid)
        # Neither source carried the coid. Only a COMPLETE order-history read is
        # evidence of absence — a truncated / failed read stays still_unknown and
        # is retried by the next runtime reconcile re-entry. Never re-dispatch.
        if found.conclusive:
            self._maybe_abandon_unknown(row)
        return False

    async def _resolve_recovered_order(
            self, row, order, open_positions: 'dict[int, OpenApiModelMessages.ProtoOAPosition]',
            from_ms: int, *,
            orders_by_coid: Mapping[str, OpenApiModelMessages.ProtoOAOrder],
    ) -> bool:
        """Classify a recovered order's disposition from order + deal history.

        :param orders_by_coid: The live snapshot's working orders by client
            order id.
        """
        if self.store_ctx is None:
            return False
        # A terminal status with ZERO executed volume is a clean reject / cancel /
        # expiry — retire it. A terminal status with positive ``executedVolume``
        # is a LIMIT/STOP that partially filled before the residual was cancelled
        # / expired: that fill is a live (possibly closed) position, so it must be
        # classified through the deal bridge below, never zero-filled away.
        if order.orderStatus in _TERMINAL_ORDER_STATUSES and not order.executedVolume:
            self._retire_recovered_terminal(row, order)
            return False
        # ACCEPTED / FILLED (or a partially-filled terminal) — the order landed.
        # Read the deal history for the precise fill + closure classification
        # (closedVolume-primary), then confirm — or, when it filled then closed
        # during the gap, retire the row (``confirmed`` != live exposure; the
        # engine adoption owns the size).
        bridge = await self._find_fill_deal(order.orderId, from_ms)
        position_id = (order.positionId
                       or (bridge.deal.positionId if bridge.deal else 0)
                       or None)
        if position_id and position_id not in open_positions:
            if (bridge.closed_cents > 0 and bridge.filled_cents > 0
                    and bridge.deal is not None):
                # Filled then closed while down. Who closed it is judged first,
                # against the bot's share counting the fill this row recovers (its
                # order is the bot's entry, though no ref links it yet) and those
                # of the run's other entry rows whose orders filled onto the same
                # position (:meth:`_unbooked_position_entries`), which retire
                # with it: a retired row leaves every later reconcile scan, so
                # this is the only pass that can apply the intervention policy.
                # Right after an operator resume the close is accepted instead.
                # While a closing order, a sibling's order or its deals cannot be
                # looked up the row stays pending with its refs, like an
                # inconclusive history read below, and so does it while the
                # order's deals show less than the ``executedVolume`` the
                # order reports (:meth:`_fill_history_short`). The closes are
                # judged over the position's history from the earliest window
                # of the rows that may join (:meth:`_with_group_position_history`).
                group = None
                pending_orders = None
                widened = None
                if not self._fill_history_short(
                        bridge, row, order.orderId, order.executedVolume):
                    widened = await self._with_group_position_history(
                        bridge, position_id, from_ms)
                if widened is not None:
                    bridge = widened
                    pending_orders = await self._pending_position_orders(
                        row, order.orderId, position_id, bridge.position_deals,
                        orders_by_coid)
                if pending_orders is not None:
                    group = await self._unbooked_position_entries(
                        row, position_id, bridge.position_deals,
                        working_orders={o.orderId for o in orders_by_coid.values()},
                        pending_orders=pending_orders)
                judged = group is not None
                if group is not None and _accepted_by_resume(row):
                    self._note_accepted_by_resume(
                        position_id, client_order_id=row.client_order_id, volume=None,
                        deal_ids=bridge.closing_deal_ids)
                    self._mark_close_judged(list(bridge.closing_deal_ids))
                elif group is not None:
                    judged = await self._judge_recovered_closes(
                        position_id, bridge.position_deals, from_entry=row.pine_entry_id,
                        share_now=self._share_with_recovered_fills(position_id, [
                            (entry_row, volume_to_units(entry_bridge.filled_cents))
                            for entry_row, entry_bridge in ((row, bridge), *group)]),
                        counted=(*bridge.deal_ids, *(
                            deal_id for _row, other in group for deal_id in other.deal_ids)),
                        own_orders=(order.orderId, *(
                            other.deal.orderId for _row, other in group
                            if other.deal is not None))) is not None
                if not judged or group is None:
                    self._record_recovered_refs(
                        row, order_id=order.orderId, position_id=position_id,
                        submitted_at_ms=(order.utcLastUpdateTimestamp
                                         or int(epoch_time() * 1000)),
                        executed_cents=order.executedVolume,
                    )
                    return False
                # Seed the shared de-dup with the entry fills AND the closing
                # deals BEFORE retiring, so a reconnect PUSH replay of either is
                # not re-applied, then land the rows closed through the writer.
                self._seen_deal_ids.update(bridge.deal_ids)
                self._seen_deal_ids.update(bridge.closing_deal_ids)
                for _row, other in group:
                    self._seen_deal_ids.update(other.deal_ids)
                for entry_row, entry_bridge in ((row, bridge), *group):
                    self._retire_filled_then_closed(
                        entry_row, entry_bridge.deal, position_id, bridge.closed_cents)
                    self._clear_intent_anchor(entry_row)
                return False
            if not bridge.conclusive:
                # Position gone from the open snapshot but the deal-history read
                # was truncated / failed — the missing ``closedVolume`` is a
                # transport gap, not proof the position is still open. Confirming
                # here would record a closed broker position as a live row, so
                # leave it pending: the runtime reconcile re-entry resolves it
                # once the history read completes (evidence-gated, never inferred
                # from a partial read). Record the broker refs already matched by
                # coid so a fill PUSH arriving before that re-entry reverse-maps
                # to this row instead of being treated as external activity.
                self._record_recovered_refs(
                    row, order_id=order.orderId, position_id=position_id,
                    submitted_at_ms=(order.utcLastUpdateTimestamp
                                     or int(epoch_time() * 1000)),
                    executed_cents=order.executedVolume,
                )
                return False
        if not bridge.filled_cents and order.executedVolume and not bridge.conclusive:
            # The order reports executed volume but the deal-history read failed /
            # was truncated, so it surfaced no ``dealId`` to seed the shared
            # de-dup. Confirming from ``order.executedVolume`` here would advance
            # ``filled_qty`` with NO de-dup anchor, so the reconnect PUSH replay of
            # this same fill would pass the deal-id filter and be applied a second
            # time. Leave the row pending: the runtime reconcile re-entry recovers
            # it (with the deal ids) once the history read completes. Record the
            # broker refs already matched by coid so a fill PUSH arriving before
            # that re-entry reverse-maps to this row instead of external activity.
            self._record_recovered_refs(
                row, order_id=order.orderId, position_id=position_id,
                submitted_at_ms=(order.utcLastUpdateTimestamp
                                 or int(epoch_time() * 1000)),
                executed_cents=order.executedVolume,
            )
            return False
        recovered = (volume_to_units(bridge.filled_cents) if bridge.filled_cents
                     else volume_to_units(order.executedVolume))
        self._seen_deal_ids.update(bridge.deal_ids)
        # A terminal (cancelled / expired / rejected) LIMIT/STOP that filled less
        # than ``row.qty`` is a partial whose residual is GONE — it will never
        # fill further. The startup adoption baseline sheds such an order from
        # ``order[]`` yet still sees the adopted-open ``position_id``, so it would
        # bump ``filled_qty`` to ``row.qty`` (its "vanished order on an open
        # position fully filled" rule), undoing the partial just recovered. Mark
        # the row so :meth:`_apply_adoption_baseline` caps the baseline at the
        # recovered partial instead.
        partial_terminal = (order.orderStatus in _TERMINAL_ORDER_STATUSES
                            and recovered < row.qty - 1e-9)
        self._confirm_recovered_entry(
            row, order_id=order.orderId, position_id=position_id, filled=recovered,
            avg_price=(bridge.avg_price or order.executionPrice or None),
            submitted_at_ms=order.utcLastUpdateTimestamp or int(epoch_time() * 1000),
            source='order_history', partial_terminal=partial_terminal,
            position=open_positions.get(position_id) if position_id else None,
        )
        return True

    def _confirm_recovered_entry(
            self, row, *, order_id: int, position_id: int | None,
            filled: float, avg_price: float | None, submitted_at_ms: int,
            source: str, partial_terminal: bool = False,
            position: 'OpenApiModelMessages.ProtoOAPosition | None' = None,
    ) -> None:
        """Promote a pending persist-first row to ``confirmed`` from broker truth.

        Mirrors :meth:`_persist_entry`'s ref-mapping for a row whose ack the
        crash lost: records the broker ``orderId`` / ``positionId`` aliases,
        advances ``filled_qty`` (monotone-clamped into the order's own size) and
        persists the broker-clock ``submitted_at_ms`` since-anchor exactly as
        :meth:`_persist_entry` does. Without that anchor a still-working
        LIMIT/STOP row recovered here would later force
        :meth:`_recover_vanished_working_row` onto its fixed-window fallback and
        miss a fill that lands after a longer offline gap.
        Emits NO fill event — recovery runs before the engine's startup
        adoption, which adopts the broker net position; the adoption-bounded
        baseline then aligns the cursor. The shared de-dup is seeded by the
        caller so a reconnect PUSH replay is not double-counted.

        The open position the recovered fill built (``position``, from the
        recovery snapshot) gets the protection the fill left on it, as a
        pushed fill of the bot's own does
        (:meth:`~pynecore_ctrader.protection._ProtectionMixin._take_own_fill_protection`):
        a position without any is bare, so protection someone adds to it later
        is an intervention. The PUSH replay of the fill is dropped by the
        seeded de-dup, so nothing else takes the position up.

        The crashed dispatch may have left a parked ``pending_verifications``
        row (the sync engine parks a coid on ``OrderDispositionUnknownError``).
        For a MARKET fill or an already-filled LIMIT/STOP, ``get_open_orders``
        never re-surfaces that coid, so ``_verify_pending_dispatches`` would keep
        replaying the stale park forever; drop it here now that recovery has
        resolved the dispatch. The envelope stays alive — the intent is a live
        position the strategy still tracks.
        """
        if self.store_ctx is None:
            return
        coid = row.client_order_id
        cumulative = min(row.qty, max(row.filled_qty, filled))
        extras = dict(row.extras or {})
        extras['order_id'] = str(order_id)
        extras['position_id'] = position_id or None
        extras['submitted_at_ms'] = self._recovered_since_anchor_ms(row, submitted_at_ms)
        if partial_terminal:
            # The order is terminal and filled less than ``row.qty``: the
            # adoption baseline must NOT raise this row to ``row.qty`` on the
            # strength of the adopted-open position alone.
            extras['recovered_partial_terminal'] = True
        self.store_ctx.upsert_order(
            coid, state='confirmed', filled_qty=cumulative,
            exchange_order_id=(str(position_id) if position_id else str(order_id)),
            extras=extras,
        )
        self.store_ctx.add_ref(coid, 'order_id', str(order_id))
        self.store_ctx.record_unpark(coid)
        if position_id:
            self._link_position_ref(coid, position_id)
        if (position is not None and cumulative > 1e-9
                and position.positionId == position_id
                and self._position_owned(position.positionId)):
            self._take_own_fill_protection(position)
        self.store_ctx.log_event(
            'recovered_in_flight_confirmed', client_order_id=coid,
            exchange_order_id=str(order_id), intent_key=row.intent_key,
            payload={'position_id': position_id, 'filled_qty': cumulative,
                     'source': source, 'avg_price': avg_price},
        )

    def _record_recovered_refs(
            self, row, *, order_id: int, position_id: int | None,
            submitted_at_ms: int, executed_cents: int, before_adoption: bool = True,
    ) -> None:
        """Mirror the broker refs onto a still-pending coid-matched row.

        Used when the live snapshot / order history matched the row by
        ``clientOrderId`` (so the broker ``orderId`` / ``positionId`` are known)
        but the deal-history read was inconclusive, so the row must stay pending
        for the next reconcile re-entry rather than confirm. Records the
        ``order_id`` / ``position_id`` aliases and mirrors them into
        ``orders.extras`` exactly as :meth:`_persist_entry` does, but WITHOUT
        advancing ``filled_qty`` or leaving the ``submitted`` state.

        Without these refs the row carries only its coid, which neither the PUSH
        identity path (:meth:`_resolve_identity` resolves by ``order_id`` then
        ``position_id``) nor the runtime ``_reconcile_snapshot`` (keyed on
        ``extras['order_id']``) can reverse-map: a fill that lands before the
        next recovery re-entry would then be mis-classified as external activity
        and silently dropped. Recording the refs here is safe because the row
        kept ``filled_qty`` unadvanced, so the PUSH stream applies that fill
        exactly once.

        The ``recovered_inconclusive`` marker tells
        :meth:`_apply_adoption_baseline` to leave this row's cursor alone: the
        ``order_id`` it records would otherwise match the adoption snapshot's
        ``executedVolume`` (or, once shed from ``order[]``, the adopted-open
        ``position_id``) and silently raise ``filled_qty`` with NO ``dealId``
        seeded into the shared de-dup — defeating the very "kept ``filled_qty``
        unadvanced so the PUSH stream applies that fill exactly once" guarantee
        above. The runtime reconcile re-entry owns the advance once the deal
        history reads conclusively. A running bot has its adoption baseline
        behind it (``before_adoption=False``), so its rows get no marker.

        The ``executedVolume`` the order reports is kept as the row's
        ``executed_cents`` floor (never lowered, never a fill cursor): the
        runtime re-entry that later judges the row's deal evidence
        (:meth:`_fill_history_short`) still knows every fill the venue already
        reported for the order, though the journal counts none of it yet.

        :param executed_cents: The order's ``executedVolume``.
        """
        if self.store_ctx is None:
            return
        coid = row.client_order_id
        extras = dict(row.extras or {})
        extras['order_id'] = str(order_id)
        extras['position_id'] = position_id or None
        extras['submitted_at_ms'] = self._recovered_since_anchor_ms(row, submitted_at_ms)
        extras['executed_cents'] = max(
            int(executed_cents), int(extras.get('executed_cents') or 0))
        if before_adoption:
            extras['recovered_inconclusive'] = True
        self.store_ctx.upsert_order(coid, extras=extras)
        self.store_ctx.add_ref(coid, 'order_id', str(order_id))
        if position_id:
            self._link_position_ref(coid, position_id)

    def _retire_recovered_terminal(self, row, order) -> None:
        """Land a rejected / cancelled / expired recovered row in ``rejected``."""
        if self.store_ctx is None:
            return
        DispatchJournal(self.store_ctx).apply_reconcile_outcome(
            row.client_order_id,
            ReconcileOutcome(
                kind='terminal_close',
                reason='recovered_in_flight_terminal',
                new_state='rejected',
                audit_event='recovered_in_flight_rejected',
                close_row=True,
                audit_payload={'order_id': order.orderId,
                               'order_status': int(order.orderStatus)},
                exchange_order_id=str(order.orderId),
            ),
        )
        self._clear_intent_anchor(row)

    async def _resolve_parked_dispatches(self) -> None:
        """Resolve the dispatches parked with an unknown disposition in-run.

        An order send that timed out or lost its connection after the send
        leaves its persist-first row in ``disposition_unknown`` and the
        engine's dispatch parked. A row the startup recovery could not
        resolve is in the same position, in any pending state: an earlier
        process sent it, and the engine holds its intent back until it is
        resolved. The core rule
        (:class:`~pynecore.core.broker.parked_dispatch.ParkedDispatchResolver`)
        releases the ones the venue never accepted; this pass runs it at the
        reconcile cadence and answers its question from the two sources the
        startup recovery reads, by the ``clientOrderId`` echo:

        * a row that already carries its ``order_id`` — landed.
        * the order is in the live ``order[]`` snapshot, or the order history
          has it working, filled or partly filled — landed. Its broker refs
          are recorded on the row, so the reconcile snapshot takes it over
          and recovers a fill the PUSH stream missed.
        * the order history has it rejected, cancelled or expired with
          nothing executed — the venue refused it.
        * a complete order-history read without the coid — absent.
        * a failed or truncated read — inconclusive.

        The live snapshot is read once per pass, and only when a parked row
        is due for a lookup. Never raises for a venue failure.
        """
        store = self.store_ctx
        if store is None or self._wire is None or self._live_account_id is None:
            return
        live_orders: dict[str, OpenApiModelMessages.ProtoOAOrder] | None = None

        async def lookup(row) -> ParkedVerdict:
            nonlocal live_orders
            if (row.extras or {}).get('order_id') is not None:
                return ParkedVerdict(ParkedOutcome.LANDED)
            coid = row.client_order_id
            try:
                async with asyncio.timeout_at(self._reconcile_pass_deadline):
                    if live_orders is None:
                        res = await self._reconcile()
                        live_orders = {o.clientOrderId: o for o in res.order if o.clientOrderId}
                    order = live_orders.get(coid)
                    if order is None:
                        found = await self._find_order_by_coid(
                            coid, self._recovery_lower_bound_ms(row))
                        if found.order is None:
                            return ParkedVerdict(ParkedOutcome.ABSENT if found.conclusive
                                                 else ParkedOutcome.INCONCLUSIVE)
                        order = found.order
            except Exception as exc:  # noqa: BLE001 - read failure is inconclusive evidence
                logger.warning(
                    "cTrader parked dispatch lookup for coid %s failed: %s", coid, exc)
                return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
            status = OpenApiModelMessages.ProtoOAOrderStatus.Name(order.orderStatus)
            if order.orderStatus in _TERMINAL_ORDER_STATUSES and not order.executedVolume:
                return ParkedVerdict(
                    ParkedOutcome.REFUSED, order_id=str(order.orderId), status=status)
            # Only an earlier process's send can have filled before the
            # adoption; a send of this process went out after it.
            assert store is not None
            adopted = (self._adopted_leg_of(order.positionId)
                       if store.is_inherited_dispatch(coid) else None)
            if adopted is not None:
                taken = await self._take_over_adopted_leg(row, order, adopted)
                if taken is False:
                    return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
                if taken:
                    return ParkedVerdict(
                        ParkedOutcome.LANDED, order_id=str(order.orderId), status=status)
            self._record_recovered_refs(
                row, order_id=order.orderId, position_id=order.positionId or None,
                submitted_at_ms=order.utcLastUpdateTimestamp or int(epoch_time() * 1000),
                executed_cents=order.executedVolume,
                before_adoption=False,
            )
            return ParkedVerdict(ParkedOutcome.LANDED, order_id=str(order.orderId), status=status)

        await self._parked_resolver.resolve(self.store_ctx, lookup)

    def _adopted_leg_of(self, position_id: int):
        """The live row startup adopted for an untracked position of the run, if any.

        :param position_id: A venue position id; ``0`` names none.
        :return: The ``recovered_own_leg`` row tracking that position, or ``None``.
        """
        if self.store_ctx is None or not position_id:
            return None
        for other in self.store_ctx.iter_live_orders():
            extras = other.extras or {}
            if (extras.get('recovered_own_leg')
                    and str(extras.get('position_id')) == str(position_id)):
                return other
        return None

    async def _take_over_adopted_leg(self, row, order, adopted) -> bool | None:
        """Hand the part of an adopted own leg an inherited send filled to that send's row.

        The send of ``row`` (an earlier process's) landed and filled before
        the restart, but neither venue view listed the order at startup, so
        the open position, which carries the run's label, was adopted as an
        own leg of its own (:meth:`_adopt_own_leg`) and the engine's startup
        adoption booked it. Recording the refs and letting the reconcile
        snapshot recover the fill would book the same volume a second time
        and leave two rows owning one position. Instead the row is confirmed
        from the order's own deals, as the startup recovery would have
        confirmed it (no fill event, those deals seeded into the de-dup), and
        the adopted leg gives up exactly that volume: it is closed when
        nothing of it is left. One position, one owner, booked once.

        Only the order's deals the adoption booked are taken over: those
        executed no later than the position's last change in the adoption
        snapshot (:attr:`~_CTraderBase._adoption_position_ts`). The row's
        cursor stands at their volume, so the reconcile snapshot recovers and
        books every later deal of the order as a fill.

        The transfer moves exposure between two rows and never creates or
        retires any, so it runs only when the evidence proves the adopted leg
        holds the order's adopted deals: the leg is on the order's side and
        holds at least their volume. Otherwise (part of the fill was closed
        before the restart, or the position grew by a volume the adoption
        could not attribute to the leg, see
        ``_StateMixin._grow_recovered_own_legs``) which deals the leg covers
        is not known, and the dispatch is held: it stays parked, the engine
        holds its entry, and a WARNING with an audit event says why, once.

        The deal history is read before the store is touched; both rows are
        then read again and the donor's reduction, the row's confirmation and
        the audit events commit in one transaction
        (:meth:`~pynecore_ctrader.protection._ProtectionMixin._judgment_span`), whose
        rollback also puts back what this process noted of them. The de-dup is seeded only
        after that commit, so a failed transfer suppresses no fill and is
        repeated whole by a later lookup.

        :param row: The inherited dispatch row the venue order belongs to.
        :param order: The venue order, filled at least in part.
        :param adopted: The ``recovered_own_leg`` row of the order's position.
        :return: ``True`` when the row took its part over. ``None`` when no
            deal of the order is in the adopted size: nothing is changed and
            the order is recovered like any other. ``False`` when the startup
            adoption has not run yet, the order's deal history could not be
            read completely, a row changed while it was read, the transfer is
            held, or the store write failed: nothing is changed and the lookup
            is repeated later.
        """
        store = self.store_ctx
        assert store is not None
        if not self._adoption_baselined:
            return False
        adopted_through_ms = self._adoption_position_ts.get(order.positionId)
        if adopted_through_ms is None:
            return None
        bridge = await self._find_fill_deal(
            order.orderId, self._recovery_lower_bound_ms(row),
            deadline=self._reconcile_pass_deadline)
        if not bridge.conclusive or not bridge.filled_cents:
            return False
        booked = [fill for fill in bridge.fills if fill[0] <= adopted_through_ms]
        booked_cents = sum(fill[2] for fill in booked)
        if not booked_cents:
            return None
        booked_price = sum(fill[3] * fill[2] for fill in booked) / booked_cents
        filled = 0.0
        held: str | None = None
        try:
            with self._judgment_span():
                current = store.get_order(row.client_order_id)
                donor = store.get_order(adopted.client_order_id)
                if (current is None or current.closed_ts_ms is not None
                        or current.state not in PENDING_DISPATCH_STATES
                        or current.exchange_order_id
                        or donor is None or donor.closed_ts_ms is not None
                        or not (donor.extras or {}).get('recovered_own_leg')):
                    return False
                filled = min(current.qty, volume_to_units(booked_cents))
                donor_exposure = self._row_exposure(donor)
                if current.side != donor.side:
                    held = f"the adopted row is a {donor.side} leg, the order a {current.side}"
                elif filled - donor_exposure > 1e-9:
                    held = (f"the adopted row holds {donor_exposure} units, less than the "
                            f"{filled} the order filled before the adoption")
                if held is None:
                    if donor_exposure - filled > 1e-9:
                        store.upsert_order(
                            donor.client_order_id, qty=donor.filled_qty - filled,
                            filled_qty=donor.filled_qty - filled)
                    else:
                        store.close_order(donor.client_order_id)
                    self._confirm_recovered_entry(
                        current, order_id=order.orderId, position_id=order.positionId,
                        filled=filled, avg_price=booked_price,
                        submitted_at_ms=order.utcLastUpdateTimestamp or int(epoch_time() * 1000),
                        source='adopted_leg',
                    )
                    store.log_event(
                        _ADOPTED_LEG_TAKEN_OVER_EVENT, client_order_id=current.client_order_id,
                        exchange_order_id=str(order.orderId), intent_key=current.intent_key,
                        payload={'position_id': order.positionId,
                                 'adopted_coid': donor.client_order_id,
                                 'qty': filled,
                                 'adopted_remaining': max(0.0, donor_exposure - filled),
                                 'adopted_through_ms': adopted_through_ms})
        except Exception as exc:  # noqa: BLE001 - a rolled-back transfer is retried whole
            logger.error(
                "cTrader dispatch %s: taking over adopted row %s failed, nothing changed: %s",
                row.client_order_id, adopted.client_order_id, exc)
            return False
        if held is not None:
            self._note_takeover_held(row, order, adopted, held)
            return False
        self._seen_deal_ids.update(fill[1] for fill in booked)
        logger.warning(
            "cTrader dispatch %s: its order %d filled %s units before the restart into "
            "position %d, which startup adopted as row %s; the dispatch takes them over",
            row.client_order_id, order.orderId, filled, order.positionId,
            adopted.client_order_id)
        return True

    def _note_takeover_held(self, row, order, adopted, reason: str) -> None:
        """Report, once per dispatch, an adopted-leg takeover the evidence does not prove.

        :param row: The inherited dispatch row, which stays parked.
        :param order: Its venue order.
        :param adopted: The ``recovered_own_leg`` row of the order's position.
        :param reason: Why the transfer is not proven.
        """
        store = self.store_ctx
        assert store is not None
        coid = row.client_order_id
        if any(event_coid == coid for _intent, event_coid, _xoid, _payload
               in store.iter_events_by_kind_for_run_id(_ADOPTED_LEG_TAKEOVER_HELD_EVENT)):
            return
        logger.warning(
            "cTrader dispatch %s (intent %s): its order %d filled into position %d before the "
            "adoption, which startup adopted as row %s, but %s; which deals that row covers "
            "is not proven, so the dispatch stays parked and its entry is held. Check the "
            "position on the venue",
            coid, row.intent_key, order.orderId, order.positionId, adopted.client_order_id,
            reason)
        store.log_event(
            _ADOPTED_LEG_TAKEOVER_HELD_EVENT, client_order_id=coid,
            exchange_order_id=str(order.orderId), intent_key=row.intent_key,
            payload={'position_id': order.positionId, 'adopted_coid': adopted.client_order_id,
                     'reason': reason})

    def _maybe_abandon_unknown(self, row) -> None:
        """Retire a still-unknown row once past the evidence-gated TTL.

        Reached only on a COMPLETE order-history read that found no coid match:
        the order either never reached the broker (crash after the persist-first
        write, before the wire send) or is a broker-side state cTrader does not
        surface. Either way the row is retired as ``abandoned_unknown`` (inferred
        absence, not proven non-delivery — the strategy re-derives exposure from
        the adopted net broker state) only after a minutes-scale TTL, so a
        merely-slow history-indexing window never abandons a row that did land.
        """
        if self.store_ctx is None:
            return
        age_s = epoch_time() - (row.created_ts_ms / 1000.0)
        if age_s < _ABANDON_TTL_S:
            # Too young: the row stays pending, in whatever pending state. It
            # was left by an earlier process, so the running bot looks it up
            # again at the reconcile cadence (:meth:`_resolve_parked_dispatches`)
            # and the engine holds the intent back until that lookup resolves
            # it (:func:`~pynecore.core.broker.parked_dispatch.inherited_dispatch_in_flight`).
            return
        DispatchJournal(self.store_ctx).apply_reconcile_outcome(
            row.client_order_id,
            ReconcileOutcome(
                kind='terminal_close',
                reason='recovered_abandoned_unknown',
                new_state='rejected',
                audit_event='recovered_abandoned_unknown',
                close_row=True,
                audit_payload={'age_s': round(age_s, 1), 'state': row.state},
            ),
        )
        self._clear_intent_anchor(row)

    def _clear_intent_anchor(self, row) -> None:
        """Delete the envelope + parked verifications of a terminally-retired row.

        ``close_order`` (run by the terminal writer) leaves the envelope anchor
        and any parked ``pending_verifications`` for ``row.intent_key`` behind —
        the same hazard :meth:`_retire_startup_orphans` clears. Without this, a
        restart's ``replay()`` re-surfaces the stale anchor and the next dispatch
        of the same Pine intent rebuilds the SAME coid (same ``bar_ts_ms``) onto
        the just-closed row; ``upsert_order`` then updates the closed row without
        clearing ``closed_ts_ms``, hiding the fresh entry from
        ``iter_live_orders``. ``record_complete`` deletes both in one transaction.
        """
        if self.store_ctx is None or not row.intent_key:
            return
        self.store_ctx.record_complete(row.intent_key)

    @staticmethod
    def _recovery_lower_bound_ms(row) -> int:
        """Lower bound (ms) for the recovery history window (dual-timestamp).

        The persist-first row's ``created_ts_ms`` IS the dispatch-start instant
        (the row is written immediately before the wire send), and a
        confirmed-then-pending row may also carry the broker-clock
        ``submitted_at_ms``. Use the EARLIER, minus a skew margin, so a fill /
        terminal event can never sit before the window.
        """
        candidates = [row.created_ts_ms]
        submitted_at_ms = (row.extras or {}).get('submitted_at_ms')
        if submitted_at_ms is not None:
            candidates.append(parse_protocol_id(
                submitted_at_ms, field='submitted_at_ms',
            ))
        return min(candidates) - _SINCE_ANCHOR_SKEW_MS

    @classmethod
    def _recovered_since_anchor_ms(cls, row, broker_ts_ms: int) -> int:
        """The ``submitted_at_ms`` since-anchor to record for a recovered order.

        A recovered order carries only its ``utcLastUpdateTimestamp``, the time
        of its LAST change: an order filled in several executions, or one that
        filled and was then cancelled, changed last well after its first fill.
        Anchored there, the deal-history window of the reconcile pass would
        start past the earlier fills and recover only part of them. The row's
        own dispatch start (and any anchor it already carries) precedes every
        execution, so the earliest of them is the anchor.

        :param row: The recovered persist-first row.
        :param broker_ts_ms: The order's broker-clock timestamp.
        :return: The anchor in ms.
        """
        return min(broker_ts_ms, cls._recovery_lower_bound_ms(row) + _SINCE_ANCHOR_SKEW_MS)

    async def _pending_position_orders(
            self, row, order_id: int, position_id: int, deals: Sequence[_PositionDeal],
            orders_by_coid: Mapping[str, OpenApiModelMessages.ProtoOAOrder],
    ) -> dict[str, tuple[int, int]] | None:
        """The orders of the run's other pending dispatch rows that filled onto ``position_id``.

        A pending row's order is the one the order history echoes its client
        order id on (its recorded refs name the same order, but carry no fill);
        one the live snapshot still shows working is left to its own recovery.
        Every pending row's order is read on every pass, so the
        ``executedVolume`` it reports is known each time the group is judged,
        not only on the pass that first found the order. A row joins when its
        order opened volume in ``deals``, or when the order history ties an
        order with executed volume to the position before any of its deals
        shows: its evidence is then short (:meth:`_fill_history_short`), and the
        group waits.

        :param row: The pending row being recovered.
        :param order_id: Its venue order.
        :param position_id: The closed position the group retires.
        :param deals: Every deal of the position's history read.
        :param orders_by_coid: The live snapshot's working orders by client order id.
        :return: Client order id -> ``(venue order, its executedVolume, 0 when
            the order history no longer shows it)``
            (:meth:`_unbooked_position_entries`); ``None`` while an
            order-history read is inconclusive.
        """
        if self.store_ctx is None:
            return {}
        pending = [other for other in find_pending_dispatch(self.store_ctx)
                   if other.client_order_id != row.client_order_id
                   and other.client_order_id not in orders_by_coid]
        pending_coids = {other.client_order_id for other in pending}
        linked = {order_id}
        for other in self.store_ctx.iter_live_orders():
            raw = (other.extras or {}).get('order_id')
            if raw is not None and other.client_order_id not in pending_coids:
                linked.add(parse_protocol_id(raw, field='order_id'))
        unlinked = {deal.order_id for deal in deals if deal.opened > 0} - linked
        found: dict[str, tuple[int, int]] = {}
        for other in pending:
            raw = (other.extras or {}).get('order_id')
            lookup = await self._find_order_by_coid(
                other.client_order_id, self._recovery_lower_bound_ms(other))
            if lookup.order is None and not lookup.conclusive:
                return None
            executed = 0
            tied = False
            if lookup.order is not None:
                other_id: int | None = lookup.order.orderId
                executed = int(lookup.order.executedVolume)
                tied = executed > 0 and lookup.order.positionId == position_id
            else:
                other_id = parse_protocol_id(raw, field='order_id') if raw is not None else None
            if other_id is not None and other_id not in linked and (
                    other_id in unlinked or tied):
                found[other.client_order_id] = (other_id, executed)
                linked.add(other_id)
                unlinked.discard(other_id)
        return found

    async def _find_order_by_coid(
            self, coid: str, from_ms: int,
    ) -> '_OrderLookupResult':
        """Page the order history for the ``ProtoOAOrder`` echoing ``coid``.

        Walks ``[from_ms, now]`` paging on ``hasMore`` (``ProtoOAOrderListReq``
        carries no ``maxRows``; the window is narrowed to the oldest page
        timestamp, inclusive, so the orders sharing that timestamp that the page
        cut off are read by the next page). ``conclusive`` is ``True`` only when
        the whole window was consumed without a transport error, so a not-found
        result is deterministic absence — the caller never abandons a row from a
        truncated read. A page that announces more but cannot narrow the window
        (empty, without timestamps, entirely at the window's upper bound, or
        already at its lower bound) leaves the rest unread: inconclusive.
        """
        wire = self._wire
        if wire is None or self._live_account_id is None:
            return _OrderLookupResult(None, False)
        to_ms = int(epoch_time() * 1000)
        try:
            while True:
                res = cast(OpenApiMessages.ProtoOAOrderListRes, await wire.send_request(
                    OpenApiMessages.ProtoOAOrderListReq(
                        ctidTraderAccountId=self._live_account_id,
                        fromTimestamp=from_ms, toTimestamp=to_ms,
                    )
                ))
                for order in res.order:
                    if order.clientOrderId == coid:
                        return _OrderLookupResult(order, True)
                if not res.hasMore:
                    break
                oldest = min((o.utcLastUpdateTimestamp for o in res.order
                              if o.utcLastUpdateTimestamp), default=0)
                if not oldest or oldest <= from_ms or oldest >= to_ms:
                    logger.warning(
                        "cTrader order-history recovery for coid %s cannot page past %d: "
                        "the window stays unread", coid, to_ms)
                    return _OrderLookupResult(None, False)
                to_ms = oldest
        except Exception as exc:  # noqa: BLE001 - read failure is inconclusive evidence
            logger.warning(
                "cTrader order-history recovery for coid %s failed: %s", coid, exc)
            return _OrderLookupResult(None, False)
        return _OrderLookupResult(None, True)

    async def _retire_startup_orphans(
            self, res: 'OpenApiMessages.ProtoOAReconcileRes', promoted_coids: set[str],
    ) -> None:
        """Retire live rows whose broker counterpart is gone (single pass).

        cTrader brackets are position attributes (``execute_exit`` persists NO
        separate TP/SL leg rows), so unlike Capital.com's two-pass retirement
        this is a single pass over the entry rows: a ``confirmed`` / ``closing``
        / ``rejected`` row whose ``orderId`` / ``positionId`` is absent from BOTH
        the live ``order[]`` and ``position[]`` snapshots is an orphan: its
        position was closed while the bot was stopped. Close it + delete its
        envelope anchor so the runtime ``_reconcile_snapshot`` does not later
        stamp ``missing_pending_since`` and the grace tracker raise a false
        ``UnexpectedCancelError`` that halts a clean restart.

        Who closed the position is judged first, from its deal history
        (:meth:`_judge_recovered_closes`): someone else's close is reported
        like one the running bot missed. A row whose history cannot be read or
        judged within the budget (:data:`REQUEST_TIMEOUT` for the whole pass)
        stays live; the runtime reconcile retires it through the grace
        tracker, which judges the same evidence. Right after an operator
        resume the close is accepted instead: the operator accepted the venue
        as it stands.

        ``promoted_coids`` (rows the in-flight recovery just confirmed) are
        skipped: a freshly opened position may not yet be in this snapshot.
        """
        if self.store_ctx is None:
            return
        live_order_ids = {str(o.orderId) for o in res.order}
        live_position_ids = {str(p.positionId) for p in res.position}
        retired = 0
        self._reconcile_pass_deadline = asyncio.get_running_loop().time() + REQUEST_TIMEOUT
        try:
            for row in list(self.store_ctx.iter_live_orders()):
                if row.client_order_id in promoted_coids:
                    continue
                if await self._retire_startup_orphan(
                        row, live_order_ids=live_order_ids,
                        live_position_ids=live_position_ids):
                    retired += 1
        finally:
            self._reconcile_pass_deadline = None
        if retired:
            logger.info(
                "cTrader startup: retired %d orphan order row(s) — no matching "
                "order/position on exchange", retired)

    async def _retire_startup_orphan(
            self, row, *, live_order_ids: set[str], live_position_ids: set[str],
    ) -> bool:
        """Retire one row if it is an orphan whose close could be judged.

        :return: Whether the row was retired.
        """
        assert self.store_ctx is not None
        if row.state not in ('confirmed', 'closing', 'rejected'):
            return False
        extras = row.extras or {}
        order_id = extras.get('order_id')
        position_id = extras.get('position_id')
        ref = row.exchange_order_id
        present = (
                (ref is not None and (ref in live_order_ids or ref in live_position_ids))
                or (order_id is not None and str(order_id) in live_order_ids)
                or (position_id is not None and str(position_id) in live_position_ids)
        )
        if present:
            return False
        if ref is None and order_id is None and position_id is None:
            # No broker handle at all — cannot prove it is an orphan; leave it
            # for the runtime reconcile rather than retire a row blindly.
            return False
        if (position_id is None and row.filled_qty < row.qty - 1e-9
                and order_id is not None):
            # An un-promoted working order (LIMIT / STOP) whose only handle is
            # an ``order_id`` absent from ``order[]``. cTrader sheds a filled
            # working order from ``order[]`` and the resulting ``position[]``
            # entry carries no coid / orderId link, so a fill that landed while
            # the bot was offline looks identical to a cancel here. Retiring it
            # would close a freshly filled position's row before the runtime
            # ``_reconcile_snapshot`` deal-history bridge can promote it, so
            # leave it pending for that evidence-gated resolution.
            return False
        if position_id is not None and order_id is not None:
            if _accepted_by_resume(row):
                self._note_accepted_by_resume(
                    parse_protocol_id(position_id, field='position_id'),
                    client_order_id=row.client_order_id, volume=None)
            elif not await self._judge_orphan_close(row, order_id, position_id):
                return False
        self.store_ctx.log_event(
            'startup_orphan_retired', client_order_id=row.client_order_id,
            exchange_order_id=ref,
            payload={'state': row.state, 'order_id': order_id,
                     'position_id': position_id},
        )
        self.store_ctx.close_order(row.client_order_id)
        # The envelope anchor survives ``close_order``; without this delete
        # the next dispatch of the same Pine intent reuses the stale
        # ``bar_ts_ms`` and regenerates the SAME coid onto the just-closed
        # row, hiding the fresh entry from ``iter_live_orders``.
        if row.intent_key:
            self.store_ctx.record_complete(row.intent_key)
        return True

    async def _judge_orphan_close(self, row, order_id, position_id) -> bool:
        """Judge who closed an orphan's position, from its deal history.

        The closing deals are seeded into the fill de-dup set, so a PUSH
        replay of them after the stream opens is not booked again.

        :return: ``False`` when the history could not be read or a closing
            order not looked up yet: the row stays live.
        """
        pid = parse_protocol_id(position_id, field='position_id')
        bridge = await self._find_fill_deal(
            parse_protocol_id(order_id, field='order_id'),
            self._recovery_lower_bound_ms(row), close_position_id=pid,
            deadline=self._reconcile_pass_deadline)
        if not bridge.conclusive:
            return False
        if bridge.closed_cents > 0:
            if await self._judge_recovered_closes(
                    pid, bridge.position_deals, from_entry=row.pine_entry_id,
                    counted=bridge.deal_ids) is None:
                return False
            self._seen_deal_ids.update(bridge.closing_deal_ids)
        return True
