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
  the order-history bridge does. No net-position inference by label / symbol.
* **Never re-issue.** A row whose outcome cannot be read from either source
  stays pending (``still_unknown``); the recoverer never dispatches a new order.
* **Evidence-gated TTL.** A ``still_unknown`` row is retired
  (``recovered_abandoned_unknown``) only when the elapsed time passed a
  minutes-scale TTL AND the order-history read was complete (paginated to
  exhaustion, no transport error) and carried no match. A truncated / failed
  read is non-evidence → the row stays pending for the next reconcile re-entry.
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
import logging
from dataclasses import dataclass
from time import time as epoch_time
from typing import cast

from pynecore.core.broker.journal import DispatchJournal, ReconcileOutcome
from pynecore.core.broker.store_helpers import find_pending_dispatch

from .helpers import volume_to_units
from .messages import OpenApiMessages_pb2 as _oa
from .messages import OpenApiModelMessages_pb2 as _model
from .reconcile import _SINCE_ANCHOR_SKEW_MS, _ReconcileMixin

logger = logging.getLogger(__name__)

#: Minutes-scale liveness escape for a ``still_unknown`` pending row. A crash
#: after the persist-first row write but before the wire send leaves a row the
#: broker never saw; without a TTL it would block the strategy forever. The TTL
#: must exceed the worst-case send-latency + broker-processing + history-indexing
#: lag + reconnect-jitter + clock-skew, and stay below the guaranteed history
#: depth. Minutes, NOT the 25s ``pending_missing_grace`` (that is the everyday
#: reconcile-noise window — a different policy).
_ABANDON_TTL_S = 600.0


@dataclass(frozen=True, slots=True)
class _OrderLookupResult:
    """Order-history lookup result for one pending row's coid.

    :ivar order: The matching ``ProtoOAOrder`` (carrying the coid echo), or
        ``None`` when nothing matched.
    :ivar conclusive: ``True`` only when the whole window was paginated without
        a transport error — a not-found result is then deterministic absence,
        never an inference from a truncated read.
    """
    order: '_model.ProtoOAOrder | None'
    conclusive: bool


_TERMINAL_ORDER_STATUSES = frozenset({
    _model.ProtoOAOrderStatus.ORDER_STATUS_REJECTED,
    _model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED,
    _model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED,
})


class _RecoveryMixin(_ReconcileMixin):
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
        has_live = next(iter(self.store_ctx.iter_live_orders()), None) is not None
        if not pending and not has_live:
            return
        res = await self._reconcile(return_protection_orders=True)
        orders_by_coid = {o.clientOrderId: o for o in res.order if o.clientOrderId}
        open_positions = {
            p.positionId: p for p in res.position
            if p.positionStatus == _model.ProtoOAPositionStatus.POSITION_STATUS_OPEN
        }
        promoted_coids: set[str] = set()
        for row in pending:
            if await self._recover_one_pending_row(row, orders_by_coid, open_positions):
                promoted_coids.add(row.client_order_id)
        self._retire_startup_orphans(res, promoted_coids)

    async def _recover_one_pending_row(
            self, row, orders_by_coid: 'dict[str, _model.ProtoOAOrder]',
            open_positions: 'dict[int, _model.ProtoOAPosition]',
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
                row, found.order, open_positions, from_ms)
        # Neither source carried the coid. Only a COMPLETE order-history read is
        # evidence of absence — a truncated / failed read stays still_unknown and
        # is retried by the next runtime reconcile re-entry. Never re-dispatch.
        if found.conclusive:
            self._maybe_abandon_unknown(row)
        return False

    async def _resolve_recovered_order(
            self, row, order, open_positions: 'dict[int, _model.ProtoOAPosition]',
            from_ms: int,
    ) -> bool:
        """Classify a recovered order's disposition from order + deal history."""
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
                # Filled then closed while down — seed the shared de-dup BEFORE
                # retiring so a reconnect PUSH replay of these deals is not
                # re-applied, then land the row closed through the existing writer.
                self._seen_deal_ids.update(bridge.deal_ids)
                self._retire_filled_then_closed(
                    row, bridge.deal, position_id, bridge.closed_cents)
                self._clear_intent_anchor(row)
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
        )
        return True

    def _confirm_recovered_entry(
            self, row, *, order_id: int, position_id: int | None,
            filled: float, avg_price: float | None, submitted_at_ms: int,
            source: str, partial_terminal: bool = False,
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
        extras['submitted_at_ms'] = submitted_at_ms
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
        self.store_ctx.log_event(
            'recovered_in_flight_confirmed', client_order_id=coid,
            exchange_order_id=str(order_id), intent_key=row.intent_key,
            payload={'position_id': position_id, 'filled_qty': cumulative,
                     'source': source, 'avg_price': avg_price},
        )

    def _record_recovered_refs(
            self, row, *, order_id: int, position_id: int | None,
            submitted_at_ms: int,
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
        history reads conclusively.
        """
        if self.store_ctx is None:
            return
        coid = row.client_order_id
        extras = dict(row.extras or {})
        extras['order_id'] = str(order_id)
        extras['position_id'] = position_id or None
        extras['submitted_at_ms'] = submitted_at_ms
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
            return  # too young — the next runtime reconcile re-entry rechecks it
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
        submitted_at_ms: int | None = (row.extras or {}).get('submitted_at_ms')
        if submitted_at_ms is not None:
            candidates.append(int(submitted_at_ms))
        return min(candidates) - _SINCE_ANCHOR_SKEW_MS

    async def _find_order_by_coid(
            self, coid: str, from_ms: int,
    ) -> '_OrderLookupResult':
        """Page the order history for the ``ProtoOAOrder`` echoing ``coid``.

        Walks ``[from_ms, now]`` paging on ``hasMore`` (``ProtoOAOrderListReq``
        carries no ``maxRows``; the window is narrowed by the oldest page
        timestamp). ``conclusive`` is ``True`` only when the whole window was
        consumed without a transport error, so a not-found result is
        deterministic absence — the caller never abandons a row from a truncated
        read.
        """
        wire = self._wire
        if wire is None or self._live_account_id is None:
            return _OrderLookupResult(None, False)
        to_ms = int(epoch_time() * 1000)
        try:
            while True:
                res = cast(_oa.ProtoOAOrderListRes, await wire.send_request(
                    _oa.ProtoOAOrderListReq(
                        ctidTraderAccountId=self._live_account_id,
                        fromTimestamp=from_ms, toTimestamp=to_ms,
                    )
                ))
                for order in res.order:
                    if order.clientOrderId == coid:
                        return _OrderLookupResult(order, True)
                if not res.hasMore or not res.order:
                    break
                oldest = min((o.utcLastUpdateTimestamp for o in res.order
                              if o.utcLastUpdateTimestamp), default=0)
                if not oldest or oldest <= from_ms:
                    break
                to_ms = oldest - 1
        except Exception as exc:  # noqa: BLE001 - read-completeness signal, not a handler
            logger.warning(
                "cTrader order-history recovery for coid %s failed: %s", coid, exc)
            return _OrderLookupResult(None, False)
        return _OrderLookupResult(None, True)

    def _retire_startup_orphans(
            self, res: '_oa.ProtoOAReconcileRes', promoted_coids: set[str],
    ) -> None:
        """Retire live rows whose broker counterpart is gone (single pass).

        cTrader brackets are position attributes (``execute_exit`` persists NO
        separate TP/SL leg rows), so unlike Capital.com's two-pass retirement
        this is a single pass over the entry rows: a ``confirmed`` / ``closing``
        / ``rejected`` row whose ``orderId`` / ``positionId`` is absent from BOTH
        the live ``order[]`` and ``position[]`` snapshots is an operator-closed
        orphan (the bot was stopped, then the position closed manually). Close
        it + delete its envelope anchor so the runtime ``_reconcile_snapshot``
        does not later stamp ``missing_pending_since`` and the grace tracker
        raise a false ``UnexpectedCancelError`` that halts a clean restart.

        ``promoted_coids`` (rows the in-flight recovery just confirmed) are
        skipped: a freshly opened position may not yet be in this snapshot.
        """
        if self.store_ctx is None:
            return
        live_order_ids = {str(o.orderId) for o in res.order}
        live_position_ids = {str(p.positionId) for p in res.position}
        retired = 0
        for row in list(self.store_ctx.iter_live_orders()):
            if row.client_order_id in promoted_coids:
                continue
            if row.state not in ('confirmed', 'closing', 'rejected'):
                continue
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
                continue
            if ref is None and order_id is None and position_id is None:
                # No broker handle at all — cannot prove it is an orphan; leave it
                # for the runtime reconcile rather than retire a row blindly.
                continue
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
                continue
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
            retired += 1
        if retired:
            logger.info(
                "cTrader startup: retired %d orphan order row(s) — no matching "
                "order/position on exchange", retired)
