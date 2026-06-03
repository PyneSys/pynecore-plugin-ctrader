"""Reconcile-snapshot loop mix-in for the cTrader Open API plugin.

The live ``ProtoOAExecutionEvent`` PUSH channel (:meth:`watch_orders`) is the
primary, real-time order-event source. This mix-in adds the periodic
``ProtoOAReconcileReq`` snapshot diff that GAP-FILLS the events lost while the
stream was down (a disconnect / reconnect window or a restart): it walks the
BrokerStore's live entry rows and emits the fills the PUSH path missed, stamps
the rows whose broker counterpart has vanished, and never duplicates or
regresses what the PUSH path already applied.

Why this is cTrader-native (and NOT a Capital.com port):

* cTrader brackets are *position attributes* (one ``ProtoOAAmendPositionSLTPReq``),
  so :meth:`execute_exit` persists NO separate TP/SL leg rows — the live order
  rows are entry rows only. There is therefore no bracket-leg disposition
  recovery, no per-poll ``record_resolution`` aggregation, none of the
  Capital.com bracket machinery.
* The reconcile snapshot carries deterministic identity only on ``order[]``
  (``ProtoOAOrder`` echoes ``orderId`` + ``clientOrderId`` + ``positionId``).
  ``ProtoOAPosition`` carries NO ``clientOrderId`` / ``orderId`` — only
  ``positionId``. So once a working LIMIT / STOP fully fills it LEAVES ``order[]``
  and the snapshot alone cannot tie the resulting position back to the row. The
  deterministic bridge for that one case is the deal history
  (``ProtoOADeal.orderId``), reused by the M3 startup recovery.

Idempotency (shared with the PUSH path): every transition is a monotonic
compare-and-set — ``position_id`` is set once, ``filled_qty`` only grows, a row
never regresses from closed to open, and the ``dealId`` de-dup set
(``_seen_deal_ids``) is the SAME one :meth:`watch_orders` uses, so a fill seen on
one path is never re-applied on the other.

Scope (2.1): existing ``confirmed`` local rows only. A crash between the entry
wire-send and its post-ack persist leaves no local row at all — that zero-row
recovery is the M3 startup-recovery's job, not this loop's.
"""
import logging
from dataclasses import dataclass
from time import time as epoch_time
from typing import AsyncIterator, cast

from pynecore.core.broker.journal import DispatchJournal, ReconcileOutcome
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

logger = logging.getLogger(__name__)

#: Fallback deal-history window (seconds) used ONLY when a row carries no
#: ``submitted_at_ms`` since-anchor — the zero-row crash-before-persist case the
#: M3 startup recovery (2.2) owns. Every row this loop persists carries the
#: anchor, so the runtime path is a per-order since-cursor, NOT this fixed
#: window: a fill never ages out of the lookback because the cursor reaches back
#: to the order's own submit time.
_DEAL_LOOKBACK_S = 300.0

#: Safety margin subtracted from a row's ``submitted_at_ms`` when deriving the
#: deal-history ``fromTimestamp``. ``submitted_at_ms`` is already a true lower
#: bound (the broker-clock order-creation time, never after a fill), so this
#: only absorbs clock granularity and the client-clock fallback skew.
_SINCE_ANCHOR_SKEW_MS = 60_000

#: ``maxRows`` per ``ProtoOADealListReq`` page.
_DEAL_PAGE_MAX_ROWS = 1000


@dataclass(frozen=True, slots=True)
class _DealBridgeResult:
    """Aggregated FILLED-deal evidence for one vanished working order.

    A single working order can fill across several partial deals at different
    prices, so the bridge sums their volume / commission and volume-weights
    their price rather than reporting a single deal's slice.

    :ivar deal: The most recent matching FILLED ``ProtoOADeal`` (identity /
        timestamp), or ``None`` when nothing matched.
    :ivar filled_cents: The order's cumulative filled volume in cents.
    :ivar avg_price: The volume-weighted execution price across the deals, or
        ``None`` when nothing matched.
    :ivar fee: The summed commission across the deals.
    :ivar conclusive: ``True`` only when the whole window was read without a
        transport error (so a no-fill result deterministically means no fill).
    :ivar closed_cents: Volume (cents) closed on this order's position over the
        window, summed from every deal's ``closePositionDetail.closedVolume``
        (closedVolume-primary closure evidence). ``0`` when nothing closed.
    :ivar deal_ids: Every matching FILLED ``dealId``, so the shared
        :attr:`_seen_deal_ids` de-dup spans an order that filled across several
        partial deals (not just the most-recent one).
    """
    deal: '_model.ProtoOADeal | None'
    filled_cents: int
    avg_price: float | None
    fee: float
    conclusive: bool
    closed_cents: int = 0
    deal_ids: tuple[int, ...] = ()


class _ReconcileMixin(_CTraderBase):
    """Periodic reconcile-snapshot diff: gap-fills events the PUSH path missed."""

    async def _reconcile_snapshot(self) -> AsyncIterator[OrderEvent]:
        """Diff the live reconcile snapshot against the BrokerStore live rows.

        Yields one :class:`OrderEvent` per fill the PUSH stream missed
        (working-order partial progress, or a working → position promotion
        recovered through the deal-history bridge), and stamps
        ``missing_pending_since`` on rows whose broker counterpart has
        deterministically vanished. Emits nothing — and stamps nothing — for an
        ambiguous observation; the next pass or the live PUSH event resolves it.

        A no-op when persistence is off (the test paths) or the live connection
        is not established.
        """
        if self.store_ctx is None:
            return
        res = await self._reconcile(return_protection_orders=True)
        orders_by_id = {o.orderId: o for o in res.order}
        open_positions = {
            p.positionId: p for p in res.position
            if p.positionStatus == _model.ProtoOAPositionStatus.POSITION_STATUS_OPEN
        }
        now_ts = epoch_time()
        for row in list(self.store_ctx.iter_live_orders()):
            extras = row.extras or {}
            order_id_str = extras.get('order_id')
            order = orders_by_id.get(int(order_id_str)) if order_id_str else None
            if order is not None:
                # The order is still in ``order[]`` — reconcile partial-fill
                # progress from ``executedVolume``. A partial that already linked
                # ``position_id`` does NOT divert the row to the position branch:
                # cTrader keeps the original order in ``order[]`` while assigning a
                # ``positionId`` after the first partial, so later partials must
                # still be diffed here.
                event = self._reconcile_working_row(row, order, open_positions, now_ts)
                if event is not None:
                    yield event
                continue
            position_id = extras.get('position_id')
            if position_id and row.filled_qty >= row.qty - 1e-9:
                # A fully-filled row whose order has left ``order[]`` is a settled
                # position: only track its disappearance.
                self._reconcile_position_row(row, int(position_id), open_positions, now_ts)
                continue
            if not order_id_str:
                continue
            # The working order is gone from ``order[]`` — it either fully filled
            # (and left for ``position[]``) or was cancelled / expired. The
            # snapshot cannot tell which (positions carry no order/COID link), so
            # bridge through the deal history. A row that earlier partial-filled
            # (``position_id`` linked, residual still unfilled) lands here too: its
            # remaining quantity may have filled while the stream was down, so it
            # must be recovered through the same bridge — never treated as settled.
            async for event in self._recover_vanished_working_row(
                    row, int(order_id_str), open_positions, now_ts):
                yield event

    def _reconcile_position_row(
            self, row, position_id: int, open_positions: dict, now_ts: float,
    ) -> None:
        """Track disappearance of an already-promoted position row.

        Clears any stale ``missing_pending_since`` when the position is back in
        the snapshot; stamps it once when the position has vanished from
        ``position[]`` without a corresponding close the PUSH path handled.
        Rows flagged ``natural_close_at`` (a TP / SL fired, the position was
        expected to vanish) are skipped so the grace tracker cannot later raise
        a false cancel. Stamping only — the grace-window raise / synthetic
        cancel is a later milestone.
        """
        if self.store_ctx is None:
            return
        extras = row.extras or {}
        if extras.get('natural_close_at') is not None:
            return
        present = position_id in open_positions
        if present:
            if 'missing_pending_since' in extras:
                self._clear_missing_pending(row)
            return
        if 'missing_pending_since' not in extras:
            patched = dict(extras)
            patched['missing_pending_since'] = now_ts
            self.store_ctx.upsert_order(row.client_order_id, extras=patched)

    def _reconcile_working_row(
            self, row, order, open_positions: dict, now_ts: float,
    ) -> OrderEvent | None:
        """Detect partial-fill progress on a still-pending working order.

        ``ProtoOAOrder`` carries ``positionId`` once any volume has filled, so a
        partial fill is deterministic from ``order[]`` alone: link the
        ``positionId`` (set-once) and, when the cumulative ``executedVolume`` has
        grown past the row's recorded ``filled_qty``, emit a partial-fill event.
        A working order that is back after a stale-missing stamp clears it.
        """
        if self.store_ctx is None:
            return None
        extras = row.extras or {}
        if 'missing_pending_since' in extras:
            self._clear_missing_pending(row)
            extras = row.extras or {}
        filled = volume_to_units(order.executedVolume)
        if filled <= row.filled_qty + 1e-9 or filled >= row.qty:
            return None
        position_id = order.positionId or None
        patch: dict = {}
        if position_id and extras.get('position_id') is None:
            patch['position_id'] = position_id
        DispatchJournal(self.store_ctx).apply_reconcile_outcome(
            row.client_order_id,
            ReconcileOutcome(
                kind='filled',
                reason='partial_fill_progress',
                new_state='confirmed',
                audit_event='reconcile_partial_fill',
                filled_qty=filled,
                extras_patch=patch or None,
                audit_payload={'cumulative': filled, 'previous': row.filled_qty},
                exchange_order_id=str(order.orderId),
            ),
        )
        if position_id:
            self._link_position_ref(row.client_order_id, position_id)
        avg = (open_positions[position_id].price
               if position_id in open_positions else order.executionPrice)
        return self._fill_event(
            row, event_type='partial', status=OrderStatus.PARTIALLY_FILLED,
            exchange_id=str(order.orderId), cumulative=filled,
            fill_qty=filled - row.filled_qty, avg_price=avg or None,
            fill_price=order.executionPrice or None, fee=0.0, timestamp=now_ts,
        )

    async def _recover_vanished_working_row(
            self, row, order_id: int, open_positions: dict, now_ts: float,
    ) -> AsyncIterator[OrderEvent]:
        """Bridge a vanished working order to its outcome via the deal history.

        The order left ``order[]``; the deal history (keyed on ``orderId``,
        which the snapshot positions lack) is the deterministic source for
        whether it filled. On a FILLED deal: promote the row to a position
        (set ``position_id`` once, advance ``filled_qty``) and emit the fill the
        PUSH stream missed. On a conclusive no-fill (the history was fully
        paginated and carried no FILLED deal): stamp ``missing_pending_since``
        so a later milestone's grace window can retire it. On an inconclusive
        / failed history query: do nothing — the next pass or the PUSH event
        resolves it (never conclude a cancel from a truncated read).
        """
        if self.store_ctx is None:
            return
        extras = row.extras or {}
        anchor = extras.get('submitted_at_ms')
        if anchor:
            from_ms = int(anchor) - _SINCE_ANCHOR_SKEW_MS
        else:
            # Zero-anchor fallback (the crash-before-persist row the M3 startup
            # recovery owns): a wide fixed window, never the runtime path.
            from_ms = int(epoch_time() * 1000) - int(_DEAL_LOOKBACK_S * 1000)
        bridge = await self._find_fill_deal(order_id, from_ms)
        if bridge.deal is not None:
            event = self._promote_from_deal(row, bridge, open_positions, now_ts)
            if event is not None:
                yield event
            return
        # Conclusive no-fill: stamp the disappearance breadcrumb only on positive
        # no-fill evidence. A row that already linked a ``position_id`` or carries
        # any fill is a live (possibly partial) position — never a cancelled
        # pending — so stamping ``missing_pending_since`` there would later raise
        # a false ``UnexpectedCancelError`` against an open position (surface c).
        if (bridge.conclusive
                and not extras.get('position_id')
                and row.filled_qty <= 1e-9
                and 'missing_pending_since' not in extras):
            patched = dict(extras)
            patched['missing_pending_since'] = now_ts
            self.store_ctx.upsert_order(row.client_order_id, extras=patched)

    def _promote_from_deal(
            self, row, bridge: '_DealBridgeResult', open_positions: dict, now_ts: float,
    ) -> OrderEvent | None:
        """Promote a working row to a filled position from its FILLED deals.

        Closure-aware (closedVolume-primary): when the order's ``positionId`` is
        gone from the open snapshot and the deal history closed it, the order
        filled-then-closed while the stream was down — the row is retired rather
        than promoted, so no phantom open position is emitted (surface d). When
        the position is still open the order's own cumulative fill is recovered
        and the missed fill emitted.

        Idempotent against the PUSH path: every recovered ``dealId`` already in
        the shared :attr:`_seen_deal_ids` set was applied live, so the cursor /
        ``position_id`` link are reconciled but nothing is emitted.
        """
        deal = bridge.deal
        if self.store_ctx is None or deal is None:
            return None
        position_id = deal.positionId or None
        position_open = bool(position_id) and position_id in open_positions
        # Closure classification. A vanished order whose ``positionId`` is no
        # longer open filled then closed during the stream gap. With positive
        # closure evidence (a deal on the position carried
        # ``closePositionDetail.closedVolume``) retire the row through the
        # existing terminal-close writer instead of promoting a phantom open
        # position; the engine's own ``reconcile()`` adoption / shrink-to-zero
        # owns the position-size side. On ambiguity (position gone but no
        # closing deal observed in the window) do nothing — never fabricate a
        # retirement from missing evidence; the next pass or PUSH resolves it.
        if position_id and not position_open:
            if bridge.closed_cents > 0 and bridge.filled_cents > 0:
                # Record the recovered deals on the shared de-dup channel BEFORE
                # retiring: cTrader can replay or push an uncorrelated copy of the
                # same execution after reconnect, and a sibling row sharing this
                # position is retired off the SAME bridge deals — without this the
                # PUSH path would re-emit a recovered deal as a fresh fill.
                self._seen_deal_ids.update(bridge.deal_ids)
                self._retire_filled_then_closed(
                    row, deal, position_id, bridge.closed_cents)
            return None
        # A working order that vanished after only a PARTIAL fill (residual
        # cancelled / expired while the stream was down) filled less than
        # ``row.qty``. The authoritative recovered size is THIS order's own
        # cumulative ``filledVolume`` (summed across its FILLED deals) — never the
        # net open-position volume, which on a NETTING / pyramiding account is
        # shared across other entries and would overstate this order's fill.
        # Clamp into ``[row.filled_qty, row.qty]`` so it never regresses or
        # overstates the strategy position past the order's own size; the clamp
        # is the monotonic guarantee (``mark_reconcile_filled`` writes the
        # absolute value with no max of its own).
        new_deal_ids = [d for d in bridge.deal_ids if d not in self._seen_deal_ids]
        recovered = volume_to_units(bridge.filled_cents)
        cumulative = min(row.qty, max(row.filled_qty, recovered))
        link_position = bool(position_id) and (row.extras or {}).get('position_id') is None
        if cumulative > row.filled_qty + 1e-9 or link_position:
            DispatchJournal(self.store_ctx).apply_reconcile_outcome(
                row.client_order_id,
                ReconcileOutcome(
                    kind='filled',
                    reason='working_promoted_position',
                    new_state='confirmed',
                    audit_event='reconcile_working_promoted',
                    filled_qty=cumulative,
                    extras_patch={'position_id': position_id} if link_position else None,
                    audit_payload={'deal_id': deal.dealId, 'position_id': position_id},
                    exchange_order_id=str(position_id or deal.orderId),
                ),
            )
            if link_position:
                self._link_position_ref(row.client_order_id, position_id)
        if not new_deal_ids:
            # Every recovered deal was already applied by the PUSH path — the
            # cursor / position link are now consistent, so emit nothing.
            return None
        self._seen_deal_ids.update(new_deal_ids)
        # ``fill_price`` is what ``BrokerPosition.record_fill`` books the whole
        # recovered quantity at, so it must be the volume-weighted average across
        # the order's FILLED deals — a single deal's ``executionPrice`` would
        # mis-state P&L when the fill spanned several deals at different prices.
        # ``avg_price`` (the ExchangeOrder snapshot's display field) still prefers
        # the open position's broker-reported average when present.
        bridge_price = bridge.avg_price or deal.executionPrice or None
        avg = (open_positions[position_id].price
               if position_open else bridge_price)
        status = (OrderStatus.FILLED if cumulative >= row.qty - 1e-9
                  else OrderStatus.PARTIALLY_FILLED)
        return self._fill_event(
            row, event_type='filled', status=status,
            exchange_id=str(position_id or deal.orderId), cumulative=cumulative,
            fill_qty=max(0.0, cumulative - row.filled_qty), avg_price=avg or None,
            fill_price=bridge_price, fee=bridge.fee,
            timestamp=(deal.executionTimestamp / 1000.0
                       if deal.executionTimestamp else now_ts),
        )

    def _retire_filled_then_closed(
            self, row, deal, position_id: int, closed_cents: int,
    ) -> None:
        """Retire a row whose order filled then closed while the stream was down.

        The position the order opened is gone from the live snapshot and the
        deal history carried a ``closePositionDetail`` closing it, so promoting
        the opening fill would leave a phantom local position against a broker
        that is flat for it. Lands the row in ``closed`` through the existing
        reconcile-path terminal writer and emits no fill — the entry is settled,
        not live; the engine's own position reconcile owns the size side.

        Flattens EVERY live row sharing ``position_id``, not just the one
        observed: a NETTING / pyramiding account merges pyramid entries onto one
        ``positionId``, so a proven close retires all of them (mirroring the PUSH
        path's :meth:`_mark_position_closed`) — closing only the observed row
        would leave sibling rows live or later mis-stamped against a flat broker.
        Siblings are materialised before any write so the live-order scan is not
        mutated mid-iteration.
        """
        if self.store_ctx is None:
            return
        pid_str = str(position_id)
        siblings = [
            r.client_order_id
            for r in self.store_ctx.iter_live_orders()
            if r.client_order_id != row.client_order_id
            and ((r.extras or {}).get('position_id') == position_id
                 or r.exchange_order_id == pid_str)
        ]
        DispatchJournal(self.store_ctx).apply_reconcile_outcome(
            row.client_order_id,
            ReconcileOutcome(
                kind='terminal_close',
                reason='bracket_natural_close_followup',
                new_state='closed',
                audit_event='reconcile_filled_then_closed_retired',
                close_row=True,
                audit_payload={'deal_id': deal.dealId, 'position_id': position_id,
                               'closed_cents': closed_cents},
                exchange_order_id=str(position_id),
            ),
        )
        for coid in siblings:
            self.store_ctx.close_order(coid)

    async def _find_fill_deal(
            self, order_id: int, from_ms: int,
    ) -> '_DealBridgeResult':
        """Return the FILLED-deal evidence for ``order_id`` and read completeness.

        Walks the deal history from ``from_ms`` (the order's own since-anchor)
        up to now, paging on ``hasMore``, and aggregates EVERY FILLED deal of
        ``order_id`` in the window — a single working order can fill across
        several partial deals, so the per-order quantity is their summed
        ``filledVolume`` (in cents), never a single deal's slice. The deals can
        fill at DIFFERENT prices, so the booking price is their volume-weighted
        average (``avg_price``) and the recovered commission is their sum
        (``fee``). Alongside, it sums ``closePositionDetail.closedVolume`` per
        position so the caller can tell a filled-then-closed order from a live
        one (closedVolume-primary closure classification). ``conclusive`` is
        ``True`` only when the whole window was consumed without a transport
        error — a failed or truncated read returns an empty, non-conclusive
        result so the caller never concludes a cancel (or a closure) from
        missing evidence.

        :param order_id: The broker order id whose fills to recover.
        :param from_ms: The lower-bound ``fromTimestamp`` (ms) — the order's
            ``submitted_at_ms`` anchor minus a skew margin, so a fill never ages
            out of a fixed window.
        :return: a :class:`_DealBridgeResult` — see its field docs.
        """
        wire = self._wire
        if wire is None or self._live_account_id is None:
            return _DealBridgeResult(None, 0, None, 0.0, False)
        to_ms = int(epoch_time() * 1000)
        latest_deal: '_model.ProtoOADeal | None' = None
        filled_cents = 0
        weighted_price = 0.0
        fee = 0.0
        deal_ids: list[int] = []
        target_position_id: int | None = None
        closed_by_position: dict[int, int] = {}
        try:
            while True:
                res = cast(_oa.ProtoOADealListRes, await wire.send_request(
                    _oa.ProtoOADealListReq(
                        ctidTraderAccountId=self._live_account_id,
                        fromTimestamp=from_ms, toTimestamp=to_ms,
                        maxRows=_DEAL_PAGE_MAX_ROWS,
                    )
                ))
                for deal in res.deal:
                    if deal.HasField('closePositionDetail'):
                        closed_by_position[deal.positionId] = (
                            closed_by_position.get(deal.positionId, 0)
                            + deal.closePositionDetail.closedVolume)
                    if (deal.orderId == order_id
                            and deal.dealStatus == _model.ProtoOADealStatus.FILLED):
                        filled_cents += deal.filledVolume
                        weighted_price += deal.executionPrice * deal.filledVolume
                        fee += money_value(deal.commission, deal.moneyDigits)
                        deal_ids.append(deal.dealId)
                        if target_position_id is None:
                            target_position_id = deal.positionId or None
                        if latest_deal is None:
                            latest_deal = deal
                if not res.hasMore or not res.deal:
                    break
                oldest = min((d.executionTimestamp or d.createTimestamp)
                             for d in res.deal)
                if oldest <= from_ms:
                    break
                to_ms = oldest - 1
        except Exception as exc:  # noqa: BLE001 - read-completeness signal, not a handler
            logger.warning(
                "cTrader deal-history bridge for order %d failed: %s", order_id, exc,
            )
            return _DealBridgeResult(None, 0, None, 0.0, False)
        avg_price = weighted_price / filled_cents if filled_cents else None
        closed_cents = (closed_by_position.get(target_position_id, 0)
                        if target_position_id is not None else 0)
        return _DealBridgeResult(
            latest_deal, filled_cents, avg_price, fee, True,
            closed_cents, tuple(deal_ids),
        )

    def _clear_missing_pending(self, row) -> None:
        """Drop a stale ``missing_pending_since`` breadcrumb (the row came back)."""
        if self.store_ctx is None:
            return
        patched = {k: v for k, v in (row.extras or {}).items()
                   if k != 'missing_pending_since'}
        self.store_ctx.upsert_order(row.client_order_id, extras=patched)

    def _fill_event(
            self, row, *, event_type: str, status: OrderStatus, exchange_id: str,
            cumulative: float, fill_qty: float, avg_price: float | None,
            fill_price: float | None, fee: float, timestamp: float,
    ) -> OrderEvent:
        """Build the reconcile-recovered fill event for an entry row.

        Carries the row's Pine identity (``pine_entry_id`` / ``LegType.ENTRY``)
        so the sync engine books it against the originating entry, exactly as
        the PUSH path's entry-fill events do.
        """
        return OrderEvent(
            order=ExchangeOrder(
                id=exchange_id, symbol=row.symbol, side=row.side,
                order_type=OrderType.MARKET,
                qty=row.qty, filled_qty=cumulative,
                remaining_qty=max(0.0, row.qty - cumulative),
                price=None, stop_price=None, average_fill_price=avg_price,
                status=status, timestamp=timestamp, fee=fee, fee_currency='',
                reduce_only=False, client_order_id=row.client_order_id,
            ),
            event_type=event_type,
            fill_price=fill_price, fill_qty=fill_qty, timestamp=timestamp,
            pine_id=row.pine_entry_id, from_entry=row.from_entry,
            leg_type=LegType.ENTRY, fee=fee,
        )
