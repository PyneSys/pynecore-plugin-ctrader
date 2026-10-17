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
import asyncio
import logging
import uuid
from abc import ABC
from itertools import combinations
from dataclasses import dataclass, field
from time import time as epoch_time
from typing import TYPE_CHECKING, AsyncIterator, cast

from pynecore.core.broker.disappearance import (
    DisappearanceTracker,
    MissingConfirmation,
    MissingResolution,
)
from pynecore.core.broker.exceptions import BrokerError
from pynecore.core.broker.journal import DispatchJournal, ReconcileOutcome
from pynecore.core.broker.models import (
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
)

from .exceptions import is_not_found
from .helpers import (
    REQUEST_TIMEOUT,
    money_value,
    parse_protocol_id,
    raw_volume,
    volume_to_units,
)
from .messages import OpenApiMessages_pb2 as OpenApiMessages
from .messages import OpenApiModelMessages_pb2 as OpenApiModelMessages
from .protection import (
    _CLOCK_SKEW_MS,
    _VOLUME_TOL,
    CloseOrigin,
    _accepted_by_resume,
    _foreign_claims_taken,
    _PositionDeal,
    _ProtectionMixin,
)
from .wire import CTraderProtocolError, CTraderTimeoutError, CTraderWireError

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

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

#: Consecutive reconcile passes an open position must show the same shortfall
#: below the bot's share before its deal history is judged.
_SHRINK_CONFIRMATIONS = 2

#: Deal statuses that executed volume.
_EXECUTED_DEAL_STATUSES = frozenset({
    OpenApiModelMessages.ProtoOADealStatus.FILLED,
    OpenApiModelMessages.ProtoOADealStatus.PARTIALLY_FILLED,
})

#: Journal event of a shrink a restart found and retired from the journal; its
#: deals are judged later, by this process or a later one of the run.
_STARTUP_SHRINK_PENDING_EVENT = 'ctrader_startup_shrink_pending'

#: Journal event naming the pending startup shrinks whose deals were judged, or
#: that an operator resume accepted.
_STARTUP_SHRINK_DONE_EVENT = 'ctrader_startup_shrink_done'

#: Disappearance grace window (seconds). A bot-owned row stamped
#: ``missing_pending_since`` (vanished from BOTH ``order[]`` and ``position[]``)
#: is only treated as an unexpected cancel once this window elapses without the
#: row reappearing. ``max(5s, 5×reconcile-cadence)`` with the
#: ``_RECONCILE_CADENCE_S = 5.0`` of the PUSH loop — wide enough to absorb the
#: snapshot-vs-PUSH/ack skew (a fill in flight can flicker out of both for one
#: snapshot). Deliberately DECOUPLED from the cadence: the cadence is how often
#: the snapshot is read, the grace is how long to wait before concluding a
#: cancel.
_MISSING_PENDING_GRACE_S = 25.0


@dataclass(frozen=True, slots=True)
class _DealBridgeResult:
    """Aggregated FILLED-deal evidence for one vanished working order.

    A single working order can fill across several partial deals at different
    prices, so the bridge sums their volume, volume-weights their price and
    keeps every deal's own terms rather than reporting a single deal's slice.

    :ivar deal: The most recent matching FILLED ``ProtoOADeal`` (identity /
        timestamp), or ``None`` when nothing matched.
    :ivar filled_cents: The order's cumulative filled volume in cents.
    :ivar avg_price: The volume-weighted execution price across the deals, or
        ``None`` when nothing matched.
    :ivar conclusive: ``True`` only when the whole window was read without a
        transport error (so a no-fill result deterministically means no fill).
    :ivar closed_cents: Volume (cents) closed on this order's position over the
        window, summed from every deal's ``closePositionDetail.closedVolume``
        (closedVolume-primary closure evidence). ``0`` when nothing closed.
    :ivar deal_ids: Every matching FILLED ``dealId``, so the shared
        :attr:`_seen_deal_ids` de-dup spans an order that filled across several
        partial deals (not just the most-recent one).
    :ivar closing_deal_ids: ``dealId`` of every deal that closed volume on the
        resolved target position (``closePositionDetail``). These usually
        belong to a DIFFERENT order (a native TP/SL or an external close), so
        they are absent from :ivar:`deal_ids` — yet a closure concluded from
        them must seed the de-dup channel too, or a late PUSH replay of the
        closing execution would re-book against the retired position.
    :ivar position_deals: Every deal that executed volume on the resolved
        target position in the window, in execution order, for judging who
        closed it (:meth:`_ProtectionMixin._judge_recovered_closes`).
    :ivar fills: ``(timestamp_ms, deal_id, volume_cents, price, fee)`` of every
        matching FILLED deal, oldest first — the per-execution terms a
        recovered DELTA is priced and charged from when part of the order's
        fill was already booked (:meth:`_ReconcileMixin._promote_from_deal`,
        :meth:`_ReconcileMixin._reconcile_working_row`).
    """
    deal: 'OpenApiModelMessages.ProtoOADeal | None'
    filled_cents: int
    avg_price: float | None
    conclusive: bool
    closed_cents: int = 0
    deal_ids: tuple[int, ...] = ()
    closing_deal_ids: tuple[int, ...] = ()
    fills: tuple[tuple[int, int, int, float, float], ...] = ()
    position_deals: tuple[_PositionDeal, ...] = ()


@dataclass(slots=True)
class _StartupShrink:
    """A shrink a restart found and retired whose deals are not judged yet.

    :ivar volume: The bot's share (units) the startup baselines retired.
    :ivar since_ms: The earliest time (ms) a deal of the bot's on the position
        can carry.
    :ivar from_entry: The Pine entry the position belongs to.
    :ivar order_ids: The bot's entry orders on the position at the restart,
        which a closed journal row no longer resolves.
    :ivar keys: The journal keys of the pending records this one merges.
    """
    volume: float
    since_ms: int
    from_entry: str | None
    order_ids: list[int] = field(default_factory=list)
    keys: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _DealWalk:
    """Deal-history evidence for one order, gathered across reconcile passes.

    The window ``[from_ms, high_ms]`` is read backwards page by page; the pages
    still owed sit in ``segments`` as ``[from_ms, to_ms]`` pairs whose ``to_ms``
    moves down as pages arrive. Only the evidence the bridge needs is kept: the
    order's own FILLED deals and the volume each closing deal closed.

    :ivar from_ms: Lower bound of the window (the order's since-anchor).
    :ivar high_ms: Upper bound the window has been extended to so far.
    :ivar segments: Sub-windows not yet fully read, oldest first.
    :ivar fills: The order's FILLED deals by ``dealId``.
    :ivar closes: ``(positionId, closedVolume, orderId, executed_ms)`` by
        ``dealId`` of every deal in the window that closed volume.
    :ivar deals: ``(positionId, orderId, executed_ms, filledVolume,
        closedVolume)`` by ``dealId`` of every deal in the window that
        executed volume.
    :ivar last_request: Event-loop time the walk last sent a page request
        (``-inf`` before its first), so unfinished walks take turns.
    """
    from_ms: int
    high_ms: int
    segments: list[list[int]] = field(default_factory=list)
    fills: 'dict[int, OpenApiModelMessages.ProtoOADeal]' = field(default_factory=dict)
    closes: dict[int, tuple[int, int, int, int]] = field(default_factory=dict)
    deals: dict[int, tuple[int, int, int, int, int]] = field(default_factory=dict)
    last_request: float = float('-inf')

    def ingest(self, deal: 'OpenApiModelMessages.ProtoOADeal', order_id: int) -> None:
        """Record one deal-history row; a re-read row replaces its earlier copy.

        :param deal: A ``ProtoOADeal`` from a ``ProtoOADealListRes`` page.
        :param order_id: The order whose FILLED deals are collected.
        """
        # Only a deal that actually closed volume is closure EVIDENCE — a
        # zero-volume detail must not enter the dedup channel or the forensic
        # audit.
        if (deal.HasField('closePositionDetail')
                and deal.closePositionDetail.closedVolume > 0):
            self.closes[deal.dealId] = (
                deal.positionId, deal.closePositionDetail.closedVolume, deal.orderId,
                deal.executionTimestamp or deal.createTimestamp)
        if deal.filledVolume > 0 and deal.dealStatus in _EXECUTED_DEAL_STATUSES:
            self.deals[deal.dealId] = (
                deal.positionId, deal.orderId, deal.executionTimestamp or deal.createTimestamp,
                deal.filledVolume,
                deal.closePositionDetail.closedVolume
                if deal.HasField('closePositionDetail') else 0)
        if (deal.orderId == order_id
                and deal.dealStatus == OpenApiModelMessages.ProtoOADealStatus.FILLED):
            kept = OpenApiModelMessages.ProtoOADeal()
            kept.CopyFrom(deal)
            self.fills[deal.dealId] = kept

    def result(self, close_position_id: int | None) -> _DealBridgeResult:
        """Aggregate the gathered evidence of a fully read window.

        :param close_position_id: The fallback closure target — see
            :meth:`_ReconcileMixin._find_fill_deal`.
        :return: The conclusive :class:`_DealBridgeResult`.
        """
        fills = sorted(
            self.fills.values(),
            key=lambda d: (d.executionTimestamp or d.createTimestamp, d.dealId),
        )
        filled_cents = sum(d.filledVolume for d in fills)
        avg_price = (sum(d.executionPrice * d.filledVolume for d in fills) / filled_cents
                     if filled_cents else None)
        target_position_id = next(
            (d.positionId for d in reversed(fills) if d.positionId), None)
        closed_target = (target_position_id if target_position_id is not None
                         else close_position_id)
        closing_ids = tuple(
            deal_id for deal_id, (position_id, *_rest) in self.closes.items()
            if closed_target is not None and position_id == closed_target)
        closed_cents = sum(self.closes[deal_id][1] for deal_id in closing_ids)
        return _DealBridgeResult(
            fills[-1] if fills else None, filled_cents, avg_price, True,
            closed_cents, tuple(d.dealId for d in reversed(fills)), closing_ids,
            tuple(
                (d.executionTimestamp or d.createTimestamp, d.dealId, d.filledVolume,
                 d.executionPrice, money_value(d.commission, d.moneyDigits))
                for d in fills
            ),
            tuple(sorted(
                (_PositionDeal(deal_id=deal_id, order_id=order_ref, executed_ms=executed_ms,
                               filled=filled, closed=min(closed, filled))
                 for deal_id, (position_id, order_ref, executed_ms, filled, closed)
                 in self.deals.items()
                 if closed_target is not None and position_id == closed_target),
                key=lambda deal: (deal.executed_ms, deal.deal_id),
            )),
        )


class _ReconcileMixin(_ProtectionMixin, ABC):
    """Periodic reconcile-snapshot diff: gap-fills events the PUSH path missed."""

    async def _reconcile_snapshot(self) -> AsyncIterator[OrderEvent]:
        """Diff the live reconcile snapshot against the BrokerStore live rows.

        Yields one :class:`OrderEvent` per fill the PUSH stream missed
        (working-order partial progress, or a working → position promotion
        recovered through the deal-history bridge), and stamps
        ``missing_pending_since`` on rows whose broker counterpart has
        deterministically vanished. Emits nothing — and stamps nothing — for an
        ambiguous observation; the next pass or the live PUSH event resolves it.
        Each working entry order's terms are judged against the terms this run
        sent (:meth:`_observe_working_order`), and finally the guarded
        positions' protection levels against the levels this run set
        (:meth:`_observe_protection_snapshot`).

        A no-op when persistence is off (the test paths) or the live connection
        is not established.
        """
        if self.store_ctx is None:
            return
        if self._wire is None or self._live_account_id is None:
            # Live connection not established — a teardown (``disconnect`` set
            # ``_wire = None``) or a reconnect in flight. Honour the documented
            # no-op contract instead of letting ``_reconcile`` raise
            # ``CTraderConnectionError("live connection not established")``:
            # ``_run_reconcile_pass`` would book that as a transient failure and
            # spam the fail-streak warning every ~5 s for the whole outage
            # (e.g. a connection lost while the market-closed reconnect gate is
            # deferring recovery), which reads as "never recovers". The reconnect
            # path — or the terminal shutdown — owns recovery; the next pass runs
            # normally once the wire is back.
            return
        # The snapshot read — rate-limit backoff and account re-auth included —
        # spends the same pass budget as the deal-history reads below; an
        # overrun fails the pass as transient and the next pass reads afresh.
        read_started_at = epoch_time()
        queue_idle = self._exec_events is None or self._exec_events.empty()
        try:
            async with asyncio.timeout_at(self._reconcile_pass_deadline):
                res = await self._reconcile()
        except TimeoutError as exc:
            raise CTraderTimeoutError(
                "reconcile snapshot read overran the reconcile pass budget"
            ) from exc
        orders_by_id = {o.orderId: o for o in res.order}
        open_positions = {
            p.positionId: p for p in res.position
            if p.positionStatus == OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN
        }
        now_ts = epoch_time()
        rows: list[tuple['OrderRow', int | None]] = []
        for row in self.store_ctx.iter_live_orders():
            order_id_raw = (row.extras or {}).get('order_id')
            rows.append((row, parse_protocol_id(order_id_raw, field='order_id')
                         if order_id_raw is not None else None))
        live_order_ids = {order_id for _row, order_id in rows}
        # Position-history walks are keyed by the negated position id
        # (:meth:`_position_history`); they live while their position is open
        # or a shrink a restart found on it waits for its judgment.
        for stale in [oid for oid in self._deal_walks
                      if (oid not in live_order_ids if oid >= 0
                          else -oid not in open_positions
                          and -oid not in self._startup_shrinks)]:
            del self._deal_walks[stale]
        # Rows whose deal-history walk an earlier pass left unfinished go first,
        # so a lookup that keeps spending most of the budget cannot starve them;
        # among those, the walk that waited longest for a request goes first,
        # so one stalled lookup cannot starve the other unfinished walks.
        def walk_turn(item: tuple['OrderRow', int | None]) -> tuple[bool, float]:
            walk: _DealWalk | None = (
                self._deal_walks.get(item[1]) if item[1] is not None else None)
            if walk is None or not walk.segments:
                return True, 0.0
            return False, walk.last_request

        rows.sort(key=walk_turn)
        filled_coids: list[str] = []
        for row, order_id in rows:
            extras = row.extras or {}
            order = orders_by_id.get(order_id) if order_id is not None else None
            if order is not None:
                # The order is still in ``order[]`` — reconcile partial-fill
                # progress from ``executedVolume``. A partial that already linked
                # ``position_id`` does NOT divert the row to the position branch:
                # cTrader keeps the original order in ``order[]`` while assigning a
                # ``positionId`` after the first partial, so later partials must
                # still be diffed here.
                event = await self._reconcile_working_row(
                    row, order, open_positions, now_ts)
                if event is not None:
                    filled_coids.append(row.client_order_id)
                    yield event
                # An edit of the working order made while the stream was
                # down reaches the bot only through this view.
                self._observe_working_order(order, read_started_at=read_started_at)
                continue
            position_id = extras.get('position_id')
            if position_id and row.filled_qty >= row.qty - 1e-9:
                # A fully-filled row whose order has left ``order[]`` is a settled
                # position: only its disappearance matters, and that is the core
                # tracker's concern — ``tracked_refs`` maps it to the
                # ``positions`` namespace for the presence feed below.
                continue
            if order_id is None:
                continue
            # The working order is gone from ``order[]`` — it either fully filled
            # (and left for ``position[]``) or was cancelled / expired. The
            # snapshot cannot tell which (positions carry no order/COID link), so
            # bridge through the deal history. A row that earlier partial-filled
            # (``position_id`` linked, residual still unfilled) lands here too: its
            # remaining quantity may have filled while the stream was down, so it
            # must be recovered through the same bridge — never treated as settled.
            async for event in self._recover_vanished_working_row(
                    row, order_id, open_positions, now_ts):
                filled_coids.append(row.client_order_id)
                yield event
        # Presence diff for the settled-position rows: stamp when the position
        # vanished from ``position[]``, clear the moment it is back. Runs AFTER
        # the row walk so a filled-then-closed retirement in this same pass
        # lands before its row could be stamped. Working rows are deliberately
        # NOT presence-tracked here — their disappearance from ``order[]`` is
        # ambiguous (fill vs cancel) and only the deal-history bridge above may
        # stamp them, on positive no-fill evidence.
        self._disappearance_tracker().observe_presence(
            {'positions': {str(pid) for pid in open_positions}}, now_ts,
        )
        self._feed_native_failsafe_observations(open_positions)
        await self._observe_protection_snapshot(
            open_positions, read_started_at=read_started_at,
            filled_positions=self._row_position_ids(filled_coids),
        )
        queue_idle = queue_idle and not filled_coids and (
                self._exec_events is None or self._exec_events.empty())
        await self._judge_position_shrinks(
            open_positions, read_started_at=read_started_at, queue_idle=queue_idle)
        await self._judge_ambiguous_close_claims()

    def _row_position_ids(self, coids: list[str]) -> set[int]:
        """Positions the given journal rows are linked to now."""
        if self.store_ctx is None:
            return set()
        position_ids: set[int] = set()
        for coid in coids:
            row = self.store_ctx.get_order(coid)
            raw = (row.extras or {}).get('position_id') if row is not None else None
            if raw is not None:
                position_ids.add(parse_protocol_id(raw, field='position_id'))
        return position_ids

    def _record_reconciled_fill(
            self, order_id: int, position_id: int | None, side: str, units: float,
            fills: tuple[tuple[int, int, int, float, float], ...], deal_ids: set[int],
    ) -> None:
        """The journal now counts own fills whose events the stream may still deliver.

        One share effect per execution the history names, the rest as one
        effect of unknown time (see :meth:`_ProtectionMixin._share_bounds`).

        :param side: The side of the row the fills belong to.
        """
        if not position_id or units <= 0.0:
            return
        sign = 1.0 if side == 'buy' else -1.0
        remaining = units
        for executed_ms, deal_id, cents, _price, _fee in fills:
            if deal_id not in deal_ids or remaining <= _VOLUME_TOL:
                continue
            part = min(remaining, volume_to_units(cents))
            self._record_share_effect(position_id, sign * part, counted=True,
                                      executed_ms=executed_ms, deal_id=deal_id,
                                      order_id=order_id)
            remaining -= part
        if remaining > _VOLUME_TOL:
            self._record_share_effect(position_id, sign * remaining, counted=True,
                                      executed_ms=None, deal_id=None, order_id=order_id)

    def _share_with_recovered_fill(
            self, row: 'OrderRow', position_id: int, recovered: float,
    ) -> float:
        """The bot's share of a position counting a row's fill recovered from history."""
        rows = self._position_entry_rows(position_id)
        net = sum(self._signed_exposure(r) for r in rows)
        counted = (row.filled_qty
                   if any(r.client_order_id == row.client_order_id for r in rows) else 0.0)
        added = max(0.0, min(row.qty, recovered) - counted)
        return abs(net + (added if row.side == 'buy' else -added))

    async def _position_history(self, position_id: int, from_ms: int) -> '_DealBridgeResult':
        """Every deal on a position since ``from_ms``, within the pass budget.

        Walked like an order's history, keyed by the negated position id, so
        the walk resumes across passes; no order's fills are collected.
        """
        return await self._find_fill_deal(
            -position_id, from_ms, close_position_id=position_id,
            deadline=self._reconcile_pass_deadline)

    def _position_window_start(
            self, position_id: int, position: 'OpenApiModelMessages.ProtoOAPosition',
    ) -> int:
        """The earliest time (ms) a deal of the bot's on a position can carry."""
        anchors = [self._deal_since_ms(row.extras or {})
                   for row in self._position_entry_rows(position_id)]
        if position.tradeData.openTimestamp:
            anchors.append(position.tradeData.openTimestamp - _CLOCK_SKEW_MS)
        return min(anchors) if anchors else int(epoch_time() * 1000) - int(
            _DEAL_LOOKBACK_S * 1000)

    def _side_volume(
            self, position_id: int, position: 'OpenApiModelMessages.ProtoOAPosition',
    ) -> float:
        """The volume (units) an open position holds on the bot's side."""
        side = self._share_side(position_id)
        venue_side = ('buy' if position.tradeData.tradeSide
                      == OpenApiModelMessages.ProtoOATradeSide.BUY else 'sell')
        if side is not None and side != venue_side:
            return 0.0
        return volume_to_units(position.tradeData.volume)

    async def _judge_position_shrinks(
            self, open_positions: 'dict[int, OpenApiModelMessages.ProtoOAPosition]', *,
            read_started_at: float, queue_idle: bool,
    ) -> None:
        """Judge open positions holding less than the bot's share.

        A partial close the PUSH stream missed (the stream was down) leaves a
        run-owned position open below the bot's journal share. Once the same
        shortfall shows on :data:`_SHRINK_CONFIRMATIONS` consecutive passes —
        with nothing of the bot's in flight and no event waiting in the queue,
        so the journal share is final — the position's deal history is judged
        like a recovered close (:meth:`_judge_recovered_closes`) and the share
        those deals took retired from the journal: the venue is the truth.
        That can be more than the shortfall when the venue counts another
        party's exposure too (a NETTING position); never less. A shrink a
        restart found and the startup baseline already retired
        (:attr:`_startup_shrinks`) is judged once, against the share the
        journal held before, and whatever the deals took beyond the baseline
        is retired then; on a position that closed, or that the bot holds no
        journal share of any more, before such a pass, from the position's
        deal history alone (:meth:`_judge_unshared_startup_shrink`). A
        shortfall the first settled pass after an operator resume finds on a
        position the resume marked (:attr:`_resume_accepted_positions`) is the
        venue the operator accepted: retired with a WARNING and an audit
        event, never judged.

        :param open_positions: The snapshot's open positions by id.
        :param read_started_at: Wall-clock time the snapshot read started.
        :param queue_idle: Whether no execution event waited in the queue
            around the snapshot read and the pass booked no fill.
        """
        if self.store_ctx is None:
            return
        for position_id in [pid for pid in self._shrink_watch if pid not in open_positions]:
            del self._shrink_watch[position_id]
        self._resume_accepted_positions.intersection_update(open_positions)
        for position_id in list(self._startup_shrinks):
            position = open_positions.get(position_id)
            if (position is None or self._own_share(position_id) is None
                    or position_id in self._external_close_reported):
                await self._judge_unshared_startup_shrink(
                    position_id, position, read_started_at=read_started_at,
                    queue_idle=queue_idle)
        for position_id, position in open_positions.items():
            share = self._own_share(position_id)
            if share is None or position_id in self._external_close_reported:
                self._shrink_watch.pop(position_id, None)
                self._resume_accepted_positions.discard(position_id)
                continue
            venue = self._side_volume(position_id, position)
            startup = self._startup_shrinks.get(position_id)
            if startup is None and venue >= share - _VOLUME_TOL:
                self._shrink_watch.pop(position_id, None)
                self._resume_accepted_positions.discard(position_id)
                continue
            if not queue_idle or not self._share_settled(position_id):
                continue
            if position_id in self._resume_accepted_positions:
                self._resume_accepted_positions.discard(position_id)
                rows = self._position_entry_rows(position_id)
                self._note_accepted_by_resume(
                    position_id, client_order_id=rows[0].client_order_id if rows else None,
                    volume=share - venue)
                self._retire_partial_close_exposure(
                    position_id, share - venue, source='resume_accepted')
                self._cover_closes(position_id, read_started_at)
                continue
            if startup is not None:
                taken = await self._judge_shrink(
                    position_id, position, venue=venue, share_now=share + startup.volume,
                    read_started_at=read_started_at)
                if taken is not None:
                    self._end_startup_shrink(position_id)
                    self._cover_closes(position_id, read_started_at)
                    if taken - startup.volume > _VOLUME_TOL:
                        self._retire_partial_close_exposure(
                            position_id, taken - startup.volume, source='position_shrink')
                continue
            key = (raw_volume(venue), raw_volume(share))
            if (position_id, key) in self._shrink_judged:
                continue
            seen_key, count = self._shrink_watch.get(position_id, (None, 0))
            count = count + 1 if seen_key == key else 1
            self._shrink_watch[position_id] = (key, count)
            if count < _SHRINK_CONFIRMATIONS:
                continue
            taken = await self._judge_shrink(
                position_id, position, venue=venue, share_now=share,
                read_started_at=read_started_at)
            if taken is not None:
                self._shrink_judged.add((position_id, key))
                self._shrink_watch.pop(position_id, None)
                self._cover_closes(position_id, read_started_at)
                retired = max(share - venue, taken)
                self._retire_partial_close_exposure(
                    position_id, retired, source='position_shrink')

    def _cover_closes(self, position_id: int, read_started_at: float) -> None:
        """A pass retired a position's closes from the journal without their events.

        Every closing deal it covered executed before the snapshot read
        (margined by the clock skew): a push of one of them that arrives later
        must not be booked again (:attr:`_closes_covered_until`).
        """
        until = int(read_started_at * 1000) - _CLOCK_SKEW_MS
        if until > self._closes_covered_until.get(position_id, 0):
            self._closes_covered_until[position_id] = until

    async def _judge_shrink(
            self, position_id: int, position: 'OpenApiModelMessages.ProtoOAPosition', *,
            venue: float, share_now: float, read_started_at: float,
    ) -> float | None:
        """Judge the deal history of one position open below the bot's share.

        :return: The volume (units) of the bot's share the judged deals took
            (:meth:`_judge_recovered_closes`); ``None`` when the history could
            not be read or judged yet, or a deal in it may postdate the
            snapshot: the caller retries on a later pass.
        """
        history = await self._position_history(
            position_id, self._position_window_start(position_id, position))
        if not history.conclusive:
            return None
        cutoff = int(read_started_at * 1000) - _CLOCK_SKEW_MS
        if any(deal.executed_ms >= cutoff for deal in history.position_deals):
            return None
        rows = self._position_entry_rows(position_id)
        return await self._judge_recovered_closes(
            position_id, history.position_deals,
            from_entry=rows[0].pine_entry_id if rows else None,
            venue_now=venue, share_now=share_now)

    async def _judge_unshared_startup_shrink(
            self, position_id: int,
            position: 'OpenApiModelMessages.ProtoOAPosition | None', *,
            read_started_at: float, queue_idle: bool,
    ) -> None:
        """Judge a shrink a restart found once nothing ties the position to the journal share.

        The position closed, or the bot's rows of it were retired, before a
        settled pass judged the shrink against the share. Its deals are judged
        from the position's history alone: against the share the startup
        baseline retired, on top of whatever share the journal still counts,
        with the closes that came later judged when they came. A position
        whose close was reported already needs no other verdict.

        :param position: The snapshot's view of the position; ``None`` when
            it is gone.
        :param read_started_at: Wall-clock time the snapshot read started.
        :param queue_idle: Whether no execution event waited in the queue
            around the snapshot read and the pass booked no fill.
        """
        if position_id in self._external_close_reported:
            self._end_startup_shrink(position_id)
            return
        if not queue_idle or not self._share_settled(position_id):
            return
        pending = self._startup_shrinks[position_id]
        history = await self._position_history(position_id, pending.since_ms)
        if not history.conclusive:
            return
        cutoff = int(read_started_at * 1000) - _CLOCK_SKEW_MS
        if any(deal.executed_ms >= cutoff for deal in history.position_deals):
            return
        venue = self._side_volume(position_id, position) if position is not None else 0.0
        taken = await self._judge_recovered_closes(
            position_id, history.position_deals, from_entry=pending.from_entry,
            venue_now=venue, share_now=(self._own_share(position_id) or 0.0) + pending.volume,
            own_orders=pending.order_ids)
        if taken is not None:
            self._end_startup_shrink(position_id)
            self._cover_closes(position_id, read_started_at)

    def _record_startup_shrink(
            self, position: 'OpenApiModelMessages.ProtoOAPosition', row: 'OrderRow',
            volume: float,
    ) -> None:
        """Keep a shrink the startup baseline retired pending until its deals are judged.

        Journalled: a process that stops before a reconcile pass judges it
        leaves it to the next process of the run, whose journal share already
        matches the venue.

        :param position: The startup snapshot's view of the shrunk position.
        :param row: The one live entry row holding the position.
        :param volume: The share (units) the baseline retired.
        """
        if self.store_ctx is None:
            return
        position_id = position.positionId
        key = uuid.uuid4().hex
        since_ms = self._position_window_start(position_id, position)
        order_ids = sorted({
            parse_protocol_id(raw, field='order_id')
            for raw in ((r.extras or {}).get('order_id')
                        for r in self._position_entry_rows(position_id))
            if raw is not None})
        self.store_ctx.log_event(
            _STARTUP_SHRINK_PENDING_EVENT, client_order_id=row.client_order_id,
            exchange_order_id=str(position_id),
            payload={'key': key, 'position_id': position_id, 'volume': volume,
                     'since_ms': since_ms, 'from_entry': row.pine_entry_id,
                     'order_ids': order_ids, 'found_ms': int(epoch_time() * 1000)})
        self._merge_startup_shrink(position_id, _StartupShrink(
            volume=volume, since_ms=since_ms, from_entry=row.pine_entry_id,
            order_ids=order_ids, keys=[key]))

    def _restore_startup_shrinks(self) -> None:
        """Take over the startup shrinks earlier processes of the run left unjudged.

        Runs once, from the startup adoption baseline, after this process's
        own findings. An operator resume accepted the venue as it stood: a
        pending shrink found before the resume, or on a position whose rows
        the resume marked, is accepted with a WARNING and an audit event.
        """
        store = self.store_ctx
        if store is None:
            return
        done: set[str] = set()
        for _intent, _coid, _ref, payload in store.iter_events_by_kind_for_run_id(
                _STARTUP_SHRINK_DONE_EVENT):
            keys = payload.get('keys')
            if isinstance(keys, list):
                done.update(key for key in keys if isinstance(key, str))
        done.update(key for pending in self._startup_shrinks.values() for key in pending.keys)
        resume_ts_ms = self.operator_resume_ts_ms
        for _intent, coid, _ref, payload in store.iter_events_by_kind_for_run_id(
                _STARTUP_SHRINK_PENDING_EVENT):
            key = payload.get('key')
            position_id = payload.get('position_id')
            volume = payload.get('volume')
            since_ms = payload.get('since_ms')
            found_ms = payload.get('found_ms')
            from_entry = payload.get('from_entry')
            order_ids = payload.get('order_ids')
            if (not isinstance(key, str) or key in done or not isinstance(position_id, int)
                    or not isinstance(volume, (int, float)) or not isinstance(since_ms, int)
                    or not isinstance(found_ms, int) or not isinstance(order_ids, list)):
                continue
            done.add(key)
            if position_id in self._resume_accepted_positions or (
                    resume_ts_ms is not None and found_ms <= resume_ts_ms):
                self._note_accepted_by_resume(
                    position_id, client_order_id=coid, volume=float(volume))
                store.log_event(
                    _STARTUP_SHRINK_DONE_EVENT, exchange_order_id=str(position_id),
                    payload={'keys': [key], 'position_id': position_id})
                continue
            self._merge_startup_shrink(position_id, _StartupShrink(
                volume=float(volume), since_ms=since_ms,
                from_entry=from_entry if isinstance(from_entry, str) else None,
                order_ids=[oid for oid in order_ids if isinstance(oid, int)], keys=[key]))

    def _merge_startup_shrink(self, position_id: int, shrink: _StartupShrink) -> None:
        """Add a pending startup shrink to the one already pending on its position."""
        pending = self._startup_shrinks.get(position_id)
        if pending is None:
            self._startup_shrinks[position_id] = shrink
            return
        pending.volume += shrink.volume
        pending.since_ms = min(pending.since_ms, shrink.since_ms)
        pending.from_entry = pending.from_entry or shrink.from_entry
        pending.order_ids.extend(oid for oid in shrink.order_ids if oid not in pending.order_ids)
        pending.keys.extend(shrink.keys)

    def _end_startup_shrink(self, position_id: int) -> None:
        """A pending startup shrink got its verdict: never judged again by any process."""
        pending = self._startup_shrinks.pop(position_id, None)
        if pending is not None and self.store_ctx is not None:
            self.store_ctx.log_event(
                _STARTUP_SHRINK_DONE_EVENT, exchange_order_id=str(position_id),
                payload={'keys': pending.keys, 'position_id': position_id})

    async def _judge_ambiguous_close_claims(self) -> None:
        """Settle closing fills taken for an own close whose outcome is unknown.

        On a HEDGED account the closing orders of those fills can settle it:
        the bot's close is a client order without a client-order-id, so a
        fill of an order carrying another client's client-order-id is never
        its own, and one without may be. Every way of taking up to as many of
        the latter as the run has closes of unknown outcome on the position
        as the bot's own is weighed: the other orders' fills are judged
        against the bot's share as an answer naming the close would judge them
        (:func:`_foreign_claims_taken`) — a close of only another party's
        exposure merged onto the position takes nothing of it. Reported when
        another party's fill took the bot's share whichever orders were the
        bot's; otherwise the ERROR and audit written when the fill was taken
        stand. On a NETTING account the orders cannot be told apart this way;
        the ERROR and audit stand.
        """
        if not self._ambiguous_closes:
            return
        if not self._hedging_enabled:
            self._ambiguous_closes.clear()
            return
        for position_id in list(self._ambiguous_closes):
            if position_id in self._external_close_reported:
                del self._ambiguous_closes[position_id]
                continue
            records = [own for own in self._own_close_history
                       if own.position_id == position_id and own.order_id is None]
            claimed = sorted({claim.order_id for own in records for claim in own.claims
                              if not claim.venue})
            kinds: dict[int, CloseOrigin] = {}
            for order_id in claimed:
                kind = await self._closing_order_kind(position_id, order_id)
                if kind is None:
                    break
                kinds[order_id] = kind
            else:
                del self._ambiguous_closes[position_id]
                own = {order_id for order_id, kind in kinds.items() if kind is CloseOrigin.OWN}
                maybe_own = [order_id for order_id, kind in kinds.items()
                             if kind in (CloseOrigin.UNATTRIBUTED, CloseOrigin.UNKNOWN)]
                takes: list[float] = []
                for count in range(min(len(records), len(maybe_own)) + 1):
                    for chosen in combinations(maybe_own, count):
                        own_orders = own | set(chosen)
                        takes.append(sum(
                            _foreign_claims_taken(record.claims, own_orders)[0]
                            for record in records))
                taken = min(takes)
                if taken <= _VOLUME_TOL:
                    logger.info(
                        "cTrader: the closes taken for bot position %d's close of unknown "
                        "outcome may all be the bot's or another party's own exposure; "
                        "the audit written when they were taken stands", position_id)
                    continue
                candidates = [order_id for order_id in claimed if order_id not in own]
                self._queue_external_close(position_id, {
                    'position_id': position_id,
                    'order_id': candidates[0],
                    'candidate_order_ids': candidates,
                    'closed_qty': sum(claim.fill_qty for record in records
                                      for claim in record.claims
                                      if not claim.venue and claim.order_id in candidates),
                    'booked_qty': taken,
                    'own_closes_unknown': len(records),
                    'recovered': True,
                }, reason=(
                    f"bot position {position_id} was closed by client orders "
                    f"{candidates} while {len(records)} close(s) of the bot had an unknown "
                    f"outcome: whichever of them was the bot's, someone else took "
                    f"{taken} units of the bot's share"))

    def _feed_native_failsafe_observations(
            self, open_positions: 'dict[int, OpenApiModelMessages.ProtoOAPosition]',
    ) -> None:
        """Feed the fail-safe observe sink for every live entry whose position
        is open — the M3 reconcile-based ``DEGRADING -> HEALTHY`` recovery.

        Independent of the fill diff: it confirms the broker is carrying the
        engine's desired native bracket (or surfaces an external edit), keyed
        by each parent entry's dispatch ref (the row's ``client_order_id``,
        the same ref :meth:`publish_native_failsafe_sl` amends under). Replaces
        the M2 actuator self-confirm so the reconcile feed and the actuator no
        longer race to confirm the same PUT. Re-reads the live rows so a
        position promoted earlier in this very pass is observed too. A no-op
        without persistence or a wired sink.
        """
        if self.store_ctx is None or self.native_failsafe_observed_sink is None:
            return
        for row in list(self.store_ctx.iter_live_orders()):
            position_id_raw = (row.extras or {}).get('position_id')
            if position_id_raw is None:
                continue
            pid = parse_protocol_id(position_id_raw, field='position_id')
            if pid not in open_positions:
                continue
            self._feed_native_failsafe_observed(
                row.client_order_id, open_positions[pid])

    def _feed_native_failsafe_observed(
            self, parent_ref: str,
            position: 'OpenApiModelMessages.ProtoOAPosition',
    ) -> None:
        """Forward one live position's broker-observed bracket to the engine's
        §2.6.7 fail-safe recovery feed via the thread-safe observed sink.

        The levels are read off the ``ProtoOAPosition`` attributes themselves
        (``stopLoss`` / ``takeProfit`` / ``trailingStopLoss``), which the
        reconcile snapshot always carries. The separate
        ``STOP_LOSS_TAKE_PROFIT`` protection order the
        ``returnProtectionOrders=True`` mode adds to ``order[]`` is NOT a
        usable source: live-measured on Pepperstone demo (probe114,
        2026-08-17), that order reports the SL in ``stopPrice`` and the TP in
        ``limitPrice`` while its ``stopLoss`` / ``takeProfit`` fields stay
        unset. Reading those unset fields fed ``None`` levels for a position
        whose stop was verifiably present, which the manager booked as an
        external edit — flipping ownership to UNKNOWN and freezing the
        symbol's entry gate in DEGRADED for the parent's whole lifetime.

        ``trailingStopLoss`` is a bool flag (the live trailing price rides in
        ``stopLoss``) — there is NO relative trailing-distance field. So
        ``trailing_stop`` is always ``None``, and the absolute ``stop_level``
        is surfaced only for a STATIC stop: while trailing is active the stop
        is moving and cannot be matched against the engine's relative
        desired-trailing, so it is reported as ``None`` rather than fighting
        the desired snapshot into a false edit. A position with no protective
        levels (the bracket was cleared / never landed) reports both levels as
        ``None`` so the manager degrades.

        Routed through ``native_failsafe_observed_sink`` — the runner wires it
        to the engine's thread-safe ``enqueue_native_bracket_observed``, NOT the
        direct ``record_native_bracket_observed``, because the reconcile pass
        runs on the broker event-loop thread and the manager's per-parent state
        must only be mutated on the main thread. Snapshots for refs the manager
        does not track are dropped at drain time, so feeding every live entry
        is safe.
        """
        sink = self.native_failsafe_observed_sink
        if sink is None:
            return
        trailing_active = (position.HasField('trailingStopLoss')
                           and position.trailingStopLoss)
        sink(
            parent_ref,
            stop_level=(position.stopLoss
                        if position.HasField('stopLoss') and not trailing_active
                        else None),
            profit_level=(position.takeProfit
                          if position.HasField('takeProfit') else None),
            trailing_stop=None,
        )

    def _disappearance_tracker(self) -> DisappearanceTracker:
        """The lazily-built core disappearance tracker for this instance.

        Built on first use because its inputs — ``store_ctx``,
        ``on_unexpected_cancel``, ``quarantine_sink`` — are injected after
        ``__init__``. Venue wiring:

        * Presence tracking covers ONLY settled position rows (fully
          filled, ``position_id`` linked): a position gone from
          ``position[]`` is stamped unconditionally, the close-versus-
          cancel decision deferred to the grace-expiry re-verification.
          Working rows return an empty ref set — their disappearance from
          ``order[]`` is ambiguous (fill vs cancel) and only the snapshot
          bridge (:meth:`_recover_vanished_working_row`) may stamp them,
          on positive no-fill evidence; the shared
          ``missing_pending_since`` key feeds both into the same grace
          protocol.
        * ``confirm_missing`` wraps the deal-history bridge — see
          :meth:`_confirm_missing_via_deal_history`.
        * A :data:`MissingResolution.CLOSED` verdict retires every live
          sibling sharing the position id (a NETTING / pyramiding account
          merges pyramid entries onto one ``positionId``), and the
          backing deal ids are seeded into :attr:`_seen_deal_ids` so a
          replayed PUSH copy of a recovered execution can never re-book.
        """
        tracker = self._disappearance
        if tracker is None:
            assert self.store_ctx is not None
            tracker = DisappearanceTracker(
                self.store_ctx,
                grace_s=_MISSING_PENDING_GRACE_S,
                policy=self.on_unexpected_cancel,
                tracked_refs=self._tracked_position_refs,
                confirm_missing=self._confirm_missing_via_deal_history,
                request_quarantine=self.quarantine_sink,
                sibling_coids=self._closed_position_siblings,
                register_executions=self._register_recovered_deals,
                cancelled_event_factory=self._cancelled_event,
            )
            self._disappearance = tracker
        return tracker

    @staticmethod
    def _tracked_position_refs(row: 'OrderRow') -> set[tuple[str, str]]:
        """Presence refs for the tracker: settled position rows only."""
        extras = row.extras or {}
        position_id = extras.get('position_id')
        if position_id and row.filled_qty >= row.qty - 1e-9:
            return {('positions', str(position_id))}
        return set()

    async def _confirm_missing_via_deal_history(
            self, row: 'OrderRow',
    ) -> MissingConfirmation:
        """Grace-expiry re-verification against a fresh deal-history read.

        The ``missing_pending_since`` stamp records that the row's broker
        counterpart had vanished at SOME earlier pass — not that it is
        still conclusively gone now. Classify from fresh evidence:

        * INCONCLUSIVE (deal-history transport down): never conclude a
          cancel from a truncated read — the tracker keeps the stamp and
          waits, throttling the warning to once per row.
        * FILLED-then-CLOSED (``closedVolume > 0``): a native TP / SL or
          external close fired while the stream was down — the tracker
          books the terminal CLOSE, never a synthetic entry-cancel. A
          working row needs the bridge to prove its own fill
          (``filled_cents``); a fully-filled position row already
          establishes its fill, so a close on its KNOWN position id is
          enough even when the entry fill aged out of the bridge window
          (the ``close_position_id`` fallback). The closing deals are first
          judged for an external close the PUSH stream missed
          (:meth:`_judge_recovered_closes`); while a closing order cannot be
          looked up the verdict stays INCONCLUSIVE. A row whose disappearance
          an operator resume accepted has its close accepted instead.
        * A zero-fill working order that FILLED during the gap: it
          demonstrably opened a position, so it is NOT a cancel — the
          tracker clears the now-false premise and the next
          :meth:`_reconcile_snapshot` pass promotes it against the full
          snapshot. No fill data is returned here: the snapshot bridge
          owns the promotion (and its dedup seeding), keeping this path
          book-free.
        * CONCLUSIVE no-fill / no-close: the order's own status decides
          (:meth:`_vanished_order_verdict`). An order the venue ended
          (expired, rejected) is retired as a synthetic cancel carrying the
          venue's reason, no policy applies; one that expired at an expiry
          the bot never set was edited by someone else, and the tracker
          applies the ``on_unexpected_cancel`` policy. A cancel the status
          cannot attribute is retired with an ERROR and an audit event and
          no policy; an order that executed or still works is no cancel.
        """
        extras = row.extras or {}
        order_id_raw = extras.get('order_id')
        position_id_raw = extras.get('position_id')
        position_id = (
            parse_protocol_id(position_id_raw, field='position_id')
            if position_id_raw is not None else None
        )
        if order_id_raw is not None:
            order_id = parse_protocol_id(order_id_raw, field='order_id')
            bridge = await self._find_fill_deal(
                order_id, self._deal_since_ms(extras), close_position_id=position_id,
                deadline=self._reconcile_pass_deadline,
            )
            if not bridge.conclusive:
                return MissingConfirmation(MissingResolution.INCONCLUSIVE)
            if bridge.closed_cents > 0 and (bridge.filled_cents > 0
                                            or position_id is not None):
                close_pid = (
                    bridge.deal.positionId if bridge.deal is not None else position_id
                )
                assert close_pid is not None
                if _accepted_by_resume(row):
                    self._note_accepted_by_resume(
                        close_pid, client_order_id=row.client_order_id, volume=None,
                        deal_ids=bridge.closing_deal_ids)
                    self._mark_close_judged(list(bridge.closing_deal_ids))
                elif await self._judge_recovered_closes(
                        close_pid, bridge.position_deals, from_entry=row.pine_entry_id,
                        counted=bridge.deal_ids) is None:
                    return MissingConfirmation(MissingResolution.INCONCLUSIVE)
                # Register the entry fills AND the closing deals: the
                # closure is concluded from the closing evidence, whose
                # deals usually belong to a DIFFERENT order (native TP/SL,
                # external close) — a late PUSH replay of either must be
                # suppressed after the retire.
                evidence = dict.fromkeys(
                    (*bridge.deal_ids, *bridge.closing_deal_ids))
                return MissingConfirmation(
                    MissingResolution.CLOSED,
                    position_ref=str(close_pid),
                    execution_ids=tuple(str(d) for d in evidence),
                )
            if position_id is None:
                if bridge.filled_cents > 0:
                    return MissingConfirmation(MissingResolution.FILLED)
                return await self._vanished_order_verdict(row, order_id)
        return MissingConfirmation(MissingResolution.CANCELLED)

    async def _vanished_order_verdict(
            self, row: 'OrderRow', order_id: int,
    ) -> MissingConfirmation:
        """The verdict on a working order that left the venue without a fill.

        The order's own status (``ProtoOAOrderDetailsReq``) tells what ended
        it. An order that executed (filled, or executed part of its volume)
        was not cancelled: the deal history does not list its fill yet, and
        the snapshot bridge promotes it once it does. An order the venue still
        reports as accepted is still working. An expired or rejected order
        was ended by the venue, so the cancel carries the venue's reason
        (:meth:`_venue_cancel_reason`) and the tracker applies no policy,
        unless it expired at an expiry the bot never set: someone else's edit,
        the unexpected cancel the policy handles. A cancelled order, or one
        the venue reports as not found, was ended by someone the evidence
        cannot name (:meth:`_unattributed_cancel_reason`). A lookup that
        failed for now leaves the verdict to a later pass.

        :param row: The vanished order's journal row.
        :param order_id: The order's id.
        """
        if self._live_account_id is None:
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        try:
            async with asyncio.timeout_at(self._reconcile_pass_deadline):
                res = cast(OpenApiMessages.ProtoOAOrderDetailsRes, await self._account_request(
                    OpenApiMessages.ProtoOAOrderDetailsReq(
                        ctidTraderAccountId=self._live_account_id, orderId=order_id)))
        except CTraderProtocolError as exc:
            if is_not_found(exc.error_code):
                return MissingConfirmation(
                    MissingResolution.CANCELLED,
                    cancel_reason=self._unattributed_cancel_reason(
                        row, order_id, status=exc.error_code))
            logger.debug("cTrader vanished-order lookup for %d deferred: %s", order_id, exc)
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        except (BrokerError, CTraderWireError, TimeoutError) as exc:
            logger.debug("cTrader vanished-order lookup for %d deferred: %s", order_id, exc)
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        order = res.order
        statuses = OpenApiModelMessages.ProtoOAOrderStatus
        status = order.orderStatus
        if status == statuses.ORDER_STATUS_FILLED or order.executedVolume > 0:
            return MissingConfirmation(MissingResolution.FILLED)
        if status == statuses.ORDER_STATUS_ACCEPTED:
            return MissingConfirmation(MissingResolution.STILL_PRESENT)
        if status in (statuses.ORDER_STATUS_EXPIRED, statuses.ORDER_STATUS_REJECTED):
            return MissingConfirmation(
                MissingResolution.CANCELLED,
                cancel_reason=self._venue_cancel_reason(
                    order, expired=status == statuses.ORDER_STATUS_EXPIRED,
                    rejected=status == statuses.ORDER_STATUS_REJECTED))
        return MissingConfirmation(
            MissingResolution.CANCELLED,
            cancel_reason=self._unattributed_cancel_reason(
                row, order_id,
                status=statuses.Name(status) if status in statuses.values() else str(status)))

    def _closed_position_siblings(
            self, row: 'OrderRow', confirmation: MissingConfirmation,
    ) -> list[str]:
        """Live rows sharing the CLOSED verdict's position id.

        A NETTING / pyramiding account merges pyramid entries onto one
        ``positionId``, so a proven close retires all of them (mirroring
        the PUSH path's ``_mark_position_closed``) — closing only the
        observed row would leave sibling rows live or later mis-stamped
        against a flat broker.
        """
        pid_ref = confirmation.position_ref
        if not pid_ref or self.store_ctx is None:
            return []
        return [
            r.client_order_id
            for r in self.store_ctx.iter_live_orders()
            if r.client_order_id != row.client_order_id
               and (str((r.extras or {}).get('position_id') or '') == pid_ref
                    or r.exchange_order_id == pid_ref)
        ]

    def _register_recovered_deals(
            self, _row: 'OrderRow', deal_ids: tuple[str, ...],
    ) -> None:
        """Seed recovered deal ids into the shared PUSH-path dedup set.

        cTrader can replay (or push an uncorrelated copy of) an execution
        after reconnect; a deal the grace-expiry verdict was concluded
        from must never re-book as a fresh fill.
        """
        self._seen_deal_ids.update(int(d) for d in deal_ids)

    async def _reconcile_working_row(
            self, row, order, open_positions: dict, now_ts: float,
    ) -> OrderEvent | None:
        """Detect partial-fill progress on a still-pending working order.

        ``ProtoOAOrder`` carries ``positionId`` once any volume has filled, so a
        partial fill is deterministic from ``order[]`` alone: link the
        ``positionId`` (set-once) and, when the cumulative ``executedVolume`` has
        grown past the row's recorded ``filled_qty``, emit a partial-fill event.
        A working order that is back after a stale-missing stamp clears it.

        The snapshot carries no per-execution terms (``executionPrice`` is the
        order's running average) and no commission, so the event takes both
        from the order's deal history: the price of exactly the new executions
        and the commission of every deal not booked yet, which are then marked
        seen. When the history cannot cover the progress (read failed, or the
        deals are not listed yet) the fill is still booked on time from the
        snapshot, without commission, and its deals stay unseen so a later
        recovery of this order books their commission.
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
        fill_qty = filled - row.filled_qty
        fill_price = order.executionPrice or None
        fee = 0.0
        # Read the history before the cursor write, so nothing is persisted for
        # an event this pass could still fail to deliver.
        bridge = await self._find_fill_deal(
            order.orderId, self._deal_since_ms(extras),
            deadline=self._reconcile_pass_deadline)
        delta_price = (self._delta_fill_price(bridge.fills, row.filled_qty, fill_qty)
                       if bridge.conclusive else None)
        covered: set[int] = set()
        if delta_price is not None:
            fill_price = delta_price
            covered = self._deals_covering(bridge.fills, filled)
            fee = sum(fill[4] for fill in bridge.fills
                      if fill[1] in covered and fill[1] not in self._seen_deal_ids)
        position_id: int | None = order.positionId or None
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
        if position_id is not None:
            self._link_position_ref(row.client_order_id, position_id)
        self._record_reconciled_fill(
            order.orderId, position_id, row.side, fill_qty, bridge.fills,
            {deal_id for deal_id in covered if deal_id not in self._seen_deal_ids})
        self._seen_deal_ids.update(covered)
        avg = (open_positions[position_id].price
               if position_id in open_positions else order.executionPrice)
        return self._fill_event(
            row, event_type='partial', status=OrderStatus.PARTIALLY_FILLED,
            exchange_id=str(order.orderId), cumulative=filled,
            fill_qty=fill_qty, avg_price=avg or None,
            fill_price=fill_price, fee=fee, timestamp=now_ts,
        )

    @staticmethod
    def _deals_covering(
            fills: tuple[tuple[int, int, int, float, float], ...], filled: float,
    ) -> set[int]:
        """Deal ids of the oldest executions that make up ``filled`` units.

        :param fills: The bridge's per-deal terms, oldest first
            (:attr:`_DealBridgeResult.fills`).
        :param filled: The order's cumulative filled units.
        :return: The ids of the deals within the first ``filled`` units.
        """
        covered: set[int] = set()
        total = 0.0
        for _ts, deal_id, cents, _price, _fee in fills:
            total += volume_to_units(cents)
            if total > filled + 1e-9:
                break
            covered.add(deal_id)
        return covered

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
        bridge = await self._find_fill_deal(
            order_id, self._deal_since_ms(extras),
            deadline=self._reconcile_pass_deadline)
        if bridge.deal is not None:
            closed_pid = bridge.deal.positionId
            if (closed_pid and closed_pid not in open_positions
                    and bridge.closed_cents > 0 and bridge.filled_cents > 0
                    and await self._judge_recovered_closes(
                        closed_pid, bridge.position_deals, from_entry=row.pine_entry_id,
                        share_now=self._share_with_recovered_fill(
                            row, closed_pid, volume_to_units(bridge.filled_cents)),
                        counted=bridge.deal_ids) is None):
                # Who closed the position is not known yet: retire it on a
                # later pass, once the closing orders could be looked up.
                return
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
        position_id: int | None = deal.positionId or None
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
                # PUSH path would re-emit a recovered deal as a fresh fill. The
                # closing deals usually belong to a DIFFERENT order (native
                # TP/SL, external close), so they must be seeded explicitly too.
                self._seen_deal_ids.update(bridge.deal_ids)
                self._seen_deal_ids.update(bridge.closing_deal_ids)
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
        # The bridge observed FILLED deals for this order (or linked its
        # position), so any ``missing_pending_since`` stamp set on a prior no-fill
        # pass is now stale: the row is a live (possibly partial) position, not a
        # cancelled pending. Clear it whenever a fill is PROVEN — not only when
        # this pass also writes a promotion. A delayed PUSH fill that already
        # advanced ``filled_qty`` AND linked the position before the bridge sees
        # the same deal leaves both ``cumulative > row.filled_qty`` and
        # ``link_position`` false, so a clear gated on the promotion write would
        # be skipped and the stale stamp would survive into
        # ``_emit_unexpected_cancellations`` and falsely retire / raise on the
        # live filled row. Clear BEFORE any promotion write so the journal's
        # extras merge lands on the cleared state.
        if ((recovered > 0 or link_position)
                and 'missing_pending_since' in (row.extras or {})):
            self._clear_missing_pending(row)
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
                assert position_id is not None
                self._link_position_ref(row.client_order_id, position_id)
        if not new_deal_ids:
            # Every recovered deal was already applied by the PUSH path — the
            # cursor / position link are now consistent, so emit nothing.
            return None
        self._record_reconciled_fill(
            deal.orderId, position_id, row.side, cumulative - row.filled_qty, bridge.fills,
            set(new_deal_ids))
        self._seen_deal_ids.update(new_deal_ids)
        if cumulative - row.filled_qty <= 1e-9:
            # The recovered deals are new to ``_seen_deal_ids`` (now seeded) but
            # the cumulative they sum to is already covered by the durable cursor
            # (a prior PUSH / reconcile counted this progress). Emitting here
            # would yield a zero-qty fill that ``record_fill`` ignores while the
            # engine still runs its post-fill side effects — seed the ids and stop.
            # Their commission is unbooked only when the snapshot partial path
            # booked them while their history was unreadable; ``OrderEvent``
            # carries a fee only with a fill, so surface the amount instead.
            new_ids = set(new_deal_ids)
            unbooked = sum(fill[4] for fill in bridge.fills if fill[1] in new_ids)
            if unbooked:
                logger.warning(
                    "cTrader order %d: commission %s of deals %s was not booked "
                    "(their fill was recovered from the snapshot without history)",
                    deal.orderId, unbooked, sorted(new_ids),
                )
            return None
        # ``fill_price`` is what ``BrokerPosition.record_fill`` books the emitted
        # quantity at, so it is the volume-weighted price of exactly the
        # executions that quantity stands for — never the whole order's average,
        # which would re-price the part the cursor already booked.
        # ``avg_price`` (the ExchangeOrder snapshot's display field) still prefers
        # the open position's broker-reported average when present.
        fill_qty = cumulative - row.filled_qty
        bridge_price = bridge.avg_price or deal.executionPrice or None
        fill_price = (self._delta_fill_price(bridge.fills, row.filled_qty, fill_qty)
                      or bridge_price)
        avg = (open_positions[position_id].price
               if position_open else bridge_price)
        status = (OrderStatus.FILLED if cumulative >= row.qty - 1e-9
                  else OrderStatus.PARTIALLY_FILLED)
        # Commission is booked once per execution: the PUSH path booked it for
        # every deal already in ``_seen_deal_ids``, the snapshot partial-fill
        # path books none, so the unseen deals carry every unbooked commission.
        new_ids = set(new_deal_ids)
        fee = sum(fill[4] for fill in bridge.fills if fill[1] in new_ids)
        return self._fill_event(
            row, event_type='filled', status=status,
            exchange_id=str(position_id or deal.orderId), cumulative=cumulative,
            fill_qty=fill_qty, avg_price=avg or None,
            fill_price=fill_price, fee=fee,
            timestamp=(deal.executionTimestamp / 1000.0
                       if deal.executionTimestamp else now_ts),
        )

    @staticmethod
    def _delta_fill_price(
            fills: tuple[tuple[int, int, int, float, float], ...],
            booked: float, qty: float,
    ) -> float | None:
        """Volume-weighted price of the ``qty`` units filled after ``booked``.

        An order fills in time order, so the ``booked`` units the row's cursor
        already counted are the OLDEST executions; the recovered delta is the
        next ``qty`` units, splitting a deal that straddles either boundary.

        :param fills: The bridge's per-deal terms, oldest first
            (:attr:`_DealBridgeResult.fills`).
        :param booked: Units already booked for the order (``filled_qty``).
        :param qty: Units of the recovered delta.
        :return: The delta's price, or ``None`` when the deals do not cover it.
        """
        skip = booked
        taken = 0.0
        notional = 0.0
        for _ts, _deal_id, cents, price, _fee in fills:
            units = volume_to_units(cents)
            if skip >= units - 1e-9:
                skip -= units
                continue
            part = min(units - skip, qty - taken)
            skip = 0.0
            notional += price * part
            taken += part
            if taken >= qty - 1e-9:
                break
        if taken < qty - 1e-9:
            return None
        return notional / taken

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

        ``deal`` may be ``None`` when the closure was proven by a
        ``close_position_id``-keyed ``closedVolume`` lookup whose entry fill had
        aged out of the bridge window (a fully-filled position row already
        establishes its own fill); only the audit ``deal_id`` is then absent.

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
                audit_payload={'deal_id': deal.dealId if deal is not None else None,
                               'position_id': position_id,
                               'closed_cents': closed_cents},
                exchange_order_id=str(position_id),
            ),
        )
        for coid in siblings:
            self.store_ctx.close_order(coid)

    @staticmethod
    def _deal_since_ms(extras: dict) -> int:
        """The deal-history ``fromTimestamp`` (ms) for a row's order.

        The row's ``submitted_at_ms`` anchor minus :data:`_SINCE_ANCHOR_SKEW_MS`,
        so a fill never ages out of a fixed window. Zero-anchor fallback (the
        crash-before-persist row the M3 startup recovery owns): a wide fixed
        window, never the runtime path.

        :param extras: The row's extras.
        :return: The lower bound of the order's deal-history window.
        """
        anchor = extras.get('submitted_at_ms')
        if anchor is not None:
            return parse_protocol_id(anchor, field='submitted_at_ms') - _SINCE_ANCHOR_SKEW_MS
        return int(epoch_time() * 1000) - int(_DEAL_LOOKBACK_S * 1000)

    async def _find_fill_deal(
            self, order_id: int, from_ms: int, *, close_position_id: int | None = None,
            deadline: float | None = None,
    ) -> '_DealBridgeResult':
        """Return the FILLED-deal evidence for ``order_id`` and read completeness.

        Covers the deal history from ``from_ms`` (the order's own since-anchor)
        up to now, paging on ``hasMore``, and aggregates EVERY FILLED deal of
        ``order_id`` in the window — a single working order can fill across
        several partial deals, so the per-order quantity is their summed
        ``filledVolume`` (in cents), never a single deal's slice. The deals can
        fill at DIFFERENT prices, so the order's average is their
        volume-weighted price (``avg_price``), and each deal's own volume, price
        and commission is kept in ``fills`` for pricing a partial delta.
        Alongside, it sums ``closePositionDetail.closedVolume`` per position so
        the caller can tell a filled-then-closed order from a live one
        (closedVolume-primary closure classification). ``conclusive`` is
        ``True`` only when the whole window was consumed without a transport
        error — a failed or truncated read returns an empty, non-conclusive
        result so the caller never concludes a cancel (or a closure) from
        missing evidence.

        The evidence is kept per order in :attr:`_deal_walks` across calls, so
        an unfinished walk resumes from its last page and a finished one reads
        only the deals since it ended (re-reading
        :data:`_SINCE_ANCHOR_SKEW_MS` of overlap, de-duplicated by ``dealId``).
        A walk anchored later than ``from_ms`` starts over.

        :param order_id: The broker order id whose fills to recover.
        :param from_ms: The lower-bound ``fromTimestamp`` (ms) — the order's
            ``submitted_at_ms`` anchor minus a skew margin, so a fill never ages
            out of a fixed window.
        :param close_position_id: When set, the closure-volume lookup falls back
            to this KNOWN position id if no FILLED deal of ``order_id`` is in the
            window to derive the target from. A fully-filled position row already
            knows its position id, so a close can be detected even when the
            original entry fill has aged out of the bridge window.
        :param deadline: Event-loop time the read must finish by (the reconcile
            pass budget), or ``None`` for the plain per-request timeout. Each
            page request waits at most until the deadline; a walk that reaches
            it unfinished is inconclusive and resumes on the next call.
        :return: a :class:`_DealBridgeResult` — see its field docs.
        """
        wire = self._wire
        if wire is None or self._live_account_id is None:
            return _DealBridgeResult(None, 0, None, False)
        now_ms = int(epoch_time() * 1000)
        walk = self._deal_walks.get(order_id)
        if walk is None or walk.from_ms > from_ms:
            walk = _DealWalk(from_ms=from_ms, high_ms=from_ms)
            self._deal_walks[order_id] = walk
        if now_ms > walk.high_ms:
            newest = walk.segments[-1] if walk.segments else None
            if newest is not None and newest[1] == walk.high_ms:
                # The newest owed sub-window has returned no page yet: widen it
                # instead of queueing another one, so a walk whose budget only
                # fits a page or two per call still reaches the present.
                newest[1] = now_ms
            else:
                walk.segments.append(
                    [max(walk.from_ms, walk.high_ms - _SINCE_ANCHOR_SKEW_MS), now_ms])
            walk.high_ms = now_ms
        loop = asyncio.get_running_loop()
        try:
            while walk.segments:
                segment = walk.segments[0]
                timeout = REQUEST_TIMEOUT
                if deadline is not None:
                    timeout = min(timeout, deadline - loop.time())
                    if timeout <= 0.0:
                        logger.debug(
                            "cTrader deal-history bridge for order %d deferred: "
                            "reconcile pass budget spent", order_id,
                        )
                        return _DealBridgeResult(None, 0, None, False)
                walk.last_request = loop.time()
                res = cast(OpenApiMessages.ProtoOADealListRes, await wire.send_request(
                    OpenApiMessages.ProtoOADealListReq(
                        ctidTraderAccountId=self._live_account_id,
                        fromTimestamp=segment[0], toTimestamp=segment[1],
                        maxRows=_DEAL_PAGE_MAX_ROWS,
                    ),
                    timeout=timeout,
                ))
                for deal in res.deal:
                    walk.ingest(deal, order_id)
                if not res.hasMore or not res.deal:
                    walk.segments.pop(0)
                    continue
                oldest = min((d.executionTimestamp or d.createTimestamp)
                             for d in res.deal)
                if oldest <= segment[0]:
                    walk.segments.pop(0)
                    continue
                segment[1] = oldest - 1
        except Exception as exc:  # noqa: BLE001 - read failure is inconclusive evidence
            if deadline is not None and loop.time() >= deadline:
                logger.debug(
                    "cTrader deal-history bridge for order %d deferred: "
                    "reconcile pass budget spent mid-read (%s)", order_id, exc,
                )
            else:
                logger.warning(
                    "cTrader deal-history bridge for order %d failed: %s",
                    order_id, exc,
                )
            return _DealBridgeResult(None, 0, None, False)
        return walk.result(close_position_id)

    def _clear_missing_pending(self, row) -> None:
        """Drop a stale ``missing_pending_since`` breadcrumb (the row came back)."""
        if self.store_ctx is None:
            return
        patched = {k: v for k, v in (row.extras or {}).items()
                   if k != 'missing_pending_since'}
        self.store_ctx.upsert_order(row.client_order_id, extras=patched)

    async def _emit_unexpected_cancellations(self) -> AsyncIterator[OrderEvent]:
        """Retire bot-owned rows missing past the grace window, as cancels.

        Drives the core tracker's grace-expiry protocol. A row stamped
        ``missing_pending_since`` (by the presence feed for settled
        positions, or by the snapshot bridge's positive no-fill evidence
        for working orders) is only acted on once the grace window
        (:data:`_MISSING_PENDING_GRACE_S`) has elapsed, and never blindly:
        each expired row is re-verified against a fresh deal-history read
        — see :meth:`_confirm_missing_via_deal_history` for the
        close-versus-fill-versus-cancel classification. A no-fill / no-close
        disappearance is retired (``rejected``) with a synthetic
        ``cancelled`` event — so the engine's ``_route_event`` clears it from
        ``_order_mapping`` and re-syncs the strategy position, exactly as a
        live ``ORDER_CANCELLED`` PUSH event would. Only a cancel the evidence
        shows to be someone else's (an expiry the bot never set) applies the
        configured ``on_unexpected_cancel`` policy (``stop`` latches the
        engine's external-intervention quarantine; ``halt``, or an unwired
        quarantine sink, raises :class:`UnexpectedCancelError` through the
        reconcile pass). An order the venue itself ended (expired, rejected)
        is retired with its venue-driven ``cancel_reason``, and a cancel the
        evidence cannot attribute with an ERROR and an audit event; no
        policy applies to either.

        Runs as a separate pass AFTER :meth:`_reconcile_snapshot`. The
        presence namespaces are passed as ``None`` (fetch-failed marker)
        so this pass never stamps or clears — the snapshot pass owns
        presence. A no-op when persistence is off.
        """
        if self.store_ctx is None:
            return
        async for event in self._disappearance_tracker().observe(
                {'positions': None}, epoch_time()):
            yield event

    @staticmethod
    def _cancelled_event(row, now_ts: float) -> OrderEvent:
        """Build the synthetic cancelled event for a vanished bot-owned row.

        Carries the row's Pine identity (``pine_entry_id`` / ``LegType.ENTRY``)
        so the sync engine books the disappearance against the originating
        entry, exactly as the PUSH path's ``ORDER_CANCELLED`` events do.
        """
        order_id = (row.extras or {}).get('order_id')
        return OrderEvent(
            order=ExchangeOrder(
                id=str(order_id) if order_id else '', symbol=row.symbol,
                side=row.side, order_type=OrderType.MARKET,
                qty=row.qty, filled_qty=row.filled_qty,
                remaining_qty=max(0.0, row.qty - row.filled_qty),
                price=None, stop_price=None, average_fill_price=None,
                status=OrderStatus.CANCELLED, timestamp=now_ts, fee=0.0,
                fee_currency='', reduce_only=False,
                client_order_id=row.client_order_id,
            ),
            event_type='cancelled',
            fill_price=None, fill_qty=None, timestamp=now_ts,
            pine_id=row.pine_entry_id, from_entry=row.from_entry,
            leg_type=LegType.ENTRY, fee=0.0,
        )

    @staticmethod
    def _fill_event(
            row, *, event_type: str, status: OrderStatus, exchange_id: str,
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
