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

Where the facts of the filled-then-closed, vanished-row and shrink judgments
live. Each must reach the next pass and a later process of the run (a
restart), or be read from the venue again before a judgment needs it:

=============================  ==========================================  ===================================
Fact                           Lives in                                    After a restart
=============================  ==========================================  ===================================
Row order id, position id      row extras ``order_id`` / ``position_id``   journalled
                               and refs (``_record_recovered_refs``)
Row executed volume floor      row extras ``executed_cents``               journalled
Row deal window start          row extras ``submitted_at_ms``              journalled
Row missing-pending stamp      row extras ``missing_pending_since``        journalled; the vanished position's
                                                                           wait derives from it
Row fill cursor, retired share ``filled_qty``, extras                      journalled
                               ``journal_exposure_retired``
Row commission booked          ``filled_qty`` (commission is booked with   journalled; a recovery charges the
                               the fill) minus row extras                  executions beyond the cursor and
                               ``fee_unbooked``                            the listed ranges, never a deal
                                                                           the cursor counts
Fills journaled, not yet       ``ctrader_fill_delivery`` events, written   journalled; a fill without the
taken by the engine            with the journal write holding the fill,    engine's receipt is delivered
                               dropped once the engine's receipt shows     again, marked journaled before
                                                                           the restart
Close fills of a vanished      ``ctrader_vanished_close_fills`` events,    journalled; delivered again by the
row's judgment                 written with the deals judged, dropped      next judgment of a row still live
                               when the verdict retires the rows
Commission delivered without   ``ctrader_fee_delivery`` events, written    journalled; delivered again until
its fill                       with the ``fee_unbooked`` ranges it takes   the receipt accounts it
Fill de-dup deal ids           memory (``_seen_deal_ids``)                 not needed: the push stream never
                                                                           redelivers an earlier connection's
                                                                           fills; a share replay's ``seen``
                                                                           widens only its upper bound
Judged closing deals, origins  ``ctrader_close_judged`` events             journalled
Own closes sent, what each     ``ctrader_own_close`` events, written on    journalled
executed, completion           send, answer, refusal and every own fill
Outcome of a sent close        ``ctrader_close`` row under the close's     journalled; a live row is ended by
                               client order id, written with the record    the pass from the record and the
                               before the send; ``confirmed`` /            close order's status
                               ``rejected`` events
External-close reports         report pending / done events                journalled
Startup shrinks                startup shrink pending / done events        journalled
Covered-until watermark        memory + ``ctrader_close_coverage`` event   journalled
Resume-accepted watermark      memory + ``ctrader_close_coverage`` event   journalled; written in the
                                                                           transaction that retires the
                                                                           accepted shrink
Expiry overlap                 memory + ``ctrader_close_coverage`` event   journalled
Shortfall wait (first seen,    memory + ``ctrader_close_coverage`` event   journalled; ended by a startup
deadline, watermark at start)                                              shrink the baseline retired
Shrink confirmations, judged   memory (``_shrink_watch``,                  re-derived: the count restarts from
shrink keys                    ``_shrink_judged``)                         the venue, and the judged deal ids
                                                                           keep a re-judgment from booking a
                                                                           deal twice
Deal walks                     memory (``_deal_walks``)                    re-read from the venue over the
                                                                           journalled window starts
Resume-accepted positions      row marks of the operator resume            re-derived from the marks
Snapshot open positions        memory                                      re-read on the first snapshot
=============================  ==========================================  ===================================
"""
import asyncio
import logging
import uuid
from abc import ABC
from functools import partial
from itertools import combinations
from dataclasses import dataclass, field, replace
from time import monotonic, time as epoch_time
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, AsyncIterator, cast

from pynecore.core.broker.disappearance import (
    MISSING_PENDING_EXTRA,
    DisappearanceTracker,
    MissingConfirmation,
    MissingResolution,
)
from pynecore.core.broker.exceptions import BrokerError
from pynecore.core.broker.journal import DispatchJournal, ReconcileOutcome
from pynecore.core.broker.models import (
    FEE_EVENT_TYPE,
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
    QuarantineKind,
)
from pynecore.core.broker.store_helpers import PENDING_DISPATCH_STATES
from pynecore.core.broker.sync_engine import FILL_RECOVERY_HOLD_S

from ._base import EXECUTION_HANDED_OVER_EXTRA, _UNKNOWN_DISPATCH_TTL_S
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
    _CLOSE_ROW_STATE,
    _FILLED_THEN_CLOSED_EVENT,
    _ENTRY_ROW_STATES,
    _OWN_CLOSE_EVENT,
    _VOLUME_TOL,
    CloseOrigin,
    _accepted_by_resume,
    _close_record_outcome,
    _ended_close_verdict,
    _foreign_claims_taken,
    _OwnClose,
    _PositionDeal,
    _ProtectionMixin,
    _ShareReplay,
)
from .wire import CTraderProtocolError, CTraderTimeoutError, CTraderWireError

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow, RunContext

logger = logging.getLogger(__name__)

#: Fallback deal-history window (seconds) used ONLY when a row carries no
#: ``submitted_at_ms`` since-anchor — the zero-row crash-before-persist case the
#: M3 startup recovery (2.2) owns. Every row this loop persists carries the
#: anchor, so the runtime path is a per-order since-cursor, NOT this fixed
#: window: a fill never ages out of the lookback because the cursor reaches back
#: to the order's own submit time.
_DEAL_LOOKBACK_S = 300.0
#: Delay (s) before a row adopted in the run reads its order's deal history
#: again when a read left executions of the order unbooked; doubled after each
#: such read up to :data:`_ADOPTION_READ_MAX_DELAY_S`.
_ADOPTION_READ_FIRST_DELAY_S = 5.0
_ADOPTION_READ_MAX_DELAY_S = 300.0

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

#: Seconds, from the first reconcile pass that showed an open position below
#: the bot's share, a confirmed shortfall no deal of the position's history
#: explains stays in the journal waiting for the close that explains it (its
#: push, or the deal history showing it on a later pass). The engine holds its
#: verdict on the venue change while :meth:`~_ReconcileMixin.fill_recovery_pending`
#: reports it, for up to its fill-report grace plus
#: :data:`~pynecore.core.broker.sync_engine.FILL_RECOVERY_HOLD_S` from when it
#: first saw the change. With the engine's book at the journal share it sees
#: the change no earlier than that pass, so the wait ends while the engine
#: still holds, and a close delivered during the wait is the engine's only
#: booking of the shortfall. Past it the judgment still waits for an own close
#: that may execute, within that close's grace (:meth:`~_ReconcileMixin._judge_shrink`).
_UNEXPLAINED_SHRINK_WAIT_S = FILL_RECOVERY_HOLD_S

#: Audit of a confirmed shortfall no closing deal explained within
#: :data:`_UNEXPLAINED_SHRINK_WAIT_S`, retired from the journal unbound.
_SHRINK_UNEXPLAINED_EVENT = 'ctrader_position_shrink_unexplained'

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

#: Journal event of a position's close coverage: the watermark up to which
#: reconcile passes retired its closes, its expiry overlap and the running
#: wait of its unexplained shortfall. A keyed event
#: (:meth:`~pynecore.core.broker.storage.RunContext.replace_keyed_events`): the
#: run holds one per position, which a later process of the run takes over.
_CLOSE_COVERAGE_EVENT = 'ctrader_close_coverage'

#: Journal event of the close fills the judgment of a vanished row recovered
#: for its position, with the position's live rows at that judgment
#: (:meth:`~_ReconcileMixin._settle_vanished_close`). A keyed event, one per
#: position, written in the transaction that records the deals judged, and
#: dropped in the one that retires the position's rows
#: (:meth:`~_ReconcileMixin._closed_position_siblings`).
_VANISHED_CLOSE_EVENT = 'ctrader_vanished_close_fills'

#: Row extra listing the ``[from, to, fill id]`` unit ranges of the row's fill
#: cursor that a snapshot-only reconcile booked without their commission (the
#: deal history could not cover them), each with the fill id the booking
#: carried (:func:`_snapshot_fill_id`). Commission is otherwise booked with the
#: fill that advances the cursor, so a recovery charges exactly the executions
#: beyond the cursor plus these ranges, and clears the ranges it charged. The
#: commission of a range is delivered as a fee event of the range's fill id,
#: so it reaches the trade that booking opened, never the trade of a later
#: slice of the same deal; it leaves the ranges in the same transaction.
_FEE_UNBOOKED_EXTRA = 'fee_unbooked'

#: Row extra: the commission the fee events of each snapshot-booked fill
#: delivered so far (fill id -> total). A fee event carries a fill's total
#: commission, and a range can be charged in parts (a deal covering only part
#: of it), so its next fee event carries this total plus the new part.
_FEE_RANGED_EXTRA = 'fee_ranged'


def _vanished_cause(order_id: int) -> str:
    """The quarantine cause of a vanished order the venue reports as not found."""
    return f"vanished_order:{order_id}"


def _snapshot_fill_id(client_order_id: str, end: float) -> str:
    """The fill id of an entry slice a snapshot-only reconcile booked.

    The slice names no deal; it is identified by its row and the cumulative
    fill it ends at, which no other slice of the row shares.

    :param client_order_id: The entry row.
    :param end: The row's cumulative fill after the slice (units).
    """
    return f'{client_order_id}:snapshot:{raw_volume(end)}'


def _range_terms(client_order_id: str, entry) -> tuple[float, float, str]:
    """``(from, to, fill id)`` of a ``fee_unbooked`` range.

    A range journaled without its fill id is given the id of a slice ending
    where it ends (:func:`_snapshot_fill_id`).
    """
    start, end = float(entry[0]), float(entry[1])
    fill_id = entry[2] if len(entry) > 2 and entry[2] else _snapshot_fill_id(
        client_order_id, end)
    return start, end, str(fill_id)


def _charge_ranged_fees(
        extras: Mapping, charges: Mapping[str, float], ranges_left: Sequence,
) -> tuple[dict[str, float], dict[str, float] | None]:
    """The fee totals of snapshot-booked fills after a charge of their ranges.

    :param extras: The row's extras before the charge.
    :param charges: Fill id -> the commission of its range charged now.
    :param ranges_left: The ``fee_unbooked`` ranges left after the charge.
    :return: ``(fill id -> the total its fee event carries, the
        ``fee_ranged`` extra to write)``; the extra keeps only the fills a
        range is still left for, ``None`` when none is.
    """
    totals = {str(key): float(value)
              for key, value in (extras.get(_FEE_RANGED_EXTRA) or {}).items()}
    charged: dict[str, float] = {}
    for fill_id, fee in charges.items():
        if fee:
            totals[fill_id] = charged[fill_id] = totals.get(fill_id, 0.0) + fee
    open_ids = {str(entry[2]) for entry in ranges_left if len(entry) > 2}
    kept = {key: value for key, value in totals.items() if key in open_ids}
    return charged, kept or None


@dataclass(frozen=True)
class _RecoveredCommission:
    """The commission a recovered entry fill books
    (:meth:`_ReconcileMixin._recovered_commission`)."""
    #: Deal id -> the commission of its part beyond the cursor.
    fees: dict[int, float]
    #: The deal ids beyond the cursor not applied yet.
    new_ids: set[int]
    #: Fill id of a snapshot booking -> the total commission its fee event carries.
    ranged: dict[str, float]
    #: The extras patch of the ranges and totals left; ``None`` when the row
    #: lists no range.
    patch: dict | None

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
        transport error (so a no-fill result deterministically means no fill)
        and every fill of the order it holds carries a price.
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
        closedVolume, side, executionPrice, commission)`` by ``dealId`` of
        every deal in the window that executed volume.
    :ivar last_request: Event-loop time the walk last sent a page request
        (``-inf`` before its first), so unfinished walks take turns.
    """
    from_ms: int
    high_ms: int
    segments: list[list[int]] = field(default_factory=list)
    fills: 'dict[int, OpenApiModelMessages.ProtoOADeal]' = field(default_factory=dict)
    closes: dict[int, tuple[int, int, int, int]] = field(default_factory=dict)
    deals: dict[int, tuple[int, int, int, int, int, str, float, float]] = field(
        default_factory=dict)
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
                if deal.HasField('closePositionDetail') else 0,
                'buy' if deal.tradeSide == OpenApiModelMessages.ProtoOATradeSide.BUY
                else 'sell',
                deal.executionPrice, money_value(deal.commission, deal.moneyDigits))
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
        # ``executionPrice`` is optional: a fill of the order the history lists
        # without it cannot be booked (an unpriced fill books nothing), so the
        # window proves nothing yet and is judged once the deal is priced. The
        # position's other deals stay volume and identity evidence: a close
        # among them is booked only once priced
        # (:meth:`~_ProtectionMixin._judge_recovered_close_deals`).
        unpriced = sorted(d.dealId for d in fills if d.executionPrice <= 0.0)
        if unpriced:
            logger.warning(
                "cTrader: the deal history lists fills of the order without a price (%s); "
                "judged once they are priced", unpriced)
            return _DealBridgeResult(None, 0, None, False)
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
                               filled=filled, closed=min(closed, filled), side=side,
                               price=price, fee=fee)
                 for deal_id, (position_id, order_ref, executed_ms, filled, closed, side,
                               price, fee)
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
        self._load_close_evidence()
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
        self._release_settled_vanished_causes(rows)
        live_position_ids = {
            parse_protocol_id(raw, field='position_id')
            for raw in ((row.extras or {}).get('position_id') for row, _order_id in rows)
            if raw is not None}
        # Position-history walks are keyed by the negated position id
        # (:meth:`_position_history`); they live while their position is open,
        # a shrink a restart found on it waits for its judgment, or a live row
        # of the bot's still holds it (its judgment resumes the walk).
        for stale in [oid for oid in self._deal_walks
                      if (oid not in live_order_ids if oid >= 0
                          else -oid not in open_positions
                          and -oid not in self._startup_shrinks
                          and -oid not in live_position_ids)]:
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
            if order_id is not None and extras.get(EXECUTION_HANDED_OVER_EXTRA):
                # Handed to the engine's held-execution recovery: it owns the
                # order (its absence is no disappearance) until it settled it,
                # and then the row takes the outcome from the venue.
                if self._execution_handed_over(order_id):
                    if 'missing_pending_since' in extras:
                        self._clear_missing_pending(row)
                else:
                    for event in await self._settle_handed_over_row(
                            row, order_id, open_positions):
                        filled_coids.append(row.client_order_id)
                        yield event
                continue
            adoption_owed = self._adoption_owed(row)
            if adoption_owed and not self._adoption_read_due(row.client_order_id):
                continue
            order = orders_by_id.get(order_id) if order_id is not None else None
            if order is not None:
                # The order is still in ``order[]`` — reconcile partial-fill
                # progress from ``executedVolume``. A partial that already linked
                # ``position_id`` does NOT divert the row to the position branch:
                # cTrader keeps the original order in ``order[]`` while assigning a
                # ``positionId`` after the first partial, so later partials must
                # still be diffed here.
                events = await self._reconcile_working_row(
                    row, order, open_positions, now_ts)
                if adoption_owed:
                    self._pace_adoption_read(row.client_order_id)
                if events:
                    filled_coids.append(row.client_order_id)
                    for event in events:
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
                # ``positions`` namespace for the presence feed below. Only the
                # commission of a fill a snapshot-only pass booked without it is
                # still owed.
                if order_id is not None and extras.get(_FEE_UNBOOKED_EXTRA):
                    for event in await self._settled_row_range_commission(
                            row, order_id, str(position_id), now_ts):
                        yield event
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
                    row, order_id, open_positions, now_ts, working_orders=orders_by_id):
                filled_coids.append(row.client_order_id)
                yield event
            if adoption_owed:
                self._pace_adoption_read(row.client_order_id)
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
        for event in await self._judge_position_shrinks(
                open_positions, read_started_at=read_started_at, queue_idle=queue_idle):
            yield event
        await self._judge_ambiguous_close_claims()
        await self._resolve_close_rows()

    async def _order_details_response(
            self, order_id: int) -> 'OpenApiMessages.ProtoOAOrderDetailsRes | None':
        """An order's own details (``ProtoOAOrderDetailsReq``), within the pass budget.

        The answer lists every deal the order's fills created, whatever its
        age, so it is the venue's word on the order's execution where the
        time-windowed deal history falls short (:meth:`_details_executed_cents`).

        :param order_id: The venue order.
        :return: The details; ``None`` when the read failed or the venue does
            not find the order.
        """
        if self._live_account_id is None:
            return None
        deadline = self._reconcile_pass_deadline
        try:
            async with (asyncio.timeout_at(deadline) if deadline is not None
                        else asyncio.timeout(REQUEST_TIMEOUT)):
                return cast(OpenApiMessages.ProtoOAOrderDetailsRes, await self._account_request(
                    OpenApiMessages.ProtoOAOrderDetailsReq(
                        ctidTraderAccountId=self._live_account_id, orderId=order_id)))
        except (BrokerError, CTraderWireError, TimeoutError) as exc:
            logger.debug("cTrader order %d details read deferred: %s", order_id, exc)
            return None

    @staticmethod
    def _details_executed_cents(res: 'OpenApiMessages.ProtoOAOrderDetailsRes') -> int:
        """What an order executed by its details: the larger of its
        ``executedVolume`` and its executed deals' sum (cents)."""
        return max(int(res.order.executedVolume), sum(
            int(deal.filledVolume) for deal in res.deal
            if deal.filledVolume > 0 and deal.dealStatus in _EXECUTED_DEAL_STATUSES))

    @staticmethod
    def _adoption_owed(row) -> bool:
        """Whether a row adopted in the run still owes executions of its order.

        The row's ``executed_cents`` floor (what the order executed when its
        fill was held, :meth:`~_EventStreamMixin._held_for_adoption_read`)
        is above the cursor: the order's deal history must book the rest.
        """
        extras = row.extras or {}
        return bool(extras.get('adopted_in_run')) and row.filled_qty < volume_to_units(
            int(extras.get('executed_cents') or 0)) - _VOLUME_TOL

    def _adoption_read_due(self, coid: str) -> bool:
        """Whether the deal history of an adopted row's order is read on this pass.

        The first read is due at once; one that leaves the row owing waits
        :data:`_ADOPTION_READ_FIRST_DELAY_S`, doubling up to
        :data:`_ADOPTION_READ_MAX_DELAY_S`, and is never given up
        (:meth:`_pace_adoption_read`).
        """
        pace = self._adoption_reads.get(coid)
        return pace is None or monotonic() >= pace[0]

    def _pace_adoption_read(self, coid: str) -> None:
        """Schedule the next read of an adopted row's order, or end its pacing once settled."""
        store = self.store_ctx
        row = store.get_order(coid) if store is not None else None
        if row is None or row.closed_ts_ms is not None or not self._adoption_owed(row):
            self._adoption_reads.pop(coid, None)
            return
        delay = self._adoption_reads.get(coid, (0.0, _ADOPTION_READ_FIRST_DELAY_S))[1]
        self._adoption_reads[coid] = (
            monotonic() + delay, min(delay * 2.0, _ADOPTION_READ_MAX_DELAY_S))
        logger.warning(
            "cTrader: row %s adopted in the run still owes executions of its order; its deal "
            "history is read again in %.0f s", coid, delay)

    def fill_recovery_pending(self, entry_ids: Collection[str]) -> bool:
        """Whether the reconcile pass still owes the engine fills for these entries.

        Answered from the latest reconcile snapshot — the one the engine's
        position check read through :meth:`get_position` — so a change is
        pending from the moment the venue shows it, before any pass stamped or
        judged it. Pending while a fully filled entry row of the entries is
        live although its position is gone (the deal history has not shown who
        closed it yet: :meth:`_confirm_missing_via_deal_history`), or while a
        position of theirs holds less on the bot's side than the bot's share
        (the shrink is not judged yet: :meth:`_judge_position_shrinks`). A
        position whose external close was reported has its report pending
        instead. Also pending while the disappearance tracker holds a stamp on
        one of their rows (:meth:`BrokerPlugin.fill_recovery_pending`).
        """
        if super().fill_recovery_pending(entry_ids):
            return True
        positions = self._snapshot_open_positions
        store = self.store_ctx
        if positions is None or store is None:
            return False
        for row in store.iter_live_orders():
            raw = (row.extras or {}).get('position_id')
            if (row.pine_entry_id not in entry_ids or row.state not in _ENTRY_ROW_STATES
                    or raw is None or row.filled_qty <= _VOLUME_TOL):
                continue
            position_id = parse_protocol_id(raw, field='position_id')
            if position_id in self._external_close_reported:
                continue
            position = positions.get(position_id)
            if position is None:
                if row.filled_qty >= row.qty - _VOLUME_TOL:
                    return True
                continue
            share = self._own_share(position_id)
            if (share is not None
                    and self._side_volume(position_id, position) < share - _VOLUME_TOL):
                return True
        return False

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
        return self._share_with_recovered_fills(position_id, [(row, recovered)])

    def _share_with_recovered_fills(
            self, position_id: int, recovered: Sequence[tuple['OrderRow', float]],
    ) -> float:
        """The bot's share of a position counting rows' fills recovered from history.

        :param recovered: ``(row, units)`` of each row and the cumulative fill
            (units) the history shows for its order.
        """
        rows = self._position_entry_rows(position_id)
        linked = {r.client_order_id for r in rows}
        net = sum(self._signed_exposure(r) for r in rows)
        for row, units in recovered:
            counted = row.filled_qty if row.client_order_id in linked else 0.0
            added = max(0.0, min(row.qty, units) - counted)
            net += added if row.side == 'buy' else -added
        return abs(net)

    async def close_leg_executed(
            self, symbol: str, leg_id: str, coids: Sequence[str], since_ms: int,
    ) -> int | None:
        """Volume (centi-units) the closes sent under ``coids`` executed on ``leg_id``.

        :class:`~pynecore.core.plugin.broker.PositionPort` primitive. A
        ``ProtoOAClosePositionReq`` carries no client order id, so each send is
        found through the own-close record :meth:`_send_own_close` journals
        under its ``coid`` BEFORE the request is written, read across every
        process instance of the run (:meth:`_close_send_records`):

        * no record — the send never reached the wire: it executed nothing;
        * a refused record — the venue provably refused it: nothing;
        * a record whose answer named the close order — that order's own
          status (``ProtoOAOrderDetailsReq``) settles it: a filled or ended
          order executed its ``executedVolume``; one still accepted, or a
          lookup that failed or found no order, cannot settle it yet;
        * a record without the order (the answer was lost) — only the
          position's deal history can show what it did, and how late the
          venue publishes a closing deal there is not measured: a read that
          shows less than every such send could have closed is no proof that
          it closed less, so the answer waits. Once the closing deals of the
          window (from ``since_ms`` less :data:`_SINCE_ANCHOR_SKEW_MS`, whoever
          sent them — the contract's over-count rule) cover everything those
          sends and the settled ones could have closed, their sum is the
          answer.

        ``None`` also while the snapshot shows a closing order still working on
        the position (a close in flight may execute more), and without a
        journal to read.
        """
        del symbol
        position_id = int(leg_id)
        records = self._close_send_records(coids)
        if records is None:
            return None
        snapshot = await self._reconcile()
        if any(order.closingOrder and order.positionId == position_id
               for order in snapshot.order):
            return None
        settled = 0
        unanswered = 0
        for payload in records:
            if payload.get('refused'):
                continue
            order_id = payload.get('order_id')
            if order_id:
                executed = await self._close_order_executed(int(order_id))
                if executed is None:
                    return None
                proven = payload.get('executed')
                settled += max(executed, raw_volume(float(proven))
                               if isinstance(proven, (int, float)) else 0)
                continue
            volume = payload.get('volume_cents')
            if not isinstance(volume, int):
                return None
            unanswered += volume
        if not unanswered:
            return settled
        from_ms = since_ms - _SINCE_ANCHOR_SKEW_MS
        history = await self._find_fill_deal(
            -position_id, from_ms, close_position_id=position_id)
        if not history.conclusive:
            return None
        closed = sum(deal.closed for deal in history.position_deals
                     if deal.executed_ms >= from_ms)
        return closed if closed >= settled + unanswered else None

    def _close_send_records(self, coids: Sequence[str]) -> list[dict] | None:
        """The last journalled state of every own close sent under ``coids``.

        Read across every process instance of the logical run, so a send an
        earlier process made counts; one state per close record (its latest).
        ``None`` without a journal to read.
        """
        store = self.store_ctx
        if store is None:
            return None
        wanted = set(coids)
        latest: dict[str, dict] = {}
        for _intent, _coid, _ref, payload in store.iter_events_by_kind_for_run_id(
                _OWN_CLOSE_EVENT):
            key = payload.get('key')
            if isinstance(key, str) and payload.get('coid') in wanted:
                latest[key] = payload
        return list(latest.values())

    async def _close_order_executed(self, order_id: int) -> int | None:
        """Volume (centi-units) an own close order executed, once it can execute no more.

        Filled, rejected, expired and cancelled orders execute no more.
        ``None`` while the venue reports the order accepted (it may execute
        more) or in any other status, when it reports no such order, when its
        answer does not carry the executed volume, or when the lookup failed.
        """
        ended = await self._ended_close_order(order_id)
        return None if ended is None else ended[1]

    async def _ended_close_order(self, order_id: int) -> tuple[int, int | None] | None:
        """The status and executed volume (centi-units) of an own close order that ended.

        :return: ``(orderStatus, executedVolume)`` of a filled, rejected,
            expired or cancelled order, the volume ``None`` when the answer
            does not carry it; ``None`` while the venue reports the order
            accepted or in any other status, when it reports no such order,
            or when the lookup failed.
        """
        if self._live_account_id is None:
            return None
        try:
            res = cast(OpenApiMessages.ProtoOAOrderDetailsRes, await self._account_request(
                OpenApiMessages.ProtoOAOrderDetailsReq(
                    ctidTraderAccountId=self._live_account_id, orderId=order_id)))
        except (BrokerError, CTraderWireError, TimeoutError) as exc:
            logger.debug("cTrader close order %d lookup deferred: %s", order_id, exc)
            return None
        order = res.order
        statuses = OpenApiModelMessages.ProtoOAOrderStatus
        if order.orderStatus not in (statuses.ORDER_STATUS_FILLED, statuses.ORDER_STATUS_REJECTED,
                                     statuses.ORDER_STATUS_EXPIRED,
                                     statuses.ORDER_STATUS_CANCELLED):
            return None
        return int(order.orderStatus), (
            int(order.executedVolume) if order.HasField('executedVolume') else None)

    async def _resolve_close_rows(self) -> None:
        """End the close rows whose outcome the evidence now proves.

        Every live close row (:data:`~pynecore_ctrader.protection._CLOSE_ROW_STATE`)
        is judged from the close's latest journalled record, whichever process
        of the run sent it; the same verdict a record of this process gives as
        it changes (:func:`~pynecore_ctrader.protection._close_record_outcome`).
        A record that proves nothing yet is completed from the venue:

        * the answer named the close order — that order's status
          (``ProtoOAOrderDetailsReq``): executed volume proves the close
          executed; rejected with none, that the venue refused it; cancelled
          or expired with none, that it ended executing nothing
          (``'ended_unfilled'``, journaled with the core's ended-unfilled
          mark); an order still accepted, or a
          lookup that failed, leaves the row live;
        * the request was never answered — in flight in this process, the row
          stays live; sent by a process that is gone, nothing will show what it
          did (``'send_interrupted'``).

        Each verdict and its writes share a judgment span
        (:meth:`~_ProtectionMixin._judgment_span`).
        """
        store = self.store_ctx
        if store is None:
            return
        rows = [row for row in store.iter_live_orders() if row.state == _CLOSE_ROW_STATE]
        for row in rows:
            coid = row.client_order_id
            payloads = self._close_send_records([coid]) or []
            outcome: tuple[str, int | None, float] | None
            if not payloads:
                outcome = 'no_record', None, 0.0
            else:
                # The send the row tracks is the latest one under its id.
                payload = payloads[-1]
                raw_order = payload.get('order_id')
                order_id = int(raw_order) if isinstance(raw_order, int) and raw_order else None
                executed = payload.get('executed')
                raw_volume = payload.get('volume_cents')
                volume_cents = raw_volume if isinstance(raw_volume, int) else None
                outcome = _close_record_outcome(
                    refused=bool(payload.get('refused')), order_id=order_id,
                    executed=float(executed) if isinstance(executed, (int, float)) else 0.0,
                    answered=payload.get('answered_ms') is not None,
                    complete=bool(payload.get('complete')),
                    volume=volume_to_units(volume_cents) if volume_cents is not None else None)
                if outcome is None and order_id is not None:
                    outcome = await self._close_order_outcome(
                        order_id, volume_cents,
                        proven=float(executed) if isinstance(executed, (int, float)) else 0.0)
                elif outcome is None and not any(
                        record.coid == coid and record.answered_ms is None
                        for record in self._own_closes):
                    outcome = 'send_interrupted', None, 0.0
            if outcome is None:
                continue
            with self._judgment_span():
                current = store.get_order(coid)
                if (current is not None and current.state == _CLOSE_ROW_STATE
                        and current.closed_ts_ms is None):
                    self._finish_close_row(current, *outcome)

    async def _close_order_outcome(
            self, order_id: int, volume_cents: int | None = None, *, proven: float = 0.0,
    ) -> tuple[str, int | None, float] | None:
        """The outcome an own close order's venue status proves (:meth:`_resolve_close_rows`).

        A cancelled or expired order that executed part of the close's
        ``volume_cents`` ended executing that much and no more
        (``'ended_partially'``); any other executed volume proves the close
        executed. The executed volume is never taken below ``proven``, and an
        answer without it proves only that (:func:`_ended_close_verdict`).

        :param order_id: The close's venue order.
        :param volume_cents: The volume the close was sent for, when recorded.
        :param proven: Units the close's own fills, matched by its order id,
            executed.
        :return: ``(outcome, order_id, executed units)``, or ``None`` while the
            status proves nothing.
        """
        ended = await self._ended_close_order(order_id)
        if ended is None:
            return None
        status, executed_cents = ended
        statuses = OpenApiModelMessages.ProtoOAOrderStatus
        if status == statuses.ORDER_STATUS_FILLED:
            executed = max(
                volume_to_units(executed_cents) if executed_cents is not None else 0.0, proven)
            return ('executed', order_id, executed) if executed > _VOLUME_TOL else None
        return _ended_close_verdict(
            rejected=status == statuses.ORDER_STATUS_REJECTED, order_id=order_id,
            executed_cents=executed_cents, proven=proven, volume_cents=volume_cents)

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
    ) -> list[OrderEvent]:
        """Judge open positions holding less than the bot's share.

        A partial close the PUSH stream missed (the stream was down) leaves a
        run-owned position open below the bot's journal share. Once the same
        shortfall shows on :data:`_SHRINK_CONFIRMATIONS` consecutive passes —
        with nothing of the bot's in flight and no event waiting in the queue,
        so the journal share is final — the position's deal history is judged
        like a recovered close (:meth:`_judge_recovered_close_deals`) and the
        share those deals took retired from the journal: the venue is the
        truth. That can be more than the shortfall when the venue counts
        another party's exposure too (a NETTING position); never less. Each
        deal that took part of the share is delivered for that part as a close
        fill (:meth:`_recovered_close_event`), the fill the PUSH stream would
        have delivered, under the identity the PUSH path gives it
        (:meth:`_recovered_close_identity`); its part is retired from the
        entry the core settles it against first. A shortfall no deal explains
        stays in the journal share until :data:`_UNEXPLAINED_SHRINK_WAIT_S`
        after the first pass that showed the position below the share
        (:attr:`~_CTraderBase._unexplained_shrinks`): the close that explains it, pushed or
        shown by the deal history on a later pass, is booked then like one that
        arrived on time, under its own deal id and price, and retires its
        volume once. Past the wait the shortfall is retired from the rows
        oldest first with an ERROR and an audit event, and the engine's verdict
        on the venue change is no longer held. A shrink a
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
        :return: The recovered close fills of the shrinks judged now.
        """
        events: list[OrderEvent] = []
        if self.store_ctx is None:
            return events
        for position_id in [pid for pid in self._shrink_watch if pid not in open_positions]:
            del self._shrink_watch[position_id]
        for position_id in {pid for watched in (self._shrink_seen_since,
                                                self._unexplained_shrinks,
                                                self._shrink_covered_from)
                            for pid in watched if pid not in open_positions}:
            self._forget_shrink_wait(position_id)
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
                self._forget_shrink_wait(position_id)
                self._resume_accepted_positions.discard(position_id)
                continue
            venue = self._side_volume(position_id, position)
            startup = self._startup_shrinks.get(position_id)
            if startup is None and venue >= share - _VOLUME_TOL:
                self._shrink_watch.pop(position_id, None)
                self._forget_shrink_wait(position_id)
                self._resume_accepted_positions.discard(position_id)
                # A later shrink to the same volumes is a new one: the deal
                # ids keep a close from being judged twice across them.
                self._shrink_judged = {
                    judged for judged in self._shrink_judged if judged[0] != position_id}
                if (abs(venue - share) <= _VOLUME_TOL and queue_idle
                        and self._share_settled(position_id)):
                    self._close_expiry_overlap(position_id, read_started_at)
                continue
            seen_since = self._shrink_seen_since.setdefault(position_id, read_started_at)
            if not queue_idle or not self._share_settled(position_id):
                continue
            if position_id in self._resume_accepted_positions:
                rows = self._position_entry_rows(position_id)
                # The closes are accepted unjudged: their coverage is written
                # with the retirement, so no later judgment books them. A
                # failed write keeps the acceptance for the next pass.
                with self._judgment_span():
                    self._resume_accepted_positions.discard(position_id)
                    self._cover_closes(position_id, read_started_at, accepted=True)
                    self._note_accepted_by_resume(
                        position_id, client_order_id=rows[0].client_order_id if rows else None,
                        volume=share - venue)
                    self._retire_partial_close_exposure(
                        position_id, share - venue, source='resume_accepted')
                continue
            if startup is not None:
                # A close an earlier pass, or an earlier process, retired is
                # not taken again, as in a shrink judged at runtime.
                await self._judge_shrink(
                    position_id, position, venue=venue, share_now=share + startup.volume,
                    read_started_at=read_started_at,
                    settle=partial(self._settle_startup_shrink, position_id,
                                   share=share, venue=venue, baseline=startup.volume,
                                   read_started_at=read_started_at))
                continue
            key = (raw_volume(venue), raw_volume(share))
            if (position_id, key) in self._shrink_judged:
                continue
            seen_key, count = self._shrink_watch.get(position_id, (None, 0))
            count = count + 1 if seen_key == key else 1
            self._shrink_watch[position_id] = (key, count)
            if count < _SHRINK_CONFIRMATIONS:
                continue
            walk = self._deal_walks.get(-position_id)
            if position_id in self._unexplained_shrinks and walk is not None and not walk.segments:
                # The close that explains the shortfall executed before an
                # earlier read of the deal history, which did not show it: the
                # history is read again from the window's start, not only
                # since that read. A reread the pass budget cut short resumes
                # where it stopped.
                del self._deal_walks[-position_id]
            covered_from = self._shrink_covered_from.get(
                position_id, self._closes_covered_until.get(position_id, 0))
            deadline = seen_since + _UNEXPLAINED_SHRINK_WAIT_S
            # Within the wait, an own close sent after the expiry that may
            # have executed unseen is waited for, not guessed at; past it, only
            # one that may still execute is, bounded by its grace, as the
            # vanished rows' judgment waits (:meth:`_confirm_missing_via_deal_history`).
            await self._judge_shrink(
                position_id, position, venue=venue, share_now=share,
                read_started_at=read_started_at, hold_for_own_close=True,
                still_executing=read_started_at >= deadline,
                settle=partial(self._settle_shrink, position_id, events,
                               share=share, venue=venue, key=key, covered_from=covered_from,
                               deadline=deadline, read_started_at=read_started_at))
        return events

    def _settle_startup_shrink(
            self, position_id: int, replay: _ShareReplay, history: Sequence[_PositionDeal], *,
            share: float, venue: float, baseline: float, read_started_at: float,
    ) -> None:
        """Apply the judgment of a shrink the startup baseline retired.

        Runs in the transaction that records the deals judged
        (:meth:`~_ProtectionMixin._judge_recovered_close_deals`): what the
        deals took beyond the baseline is retired, the pending shrink ended and
        the closes covered.

        :param share: The bot's journal share now, after the baseline.
        :param venue: The volume (units) the position holds now.
        :param baseline: The share (units) the startup baseline retired.
        :param read_started_at: Wall-clock time the snapshot read started.
        :param replay: The verdict on the deals judged now.
        :param history: Every deal of the position's history read.
        """
        _parts, retired = self._book_expiry_overlap(
            position_id, replay.booked, history,
            covered_from=self._closes_covered_until.get(position_id, 0),
            shortfall=share + baseline - venue)
        taken = volume_to_units(replay.taken - retired)
        self._end_startup_shrink(position_id)
        self._cover_closes(position_id, read_started_at)
        if taken - baseline > _VOLUME_TOL:
            self._retire_partial_close_exposure(
                position_id, taken - baseline, source='position_shrink')

    def _settle_shrink(
            self, position_id: int, events: list[OrderEvent], replay: _ShareReplay,
            history: Sequence[_PositionDeal], *, share: float, venue: float,
            key: tuple[int, int], covered_from: int, deadline: float, read_started_at: float,
    ) -> None:
        """Apply the judgment of a shrink a settled pass confirmed.

        Runs in the transaction that records the deals judged
        (:meth:`~_ProtectionMixin._judge_recovered_close_deals`), so a process
        that stops before a debit lands leaves the deals unjudged: the next
        process judges them again and books each against the entry it names.

        :param events: The pass's recovered close fills; the fills of the
            deals judged now are appended.
        :param share: The bot's journal share before the judgment.
        :param venue: The volume (units) the position holds now.
        :param key: The shrink's ``(venue, share)`` volumes, in raw volume.
        :param covered_from: The covered-until watermark the shrink's wait
            started at.
        :param deadline: The end of the shrink's wait for its explanation.
        :param read_started_at: Wall-clock time the snapshot read started.
        :param replay: The verdict on the deals judged now.
        :param history: Every deal of the position's history read.
        """
        rows = self._position_entry_rows(position_id)
        # A close executed before the closes earlier passes retired is
        # one of them (retired without its deal when no deal explained
        # it in time): judged, but neither delivered nor debited again;
        # in an expiry's overlap only what the shortfall leaves is.
        parts, retired = self._book_expiry_overlap(
            position_id, replay.booked, history, covered_from=covered_from,
            shortfall=share - venue)
        booked = [(deal, cents, self._recovered_close_identity(position_id, deal))
                  for deal, cents in parts]
        taken = volume_to_units(replay.taken - retired)
        if rows:
            fills = [self._recovered_close_event(rows[0], deal, cents, from_entry=label)
                     for deal, cents, (label, _settle) in booked
                     if deal.deal_id not in self._seen_deal_ids]
            # Debited below, in this transaction: the record goes with them.
            self._journal_fill_delivery(fills)
            events.extend(fills)
        self._seen_deal_ids.update(deal.deal_id for deal, _cents in replay.booked)
        self._cover_closes(position_id, read_started_at)
        debited = 0.0
        for _deal, cents, (_label, settle) in booked:
            debited += sum(volume for _coid, volume in self._retire_partial_close_exposure(
                position_id, volume_to_units(cents), source='position_shrink',
                entry_id=settle))
        rest = max(share - venue, taken) - debited
        unexplained = share - venue - taken
        judged = not (unexplained > _VOLUME_TOL and read_started_at < deadline)
        if not judged:
            # The pass judges the shrink again, with the deals the
            # history shows then.
            self._await_shrink_explanation(
                position_id, unexplained, deadline, covered_from=covered_from)
            rest -= unexplained
        else:
            # The shortfall is retired: one a later pass finds is a
            # new one, with a wait of its own.
            self._shrink_watch.pop(position_id, None)
            self._forget_shrink_wait(position_id)
            if unexplained > _VOLUME_TOL:
                self._note_unexplained_shrink(position_id, unexplained)
                self._cover_unexplained_retirement(position_id, read_started_at)
        if rest > _VOLUME_TOL:
            self._retire_partial_close_exposure(
                position_id, rest, source='position_shrink')
        if judged:
            # Keyed by the share the judgment LEFT: a shrink whose
            # retirement did not lower the share is not judged again,
            # while an entry fill that restores the share before the
            # next snapshot makes a later equal shrink a new one.
            share_after = self._own_share(position_id)
            self._shrink_judged.add((position_id, key if share_after is None else (
                key[0], raw_volume(share_after))))

    def _recovered_close_identity(
            self, position_id: int, deal: _PositionDeal,
    ) -> tuple[str | None, str | None]:
        """The label and settlement entry of a closing deal a shrink pass recovered.

        The identity the PUSH path gives the same fill: a close of the run
        the deal belongs to (:meth:`_own_close_of_deal`) gives its own label
        and the entry recorded with it; otherwise the run's exposure in the
        position does, naming its entry only when one entry holds it
        (:meth:`_run_owned_position_entry`). With several entries the close
        names none, and the core settles it oldest first, as the journal
        retires it.

        :param position_id: The position the deal closed volume of.
        :param deal: The closing deal from the position's deal history.
        :return: ``(from_entry, settle_entry)``.
        """
        record = self._own_close_of_deal(
            position_id, deal, self._judged_close_origins.get(deal.deal_id))
        if record is not None:
            return record.from_entry, record.settle_entry
        owners = self._run_owned_position_entry(position_id)
        owner = owners[0] if owners is not None and len(owners) == 1 else None
        return owner, owner

    def _recovered_close_coid(self, deal: _PositionDeal) -> str | None:
        """The client order id of the run's close a recovered closing deal belongs to.

        As on the PUSH path (:meth:`~_ProtectionMixin._own_close_fill_coid`),
        only the close whose order the venue named the deal's carries its id
        over: a close of unknown order id whose window holds the deal gives
        the deal its label (:meth:`_own_close_of_deal`), never its id.
        ``None`` when no close of the run sent under a client order id is the
        deal's order.
        """
        self._load_close_evidence()
        record = next((record for record in self._own_close_history
                       if record.order_id == deal.order_id), None)
        return record.coid if record is not None else None

    def _vanished_close_label(
            self, position_id: int, deal: _PositionDeal, row_entry: str | None,
    ) -> str | None:
        """The label of a closing deal the judgment of a vanished row recovered.

        A close of the run the deal belongs to (:meth:`_own_close_of_deal`)
        gives its own label, as its push would; any other close is labelled
        with the vanished row's entry.
        """
        record = self._own_close_of_deal(
            position_id, deal, self._judged_close_origins.get(deal.deal_id))
        return record.from_entry if record is not None else row_entry

    def _reread_finished_walk(self, walk_key: int) -> None:
        """Read a vanished position's window again from its start for its judgment.

        A deal the history shows only after an earlier read, stamped before
        that read's end, is one a resumed walk never reads (it reads only from
        :data:`_SINCE_ANCHOR_SKEW_MS` before the end on): a close whose push
        was deferred to the history (an expiry overlap's), or a confirmed own
        close. The judgment that retires the rows therefore reads the whole
        window; a walk the pass budget cut short resumes where it stopped.
        """
        walk = self._deal_walks.get(walk_key)
        if walk is not None and not walk.segments:
            del self._deal_walks[walk_key]

    def _fill_history_short(
            self, bridge: '_DealBridgeResult', row, order_id: int, executed_cents: int = 0,
    ) -> bool:
        """Whether an entry order's deal evidence shows less than the fill known for it.

        A recovery that retires an entry row filled then closed judges the
        closes against the share its order's deals show, so the deals must
        cover every fill the plugin already knows the order made: the
        ``executedVolume`` the venue reports for it (``executed_cents``, when
        its order details were read in this pass, and the ``executed_cents``
        floor an earlier pass recorded on the row while it waited,
        :meth:`~_RecoveryMixin._record_recovered_refs`) and the fill the row's
        journal counts. A
        history short of that has not published every fill yet; the order's
        finished walk is dropped so the next pass reads its whole window again
        (a resumed walk reads only from shortly before its last end, and a
        late deal may be stamped earlier).

        :param bridge: The order's deal evidence.
        :param row: The order's entry row.
        :param order_id: The venue order (its deal walk's key).
        :param executed_cents: The order's ``executedVolume``; ``0`` when not read.
        :return: ``True`` while the recovery must wait for a later pass.
        """
        known = raw_volume(self._execution_evidence(
            row, reported=volume_to_units(int(executed_cents))).executed)
        if bridge.filled_cents >= known:
            return False
        self._reread_finished_walk(order_id)
        return True

    async def _with_group_position_history(
            self, bridge: '_DealBridgeResult', position_id: int, from_ms: int,
    ) -> '_DealBridgeResult | None':
        """An entry order's evidence widened to its closed position's history for the group.

        A recovery that finds an entry order filled then closed retires the
        bot's rows of the position and the other entry rows whose orders
        filled onto it (:meth:`_unbooked_position_entries`), so the closes are
        judged over the windows of all of them: the position's history from
        :meth:`_vanished_window_start`, lowered to the window start of every
        live entry row that may join the group (a working row not fully filled
        and a pending dispatch row), merged into ``bridge``. A sibling's
        opening deal before ``from_ms`` (the recovered order's own window) is
        then seen, and its fill counts in the share the closes are judged
        against.

        :return: The widened evidence; ``None`` while the history read is
            inconclusive (the caller waits for a later pass).
        """
        start = self._vanished_window_start(position_id, from_ms)
        for other in self.store_ctx.iter_live_orders() if self.store_ctx is not None else ():
            extras = other.extras or {}
            if other.filled_qty >= other.qty - _VOLUME_TOL:
                continue
            if other.state in PENDING_DISPATCH_STATES:
                start = min(start, other.created_ts_ms - _SINCE_ANCHOR_SKEW_MS,
                            self._deal_since_ms(extras))
            elif other.state in _ENTRY_ROW_STATES and extras.get('order_id') is not None:
                start = min(start, self._deal_since_ms(extras))
        self._reread_finished_walk(-position_id)
        history = await self._position_history(position_id, start)
        if not history.conclusive:
            return None
        deals = {deal.deal_id: deal for deal in bridge.position_deals}
        deals.update((deal.deal_id, deal) for deal in history.position_deals)
        return replace(
            bridge,
            closing_deal_ids=tuple(dict.fromkeys(
                (*bridge.closing_deal_ids, *history.closing_deal_ids))),
            position_deals=tuple(sorted(
                deals.values(), key=lambda deal: (deal.executed_ms, deal.deal_id))))

    def _vanished_window_start(self, position_id: int, from_ms: int) -> int:
        """The deal-history window start (ms) of a vanished position's judgment.

        ``from_ms`` (the judged row's own window start) lowered to every live
        entry row's window start on the position (:meth:`_deal_since_ms`) and
        to the closes earlier passes retired, less the clock skew
        (:attr:`_closes_covered_until`). Whichever row's judgment runs first
        retires every row of the position, so it reads the deals of all of
        them, an expiry overlap included, and the split of
        :meth:`_book_expiry_overlap` sets apart what earlier passes retired.
        """
        self._load_close_evidence()
        anchors = [from_ms, *(self._deal_since_ms(row.extras or {})
                              for row in self._position_entry_rows(position_id))]
        covered = self._closes_covered_until.get(position_id)
        if covered is not None:
            anchors.append(covered - _CLOCK_SKEW_MS)
        return min(anchors)

    def _vanished_position_in_wait(self, position_id: int) -> bool:
        """Whether a vanished position is within the shrink wait of its settled rows.

        The wait runs :data:`_UNEXPLAINED_SHRINK_WAIT_S` from the earliest
        ``missing_pending_since`` stamp on the bot's settled rows of the
        position, the bound :meth:`_confirm_missing_via_deal_history` waits
        within; a settled row not stamped yet starts it now. Without a settled
        row there is no such wait.
        """
        if self.store_ctx is None:
            return False
        stamps: list[float] = []
        settled = False
        for row in self.store_ctx.iter_live_orders():
            if self._tracked_position_refs(row) != {('positions', str(position_id))}:
                continue
            settled = True
            since = (row.extras or {}).get(MISSING_PENDING_EXTRA)
            if isinstance(since, (int, float)):
                stamps.append(float(since))
        if not settled:
            return False
        return not stamps or epoch_time() < min(stamps) + _UNEXPLAINED_SHRINK_WAIT_S

    def _cover_closes(
            self, position_id: int, read_started_at: float, *, accepted: bool = False,
    ) -> None:
        """A pass retired a position's closes from the journal without their events.

        Every closing deal it covered executed before the snapshot read
        (margined by the clock skew): a push of one of them that arrives later
        must not be booked again (:attr:`_closes_covered_until`), and a
        judgment of the position's history never books it again either
        (:meth:`_book_expiry_overlap`).

        :param accepted: An operator resume accepted the closes as the venue
            shows them: no judgment reports one of them either
            (:attr:`~_CTraderBase._resume_accepted_until`).
        """
        self._load_close_evidence()
        until = int(read_started_at * 1000) - _CLOCK_SKEW_MS
        changed = False
        if until > self._closes_covered_until.get(position_id, 0):
            self._closes_covered_until[position_id] = until
            changed = True
        if accepted and until > self._resume_accepted_until.get(position_id, 0):
            self._resume_accepted_until[position_id] = until
            changed = True
        if changed:
            self._journal_close_coverage(position_id)

    def _journal_close_coverage(self, position_id: int) -> None:
        """Journal a position's close coverage once it changed.

        What a judgment of the position's closes needs from earlier passes,
        in this process or after a restart: the venue time up to which they
        retired its closes (:attr:`~_CTraderBase._closes_covered_until`), the
        venue time up to which an operator resume accepted them
        (:attr:`~_CTraderBase._resume_accepted_until`), its expiry overlap (:attr:`~_CTraderBase._expiry_overlaps`) and the
        running wait of its unexplained shortfall
        (:attr:`~_CTraderBase._unexplained_shrinks`) with the watermark the
        wait began at (:attr:`~_CTraderBase._shrink_covered_from`). The run
        keeps one record per position, keyed by it, which replaces the
        earlier one in the same write, so the journal and its read at startup
        (:meth:`~_ReconcileMixin._restore_close_coverage`) grow with the positions, not with
        the changes.
        """
        store = self.store_ctx
        if store is None:
            return
        self._load_close_evidence()
        overlap = self._expiry_overlaps.get(position_id)
        deadline = self._unexplained_shrinks.get(position_id)
        payload = {
            'position_id': position_id,
            'covered_until': self._closes_covered_until.get(position_id),
            'accepted_until': self._resume_accepted_until.get(position_id),
            'overlap': None if overlap is None else [overlap[0], overlap[1]],
            'wait': None if deadline is None else {
                'seen_since': self._shrink_seen_since.get(
                    position_id, deadline - _UNEXPLAINED_SHRINK_WAIT_S),
                'deadline': deadline,
                'covered_from': self._shrink_covered_from.get(position_id),
            },
        }
        if self._coverage_journalled.get(position_id) == payload:
            return
        self._coverage_journalled[position_id] = payload
        store.replace_keyed_events(_CLOSE_COVERAGE_EVENT, {f'position:{position_id}': payload})

    def _restore_close_coverage(self, store: 'RunContext') -> None:
        """Take over the close coverage earlier processes of the run journalled.

        The record of each position (:meth:`_journal_close_coverage`). The
        watermarks only ever rise; an expiry overlap or a shortfall wait this
        process holds already stays. A wait on a position whose shrink the
        startup baseline retired ends there: the startup shrink is judged
        instead (:meth:`_merge_startup_shrink`). A wait on a position that is
        no longer open ends on the first reconcile pass, as in the process
        that began it.

        :param store: The journal of the run.
        """
        latest: dict[int, dict] = {}
        for payload in store.keyed_events_for_run_id(_CLOSE_COVERAGE_EVENT).values():
            position_id = payload.get('position_id')
            if isinstance(position_id, int):
                latest[position_id] = payload
        for position_id, payload in latest.items():
            covered = payload.get('covered_until')
            if isinstance(covered, int) and covered > self._closes_covered_until.get(
                    position_id, 0):
                self._closes_covered_until[position_id] = covered
            accepted = payload.get('accepted_until')
            if isinstance(accepted, int) and accepted > self._resume_accepted_until.get(
                    position_id, 0):
                self._resume_accepted_until[position_id] = accepted
            overlap = payload.get('overlap')
            if (isinstance(overlap, list) and len(overlap) == 2
                    and all(isinstance(bound, int) for bound in overlap)
                    and position_id not in self._expiry_overlaps):
                self._expiry_overlaps[position_id] = (overlap[0], overlap[1])
            wait = payload.get('wait')
            if isinstance(wait, dict) and position_id not in self._unexplained_shrinks:
                seen_since = wait.get('seen_since')
                deadline = wait.get('deadline')
                covered_from = wait.get('covered_from')
                if (isinstance(seen_since, (int, float))
                        and isinstance(deadline, (int, float))):
                    self._shrink_seen_since[position_id] = float(seen_since)
                    self._unexplained_shrinks[position_id] = float(deadline)
                    if isinstance(covered_from, int):
                        self._shrink_covered_from[position_id] = covered_from
            self._coverage_journalled[position_id] = payload

    def _cover_unexplained_retirement(self, position_id: int, read_started_at: float) -> None:
        """A wait's expiry retired a shortfall no deal explained: cover its closes too.

        The shortfall retired is the snapshot's. The closes in it executed
        before the read in venue time, which is known only up to the clock
        skew: the regular coverage (:meth:`_cover_closes`) ends the skew
        BEFORE the read, and the span from there to the skew AFTER the read
        is kept as the position's expiry overlap
        (:attr:`~_CTraderBase._expiry_overlaps`). A close executed in it is
        either one the expiry retired or one the snapshot did not hold yet.
        Its push never books it (:meth:`~_EventStreamMixin._in_expiry_overlap`):
        only the deal history's judgment does, for the part the journal
        still counts above the venue, newest first against the shortfall
        that judgment finds (:meth:`_book_expiry_overlap`). A close the
        snapshot did not hold is booked under its own deal, price and entry;
        one the expiry retired is never booked twice.
        """
        self._cover_closes(position_id, read_started_at)
        start = self._closes_covered_until[position_id]
        end = int(read_started_at * 1000) + _CLOCK_SKEW_MS
        if end > start:
            self._expiry_overlaps[position_id] = (start, end)
            self._journal_close_coverage(position_id)

    def _close_expiry_overlap(self, position_id: int, read_started_at: float) -> None:
        """A settled snapshot showed the position holding the bot's share: end its overlap.

        Once the read started more than the clock skew after the overlap
        (:attr:`~_CTraderBase._expiry_overlaps`) ended, every close executed
        in it is one the venue shows, and the journal holding exactly the
        venue counts none of them: each is retired, and a later shortfall is
        never one of them (:attr:`_closes_covered_until`).
        """
        self._load_close_evidence()
        overlap = self._expiry_overlaps.get(position_id)
        if overlap is None or int(read_started_at * 1000) - _CLOCK_SKEW_MS < overlap[1]:
            return
        del self._expiry_overlaps[position_id]
        if overlap[1] > self._closes_covered_until.get(position_id, 0):
            self._closes_covered_until[position_id] = overlap[1]
        self._journal_close_coverage(position_id)

    def _book_expiry_overlap(
            self, position_id: int, booked: Sequence[tuple[_PositionDeal, int]],
            deals: Sequence[_PositionDeal], *, covered_from: int, shortfall: float,
    ) -> tuple[list[tuple[_PositionDeal, int]], int]:
        """Split a shrink verdict's closes into the parts to book and those retired already.

        A close executed before ``covered_from`` was retired by an earlier
        pass. One executed in the position's expiry overlap
        (:attr:`~_CTraderBase._expiry_overlaps`) is booked only for the part
        the shortfall leaves after every later close, newest first: the
        closes the snapshot did not hold executed after every close the
        expiry retired, on the venue's own clock. The bot's own closes sent
        after the expiry's read may explain the shortfall too, with a deal the
        history does not show yet (:meth:`_post_expiry_close_pending`): their
        volume is left to them, so an overlap close the expiry retired is
        never booked in place of one still missing. Within a shrink's wait the
        judgment waits for them instead (:meth:`_overlap_awaits_own_close`),
        so this reservation only decides a verdict the wait could not settle.
        A close executed after the overlap is booked whole, and so is one in
        it whose order is an own close sent after the expiry's read: it never
        executed before that read, so the expiry cannot have retired it.

        :param booked: The closes of the bot's share the verdict stands for,
            with the volume (cents) each took.
        :param deals: Every deal of the history the verdict was judged from.
        :param covered_from: Venue time (ms) up to which earlier passes had
            retired the position's closes.
        :param shortfall: Units the journal share counts above the venue now.
        :return: The ``(deal, cents)`` parts to book, in execution order, and
            the cents of the verdict retired already.
        """
        start, end = self._expiry_overlaps.get(position_id, (0, 0))
        start = max(start, covered_from)
        own_after = {record.order_id for record in self._post_expiry_closes(position_id, end)
                     if record.order_id is not None}
        retired = 0
        later: list[tuple[_PositionDeal, int]] = []
        inside: list[tuple[_PositionDeal, int]] = []
        for deal, cents in booked:
            if deal.executed_ms < covered_from:
                retired += cents
            elif start <= deal.executed_ms < end and deal.order_id not in own_after:
                inside.append((deal, cents))
            else:
                later.append((deal, cents))
        budget = raw_volume(shortfall) - sum(cents for _deal, cents in later)
        if inside:
            budget -= raw_volume(self._post_expiry_close_pending(position_id, end, deals))
        kept: list[tuple[_PositionDeal, int]] = []
        for deal, cents in sorted(inside, key=lambda part: part[0].executed_ms, reverse=True):
            part = max(0, min(cents, budget))
            budget -= part
            retired += cents - part
            if part > 0:
                kept.append((deal, part))
        parts = kept + later
        parts.sort(key=lambda part: part[0].executed_ms)
        return parts, retired

    def _post_expiry_closes(self, position_id: int, end: int) -> list[_OwnClose]:
        """The bot's own closes of the position sent after an expiry's read.

        The read is ``end`` (ms) less the clock skew, both on this process's
        clock: a close sent after it never executed before it, so the expiry
        never retired any of its volume.

        :param end: The end (ms) of the position's expiry overlap.
        """
        return [record for record in self._own_close_history
                if record.position_id == position_id and record.sent_ms > end - _CLOCK_SKEW_MS]

    def _post_expiry_close_pending(
            self, position_id: int, end: int, deals: Sequence[_PositionDeal], *,
            still_executing: bool = False,
    ) -> float:
        """Units of the bot's own closes sent after an expiry the history does not show yet.

        A close sent after the expiry's read (``end`` less the clock skew,
        both on this process's clock) is never one the expiry retired. What
        it executed, or may still have executed, beyond its deals in
        ``deals`` (matched by its order id) may stand in the shortfall with a
        deal the history does not show yet: a close confirmed filled (by its
        answer or its push, in this process or in an earlier one that
        journalled it) counts what it executed; one in flight, or answered and
        still able to execute within its grace, counts the volume it asked
        for.

        :param end: The end (ms) of the position's expiry overlap.
        :param deals: Every deal of the position's history read now.
        :param still_executing: Count only the closes that may still execute.
        """
        now = monotonic()
        pending = 0.0
        for record in self._post_expiry_closes(position_id, end):
            done = record.complete or record.expires_at <= now
            if done and still_executing:
                continue
            if done:
                expected = record.executed + record.unfolded_claims()
            elif record.volume is None:
                continue
            else:
                expected = record.volume
            shown = 0.0 if record.order_id is None else volume_to_units(sum(
                deal.closed for deal in deals if deal.order_id == record.order_id))
            pending += max(0.0, expected - shown)
        return pending

    def _overlap_awaits_own_close(
            self, position_id: int, deals: Sequence[_PositionDeal], *,
            still_executing: bool = False, vanished: bool = False,
    ) -> bool:
        """Whether judging the position's history now would have to guess an own close.

        True while an unjudged closing deal executed in the position's expiry
        overlap, of an order no own close sent after the expiry's read made
        (that one is booked whole), and an own close sent after the expiry may have executed
        without its deal in ``deals`` (:meth:`_post_expiry_close_pending`):
        the overlap deal is one the expiry retired or one the snapshot did
        not hold, and only the close's deal, or its end without one, tells
        how much of the shortfall is left to the overlap. A later pass judges
        the history with every deal still unjudged.

        A position that vanished is closed in full, so every unit an own close
        sent after the expiry executed, or may execute, is part of its close:
        with ``vanished`` set, such a close missing from ``deals`` is waited
        for even while no overlap deal shows yet, as the rows' judgment would
        otherwise retire them without it.

        :param still_executing: Wait only for a close that may still execute
            (in flight, or answered and within its grace); a close confirmed
            filled whose deal the history lacks is then left to the
            reservation of :meth:`_book_expiry_overlap`.
        :param vanished: The position no longer shows on the venue.
        """
        overlap = self._expiry_overlaps.get(position_id)
        if overlap is None:
            return False
        start, end = overlap
        own_after = {record.order_id for record in self._post_expiry_closes(position_id, end)
                     if record.order_id is not None}
        return ((vanished or any(
                    deal.closed > 0 and deal.deal_id not in self._judged_close_deals
                    and start <= deal.executed_ms < end and deal.order_id not in own_after
                    for deal in deals))
                and self._post_expiry_close_pending(
                    position_id, end, deals, still_executing=still_executing) > _VOLUME_TOL)

    def _await_shrink_explanation(
            self, position_id: int, volume: float, deadline: float, *, covered_from: int,
    ) -> None:
        """Keep a confirmed shortfall no deal explains in the journal share until ``deadline``.

        Logged once per wait. Until then a closing push of the position no pass
        judged, executed after the closes earlier passes retired, is not a late
        copy of a covered close (:attr:`~_CTraderBase._unexplained_shrinks`).

        :param position_id: The open position below the bot's share.
        :param volume: The shortfall (units) no judged deal explains.
        :param deadline: Wall-clock time the wait ends.
        :param covered_from: Venue time (ms) up to which earlier passes had
            retired the position's closes when the shortfall was first judged
            (:attr:`~_CTraderBase._shrink_covered_from`).
        """
        self._load_close_evidence()
        if position_id not in self._unexplained_shrinks:
            logger.warning(
                "cTrader: bot position %d holds %s units less than the bot's share and its "
                "deal history shows no close that took them; waiting up to %.0fs for the "
                "close's fill", position_id, volume, deadline - epoch_time())
            self._shrink_covered_from[position_id] = covered_from
        self._unexplained_shrinks[position_id] = deadline
        self._journal_close_coverage(position_id)

    def _forget_shrink_wait(self, position_id: int) -> None:
        """A position no longer holds less than the bot's share: its shrink wait ends."""
        self._load_close_evidence()
        self._shrink_seen_since.pop(position_id, None)
        self._unexplained_shrinks.pop(position_id, None)
        self._shrink_covered_from.pop(position_id, None)
        self._journal_close_coverage(position_id)

    def _note_unexplained_shrink(self, position_id: int, volume: float) -> None:
        """A confirmed shortfall no closing deal explained in time: ERROR + audit.

        The caller retires it from the journal unbound; the engine's position
        check books the venue change the same way.
        """
        logger.error(
            "cTrader: bot position %d holds %s units less than the bot's share and no deal "
            "explains it after %.0fs; retired from the journal without a fill", position_id,
            volume, _UNEXPLAINED_SHRINK_WAIT_S)
        if self.store_ctx is not None:
            rows = self._position_entry_rows(position_id)
            self.store_ctx.log_event(
                _SHRINK_UNEXPLAINED_EVENT,
                client_order_id=rows[0].client_order_id if rows else None,
                exchange_order_id=str(position_id),
                payload={'position_id': position_id, 'volume': volume})

    async def _judge_shrink(
            self, position_id: int, position: 'OpenApiModelMessages.ProtoOAPosition', *,
            venue: float, share_now: float, read_started_at: float,
            settle: Callable[[_ShareReplay, Sequence[_PositionDeal]], None],
            hold_for_own_close: bool = False, still_executing: bool = False,
    ) -> bool:
        """Judge the deal history of one position open below the bot's share.

        A wait past :data:`_UNEXPLAINED_SHRINK_WAIT_S` (``still_executing``)
        can outlast the engine's hold on its verdict, which then books the
        shortfall as an external reduction. The close's fill delivered after
        that is still booked once: the engine holds a fill of an order it
        counted live at the adoption (a dispatched close's intent maps its
        order) against the adopted reduction and lets the venue settle it,
        re-pricing the reduction's synthetic close instead of reducing the
        book again (``OrderSyncEngine._hold_fill_against_adopted_reduction``).

        :param settle: The caller's writes applying the verdict on the deals
            judged now, given the verdict and every deal of the history read.
            They run in the transaction that records the deals judged
            (:meth:`~_ProtectionMixin._judge_recovered_close_deals`).
        :param hold_for_own_close: Leave the history unjudged while an own
            close sent after the position's expiry may have executed without
            its deal in the history (:meth:`_overlap_awaits_own_close`).
        :param still_executing: With ``hold_for_own_close``, wait only for a
            close that may still execute (in flight, or answered and within
            its grace), as past the shrink wait.
        :return: Whether the history was judged and ``settle`` applied;
            ``False`` when the history could not be read or judged yet, or a
            deal in it may postdate the snapshot: the caller retries on a
            later pass.
        """
        history = await self._position_history(
            position_id, self._position_window_start(position_id, position))
        if not history.conclusive:
            return False
        cutoff = int(read_started_at * 1000) - _CLOCK_SKEW_MS
        if any(deal.executed_ms >= cutoff for deal in history.position_deals):
            return False
        if hold_for_own_close and self._overlap_awaits_own_close(
                position_id, history.position_deals, still_executing=still_executing):
            # The close's deal may show later with an execution time this
            # read already passed: the next pass reads the window again.
            self._deal_walks.pop(-position_id, None)
            return False
        rows = self._position_entry_rows(position_id)
        deals = history.position_deals
        replay = await self._judge_recovered_close_deals(
            position_id, deals, from_entry=rows[0].pine_entry_id if rows else None,
            venue_now=venue, share_now=share_now,
            settle=lambda judged: settle(judged, deals))
        return replay is not None

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
        await self._judge_recovered_closes(
            position_id, history.position_deals, from_entry=pending.from_entry,
            venue_now=venue, share_now=(self._own_share(position_id) or 0.0) + pending.volume,
            own_orders=pending.order_ids,
            settle=lambda _taken: self._end_judged_startup_shrink(position_id, read_started_at))

    def _end_judged_startup_shrink(self, position_id: int, read_started_at: float) -> None:
        """End a startup shrink whose deals are judged, covering its closes up to the read."""
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
        the resume marked, is accepted with a WARNING and an audit event, and
        the closes it retired are covered up to the read that found it
        (:meth:`_cover_closes`), so no judgment books them later.
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
                # Accepted without its closes judged: every one the baseline
                # that found it retired executed before that process's read.
                self._cover_closes(position_id, found_ms / 1000.0, accepted=True)
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
        """Add a pending startup shrink to the one already pending on its position.

        A shortfall wait an earlier process journalled on the position ends:
        the startup baseline retired the shortfall, and the startup shrink
        judges its deals.
        """
        pending = self._startup_shrinks.get(position_id)
        if pending is None:
            self._startup_shrinks[position_id] = shrink
        else:
            pending.volume += shrink.volume
            pending.since_ms = min(pending.since_ms, shrink.since_ms)
            pending.from_entry = pending.from_entry or shrink.from_entry
            pending.order_ids.extend(
                oid for oid in shrink.order_ids if oid not in pending.order_ids)
            pending.keys.extend(shrink.keys)
        self._forget_shrink_wait(position_id)

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
        """Grace-expiry re-verification of a vanished row (:meth:`_missing_verdict`).

        A cancel verdict stands for the order having executed nothing beyond
        the row's fill cursor: the complete reads behind it (the order's deal
        history, and its own details where they were asked) are the venue's
        word on that. It passes the one verdict on the order's execution
        (:meth:`~_CTraderBase._execution_evidence`) first: an executed volume
        the venue reported earlier beyond the cursor turns it into FILLED (the
        fill waits for the deal history), never a zero-fill cancel.

        :param row: The vanished row.
        """
        order_id_raw = (row.extras or {}).get('order_id')
        if self._execution_handed_over(order_id_raw):
            # The engine's held-execution recovery owns the order: its absence
            # is no disappearance (the next pass clears the stamp).
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        confirmation = await self._missing_verdict(row)
        if (confirmation.resolution is MissingResolution.CANCELLED
                and self._execution_evidence(row, reported=row.filled_qty).executed
                > row.filled_qty + _VOLUME_TOL):
            confirmation = MissingConfirmation(MissingResolution.FILLED)
        if (confirmation.resolution is not MissingResolution.INCONCLUSIVE
                and order_id_raw is not None):
            self._release_vanished_cause(
                parse_protocol_id(order_id_raw, field='order_id'),
                f"the venue's evidence decided it ({confirmation.resolution.value})")
        return confirmation

    async def _missing_verdict(
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
          (:meth:`_judge_recovered_close_deals`); while a closing order cannot
          be looked up the verdict stays INCONCLUSIVE. The closes of the
          bot's share the judged deals stand for — the position's own SL / TP
          that fired, the bot's own close, or someone else's — travel with
          the verdict as close fills of the row's entry
          (:meth:`_recovered_close_event`), the fills the PUSH stream would
          have delivered, so the engine books the exit instead of reading the
          flat venue as an unexplained flatten. A row whose disappearance an
          operator resume accepted has its close accepted instead, without
          close fills.
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
        order_id: int | None = None
        if order_id_raw is not None:
            order_id = parse_protocol_id(order_id_raw, field='order_id')
        elif position_id is not None:
            # A row startup adopted from an open position knows no entry order:
            # its position's history, walked under the negated position id like
            # a shrink's, holds the closes.
            order_id = -position_id
        if order_id is not None:
            from_ms = self._deal_since_ms(extras)
            if position_id is not None and self._own_share(position_id) is not None:
                # Whichever row of the position is judged first retires the
                # others: its window spans theirs and any expiry overlap.
                from_ms = self._vanished_window_start(position_id, from_ms)
            self._reread_finished_walk(order_id)
            bridge = await self._find_fill_deal(
                order_id, from_ms, close_position_id=position_id,
                deadline=self._reconcile_pass_deadline,
            )
            if not bridge.conclusive:
                return MissingConfirmation(MissingResolution.INCONCLUSIVE)
            held_pid = (bridge.deal.positionId
                        if bridge.deal is not None and bridge.deal.positionId else position_id)
            if (held_pid is not None and not _accepted_by_resume(row)
                    and self._own_share(held_pid) is not None):
                since = extras.get(MISSING_PENDING_EXTRA)
                # Within the shrink wait from the first pass that missed the
                # position, a confirmed own close whose deal the history lacks
                # is waited for, whether or not the history shows any close
                # yet; past it, only one that may still execute is, bounded by
                # its grace.
                in_wait = (isinstance(since, (int, float))
                           and epoch_time() < since + _UNEXPLAINED_SHRINK_WAIT_S)
                if self._overlap_awaits_own_close(
                        held_pid, bridge.position_deals, still_executing=not in_wait,
                        vanished=True):
                    # An own close sent after the expiry may still execute or
                    # report its fill: judged once it can no longer. Its deal
                    # may show with an execution time this read already passed,
                    # so the next check reads the window again.
                    self._deal_walks.pop(order_id, None)
                    return MissingConfirmation(MissingResolution.INCONCLUSIVE)
            if bridge.closed_cents > 0 and (bridge.filled_cents > 0
                                            or position_id is not None):
                close_pid = (
                    bridge.deal.positionId if bridge.deal is not None else position_id
                )
                assert close_pid is not None
                close_events: tuple[OrderEvent, ...] = ()
                if _accepted_by_resume(row):
                    self._note_accepted_by_resume(
                        close_pid, client_order_id=row.client_order_id, volume=None,
                        deal_ids=bridge.closing_deal_ids)
                    self._mark_close_judged(list(bridge.closing_deal_ids))
                else:
                    closes: list[tuple[_PositionDeal, int, str | None]] = []
                    replay = await self._judge_recovered_close_deals(
                        close_pid, bridge.position_deals, from_entry=row.pine_entry_id,
                        counted=bridge.deal_ids,
                        settle=partial(self._settle_vanished_close, row, close_pid, closes,
                                       self._own_share(close_pid), bridge.position_deals,
                                       self._undelivered_vanished_closes(
                                           close_pid, row.client_order_id)))
                    if replay is None:
                        return MissingConfirmation(MissingResolution.INCONCLUSIVE)
                    close_events = tuple(
                        self._recovered_close_event(row, deal, cents, from_entry=label)
                        for deal, cents, label in closes
                        if deal.deal_id not in self._seen_deal_ids)
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
                    close_events=close_events,
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
        it. An order that executed (filled, executed part of its volume, or
        listed with a deal that executed: the answer carries every deal its
        fills created) was not cancelled: the deal history does not list its
        fill yet, and the snapshot bridge promotes it once it does. An order the venue still
        reports as accepted is still working. An expired or rejected order
        was ended by the venue, so the cancel carries the venue's reason
        (:meth:`_venue_cancel_reason`) and the tracker applies no policy,
        unless it expired at an expiry the bot never set: someone else's edit,
        the unexpected cancel the policy handles. A cancelled order was ended
        by someone the evidence cannot name (:meth:`_unattributed_cancel_reason`).
        A lookup that failed for now leaves the verdict to a later pass.

        A row whose journalled ``executed_cents`` floor exceeds its fill
        cursor was reported executed by the venue earlier, so it is FILLED:
        its fill waits for the deal history. An order the venue reports as not
        found carries no status, executed volume or deal list, so its own
        details prove nothing; the deal history decides
        (:meth:`_not_found_vanished_verdict`).

        :param row: The vanished order's journal row.
        :param order_id: The order's id.
        """
        if self._execution_evidence(row, reported=None).executed > row.filled_qty + _VOLUME_TOL:
            return MissingConfirmation(MissingResolution.FILLED)
        if self._live_account_id is None:
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        try:
            async with asyncio.timeout_at(self._reconcile_pass_deadline):
                res = cast(OpenApiMessages.ProtoOAOrderDetailsRes, await self._account_request(
                    OpenApiMessages.ProtoOAOrderDetailsReq(
                        ctidTraderAccountId=self._live_account_id, orderId=order_id)))
        except CTraderProtocolError as exc:
            if is_not_found(exc.error_code):
                if _accepted_by_resume(row):
                    return MissingConfirmation(
                        MissingResolution.CANCELLED,
                        cancel_reason=self._unattributed_cancel_reason(
                            row, order_id, status=exc.error_code))
                return self._not_found_vanished_verdict(row, order_id, exc.error_code)
            logger.debug("cTrader vanished-order lookup for %d deferred: %s", order_id, exc)
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        except (BrokerError, CTraderWireError, TimeoutError) as exc:
            logger.debug("cTrader vanished-order lookup for %d deferred: %s", order_id, exc)
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        order = res.order
        statuses = OpenApiModelMessages.ProtoOAOrderStatus
        status = order.orderStatus
        if (status == statuses.ORDER_STATUS_FILLED or order.executedVolume > 0
                or any(deal.filledVolume > 0 and deal.dealStatus in _EXECUTED_DEAL_STATUSES
                       for deal in res.deal)):
            # Reached after a complete read of the order's deal history listed
            # no fill of it (:meth:`_missing_verdict`): the history cannot
            # deliver what the order's details show it executed.
            self._hand_over_execution(row, order_id, 'deal history lists no fill of the order')
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

    def _not_found_vanished_verdict(
            self, row: 'OrderRow', order_id: int, error_code: str,
    ) -> MissingConfirmation:
        """The verdict on a vanished order the venue reports as not found, from its deal history.

        Reached only after a complete read of the order's deal history from
        its since-anchor to now listed no execution of it
        (:meth:`_missing_verdict`), and with no executed volume the venue
        reported for it earlier (:meth:`_vanished_order_verdict`). A deal the
        venue has not published yet may still show, and how late it publishes
        one is not measured, so the history proves the order executed nothing
        only once :data:`~pynecore_ctrader._base._UNKNOWN_DISPATCH_TTL_S` (the
        bound of the venue's indexing lag the plugin already gives a lost
        dispatch) passed since the order was first missed. Until then the row
        stays unresolved and the bot is quarantined under the cause
        ``vanished_order:<order id>`` (:meth:`_quarantine_undecidable_vanished`);
        then the order is retired as an unattributed cancel
        (:meth:`_unattributed_cancel_reason`) and the cause is released
        (:meth:`_release_vanished_cause`). A later pass whose history lists a
        fill of the order ends the cause as well: the bot gets out of this by
        itself, no operator step.

        :param row: The vanished order's journal row.
        :param order_id: The order's id.
        :param error_code: The lookup's not-found error code.
        """
        since = (row.extras or {}).get(MISSING_PENDING_EXTRA)
        missed_at = float(since) if isinstance(since, (int, float)) else row.created_ts_ms / 1000.0
        if epoch_time() < missed_at + _UNKNOWN_DISPATCH_TTL_S:
            if order_id not in self._vanished_not_found_warned:
                self._vanished_not_found_warned.add(order_id)
                self._quarantine_undecidable_vanished(row, order_id, error_code)
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        confirmation = MissingConfirmation(
            MissingResolution.CANCELLED,
            cancel_reason=self._unattributed_cancel_reason(row, order_id, status=error_code))
        self._release_vanished_cause(
            order_id, f"its deal history lists no execution {_UNKNOWN_DISPATCH_TTL_S:.0f}s "
                      f"after the order left the venue: it executed nothing")
        return confirmation

    def _quarantine_undecidable_vanished(
            self, row: 'OrderRow', order_id: int, error_code: str,
    ) -> None:
        """Quarantine the bot on a vanished order whose execution nothing decides yet.

        The venue reports the order as not found, so neither its status, its
        executed volume nor its deals are known, and a deal history listing no
        fill may only lag. The row stays unresolved, logged as an ERROR, and
        the engine's entry-block quarantine (:attr:`quarantine_sink`) is
        latched under the cause ``vanished_order:<order id>``: the bot stays
        alive but opens nothing new until the deal history decides
        (:meth:`_not_found_vanished_verdict`), which releases the cause.

        :param row: The vanished order's journal row.
        :param order_id: The order's id.
        :param error_code: The lookup's not-found error code.
        """
        logger.error(
            "cTrader: bot order %d (%s) left the venue while the order stream missed it, "
            "and the venue reports it as not found (%s): nothing proves yet whether it "
            "executed; the row stays unresolved and new entries are blocked until its "
            "deal history decides", order_id, row.client_order_id, error_code)
        if self.quarantine_sink is None:
            return
        # noinspection PyBroadException
        try:
            self.quarantine_sink(
                'vanished_order_not_found',
                {'order_id': order_id, 'client_order_id': row.client_order_id,
                 'error_code': error_code, 'pine_entry_id': row.pine_entry_id},
                kind=QuarantineKind.ENTRY_BLOCK, cause=_vanished_cause(order_id))
        except Exception:
            logger.exception(
                "cTrader: quarantine request for vanished order %d failed", order_id)

    def _release_vanished_cause(self, order_id: int, resolution: str) -> None:
        """End the ``vanished_order:<order id>`` quarantine cause of a decided order.

        A no-op for an order this process did not quarantine. The order stays
        listed when the release sink raised, so the next pass releases it.

        :param order_id: The vanished order's id.
        :param resolution: What decided it, logged by the engine.
        """
        if order_id not in self._vanished_not_found_warned:
            return
        sink = self.quarantine_release_sink
        if sink is not None:
            # noinspection PyBroadException
            try:
                sink(_vanished_cause(order_id), resolution)
            except Exception:
                logger.exception(
                    "cTrader: quarantine release for vanished order %d failed", order_id)
                return
        self._vanished_not_found_warned.discard(order_id)

    def _release_settled_vanished_causes(
            self, rows: 'Sequence[tuple[OrderRow, int | None]]',
    ) -> None:
        """End the vanished-order causes whose rows another path decided.

        A row the stream, the startup recovery or any other pass retired, or
        whose order the snapshot shows again (its missing stamp is gone), no
        longer waits for its deal history.

        :param rows: The live rows of this pass with their order ids.
        """
        if not self._vanished_not_found_warned:
            return
        missing = {order_id for row, order_id in rows
                   if order_id is not None and MISSING_PENDING_EXTRA in (row.extras or {})}
        for order_id in sorted(self._vanished_not_found_warned - missing):
            self._release_vanished_cause(
                order_id, "its journal row was decided by another path")

    def _closed_position_siblings(
            self, row: 'OrderRow', confirmation: MissingConfirmation,
    ) -> list[str]:
        """Live rows sharing the CLOSED verdict's position id.

        A NETTING / pyramiding account merges pyramid entries onto one
        ``positionId``, so a proven close retires all of them (mirroring
        the PUSH path's ``_mark_position_closed``) — closing only the
        observed row would leave sibling rows live or later mis-stamped
        against a flat broker.

        The tracker asks inside the transaction that retires the row and,
        with the siblings named here, the position's whole exposure: from
        then on the position a restart starts from holds the verdict's close
        fills. Their delivery records
        (:meth:`~_ProtectionMixin._journal_fill_delivery`) are written here, in
        that transaction, and the judgment's journal of the fills
        (:data:`_VANISHED_CLOSE_EVENT`) is dropped: its rows are retired, so no
        judgment delivers them again, and a fill the engine did not take is
        delivered again from its record.
        """
        self._journal_fill_delivery(confirmation.close_events)
        pid_ref = confirmation.position_ref
        if not pid_ref:
            return []
        if self.store_ctx is not None:
            self.store_ctx.replace_keyed_events(
                _VANISHED_CLOSE_EVENT, {}, [f'position:{pid_ref}'])
        return [coid for coid in self._position_live_coids(pid_ref)
                if coid != row.client_order_id]

    def _position_live_coids(self, position_ref: str) -> list[str]:
        """Client order ids of the live journal rows on the position ``position_ref``."""
        if self.store_ctx is None:
            return []
        return [
            r.client_order_id
            for r in self.store_ctx.iter_live_orders()
            if (str((r.extras or {}).get('position_id') or '') == position_ref
                or r.exchange_order_id == position_ref)
        ]

    def _settle_vanished_close(
            self, row: 'OrderRow', position_id: int,
            closes: list[tuple[_PositionDeal, int, str | None]], share: float | None,
            history: Sequence[_PositionDeal],
            undelivered: Sequence[tuple[_PositionDeal, int, str | None]],
            replay: _ShareReplay,
    ) -> None:
        """Apply the judgment of a vanished row's position: the close fills it delivers.

        Runs in the transaction that records the deals judged
        (:meth:`~_ProtectionMixin._judge_recovered_close_deals`). Closes
        earlier passes retired without their events, and the part of an
        expiry overlap's closes the expiry retired, are judged but not
        delivered again: the same split a shrink judgment applies. The fills
        are journalled with the position's live rows
        (:data:`_VANISHED_CLOSE_EVENT`): the CLOSED verdict that delivers them
        is applied later, by the disappearance tracker, so a verdict no
        tracker applied (the process stopped first, or the tracker dropped it
        as stale) leaves them to the next judgment of one of those rows.

        :param row: The vanished row judged.
        :param closes: Receives ``(deal, cents, label)`` of every close fill
            the verdict delivers, those of an earlier undelivered judgment
            first.
        :param share: The bot's share (units) of the position before the
            judgment; ``None`` when not known.
        :param history: Every deal of the position's history read.
        :param undelivered: The close fills an earlier judgment of one of the
            row's position siblings journalled and no verdict delivered
            (:meth:`_undelivered_vanished_closes`).
        :param replay: The verdict on the deals judged now.
        """
        parts = replay.booked if share is None else self._book_expiry_overlap(
            position_id, replay.booked, history,
            covered_from=self._closes_covered_until.get(position_id, 0),
            shortfall=share)[0]
        known = {deal.deal_id for deal, _cents, _label in undelivered}
        fresh = [(deal, cents, self._vanished_close_label(position_id, deal, row.pine_entry_id))
                 for deal, cents in parts if deal.deal_id not in known]
        closes.extend(undelivered)
        closes.extend(fresh)
        if not fresh or self.store_ctx is None:
            return
        self.store_ctx.replace_keyed_events(_VANISHED_CLOSE_EVENT, {
            f'position:{position_id}': {
                'position_id': position_id,
                'coids': sorted({row.client_order_id,
                                 *self._position_live_coids(str(position_id))}),
                'closes': [
                    {'deal_id': deal.deal_id, 'order_id': deal.order_id,
                     'executed_ms': deal.executed_ms, 'filled': deal.filled,
                     'closed': deal.closed, 'side': deal.side, 'price': deal.price,
                     'fee': deal.fee, 'cents': cents, 'label': label}
                    for deal, cents, label in closes],
            }})

    def _undelivered_vanished_closes(
            self, position_id: int, coid: str,
    ) -> list[tuple[_PositionDeal, int, str | None]]:
        """The close fills a vanished-row judgment journalled for ``coid`` and never delivered.

        The judgment names the position's live rows (:meth:`_settle_vanished_close`).
        The CLOSED verdict that delivers its fills retires every one of them,
        so a row of that list still live and judged again was never given
        them: the earlier verdict was not applied.

        :return: ``(deal, cents, label)`` of each such fill; empty when the
            journal holds none for the row.
        """
        if self.store_ctx is None:
            return []
        payload = self.store_ctx.keyed_events_for_run_id(_VANISHED_CLOSE_EVENT).get(
            f'position:{position_id}')
        if not payload or coid not in (payload.get('coids') or ()):
            return []
        closes: list[tuple[_PositionDeal, int, str | None]] = []
        for entry in payload.get('closes') or ():
            try:
                label = entry.get('label')
                closes.append((_PositionDeal(
                    deal_id=int(entry['deal_id']), order_id=int(entry['order_id']),
                    executed_ms=int(entry['executed_ms']), filled=int(entry['filled']),
                    closed=int(entry['closed']), side=str(entry['side']),
                    price=float(entry['price']), fee=float(entry['fee']),
                ), int(entry['cents']), label if isinstance(label, str) else None))
            except (KeyError, TypeError, ValueError, AttributeError):
                continue
        return closes

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
    ) -> list[OrderEvent]:
        """Detect partial-fill progress on a still-pending working order.

        ``ProtoOAOrder`` carries ``positionId`` once any volume has filled, so a
        partial fill is deterministic from ``order[]`` alone: link the
        ``positionId`` (set-once) and, when the cumulative ``executedVolume`` has
        grown past the row's recorded ``filled_qty``, emit the partial fill.
        A working order that is back after a stale-missing stamp clears it.

        The snapshot carries no per-execution terms (``executionPrice`` is the
        order's running average) and no commission, so the fill is taken from
        the order's deal history: one event per new execution, with its deal id,
        price and commission (:meth:`_recovered_entry_fills`), whose deals are
        then marked seen; their delivery records land with the cursor write.
        When the history cannot cover the progress (read failed, or the deals
        are not listed yet) the fill is still booked on time from the snapshot,
        as one event without commission under a fill id of its own
        (:func:`_snapshot_fill_id`), and the row journals the range with that
        id in ``fee_unbooked``, so the commission a later push or recovery of
        this order finds for it reaches the trade this booking opened. A
        snapshot that carries no ``executionPrice`` either books nothing: the
        cursor stays, and the progress is booked once a pass or the push
        prices it.
        """
        if self.store_ctx is None:
            return []
        extras = row.extras or {}
        if 'missing_pending_since' in extras:
            self._clear_missing_pending(row)
            extras = row.extras or {}
        filled = volume_to_units(order.executedVolume)
        if filled <= row.filled_qty + 1e-9 or filled >= row.qty:
            return []
        fill_qty = filled - row.filled_qty
        # Read the history before the cursor write, so nothing is persisted for
        # an event this pass could still fail to deliver.
        bridge = await self._find_fill_deal(
            order.orderId, self._deal_since_ms(extras),
            deadline=self._reconcile_pass_deadline)
        delta_price = (self._delta_fill_price(bridge.fills, row.filled_qty, fill_qty)
                       if bridge.conclusive else None)
        covered: set[int] = set()
        new_ids: set[int] = set()
        fees: dict[int, float] = {}
        ranged: dict[str, float] = {}
        rest_fill_id: str | None = None
        patch: dict = {}
        if delta_price is not None:
            covered = self._deals_covering(bridge.fills, filled)
            commission = self._recovered_commission(row, bridge.fills, fill_qty)
            fees, new_ids, ranged = commission.fees, commission.new_ids, commission.ranged
            patch.update(commission.patch or {})
        elif not order.executionPrice:
            # Neither the history nor the snapshot prices the progress: an
            # event without a price books nothing, so the cursor stays where
            # the delivered fills put it, and a later pass or the push books
            # the progress once one of them prices it.
            logger.debug(
                "cTrader: fill progress of order %d (%s -> %s units) has no price yet; "
                "booked once the deal history or the order prices it",
                order.orderId, row.filled_qty, filled)
            return []
        else:
            # Booked from the snapshot alone: the cursor now counts executions
            # whose commission no event carried yet, charged later on the
            # booking's own fill id.
            rest_fill_id = _snapshot_fill_id(row.client_order_id, filled)
            patch[_FEE_UNBOOKED_EXTRA] = [
                *(extras.get(_FEE_UNBOOKED_EXTRA) or ()),
                [row.filled_qty, filled, rest_fill_id]]
        position_id: int | None = order.positionId or None
        if position_id and extras.get('position_id') is None:
            patch['position_id'] = position_id
        avg = (open_positions[position_id].price
               if position_id in open_positions else order.executionPrice)
        events = self._recovered_entry_fills(
            row, bridge.fills if delta_price is not None else (), fill_qty, fees,
            ranged=ranged, event_type='partial', cumulative=filled,
            exchange_id=str(order.orderId), avg_price=avg or None,
            fallback_price=order.executionPrice or None, timestamp=now_ts,
            rest_fill_id=rest_fill_id)
        with self.store_ctx.transaction():
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
            self._journal_recovered_deliveries(events)
        self._record_reconciled_fill(
            order.orderId, position_id, row.side, fill_qty, bridge.fills, new_ids)
        self._seen_deal_ids.update(covered)
        return events

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
            self, row, order_id: int, open_positions: dict, now_ts: float, *,
            working_orders: Collection[int] = (),
    ) -> AsyncIterator[OrderEvent]:
        """Bridge a vanished working order to its outcome via the deal history.

        The order left ``order[]``; the deal history (keyed on ``orderId``,
        which the snapshot positions lack) is the deterministic source for
        whether it filled. On a FILLED deal: promote the row to a position
        (set ``position_id`` once, advance ``filled_qty``) and emit the fill the
        PUSH stream missed. An order that filled onto a position the bot holds
        a share of, which then closed, retires the bot's other rows of it too:
        its closes are judged as a vanished settled row's would be, from the
        position's history since the closes earlier passes retired, waiting
        for an own close sent after the position's expiry, and never
        delivering a close those passes or the expiry retired. On a
        conclusive no-fill (the history was fully
        paginated and carried no FILLED deal): stamp ``missing_pending_since``
        so a later milestone's grace window can retire it. On an inconclusive
        / failed history query: do nothing — the next pass or the PUSH event
        resolves it (never conclude a cancel from a truncated read).
        """
        if self.store_ctx is None:
            return
        extras = row.extras or {}
        handed_over = self._execution_handed_over(order_id)
        if handed_over and 'missing_pending_since' in extras:
            # The engine's held-execution recovery owns the order: its absence
            # is no disappearance.
            self._clear_missing_pending(row)
            extras = row.extras or {}
        bridge = await self._find_fill_deal(
            order_id, self._deal_since_ms(extras),
            deadline=self._reconcile_pass_deadline)
        if bridge.deal is not None:
            closed_pid = bridge.deal.positionId
            closes: list[tuple[_PositionDeal, int]] = []
            group: list[tuple['OrderRow', _DealBridgeResult]] = []
            if (closed_pid and closed_pid not in open_positions
                    and bridge.closed_cents > 0 and bridge.filled_cents > 0):
                share = self._own_share(closed_pid)
                # The history may lack part of the order's fills: its own
                # details are the venue's word on what it executed. A failed
                # read leaves the check to the journal's evidence.
                details = await self._order_details_response(order_id)
                if self._fill_history_short(
                        bridge, row, order_id,
                        self._details_executed_cents(details) if details is not None else 0):
                    if bridge.conclusive:
                        # The position is closed and the complete history shows
                        # less than the order executed.
                        self._hand_over_execution(
                            row, order_id, 'deal history shows less than the order executed')
                    return
                widened = await self._with_group_position_history(
                    bridge, closed_pid, self._deal_since_ms(extras))
                if widened is None:
                    return
                bridge = widened
                if share is not None and self._overlap_awaits_own_close(
                        closed_pid, bridge.position_deals,
                        still_executing=not self._vanished_position_in_wait(closed_pid),
                        vanished=True):
                    # An own close sent after the expiry may still show its
                    # deal, possibly stamped before this read: the next pass
                    # reads both windows again.
                    self._deal_walks.pop(order_id, None)
                    self._deal_walks.pop(-closed_pid, None)
                    return
                group = await self._unbooked_position_entries(
                    row, closed_pid, bridge.position_deals, working_orders=working_orders)
                if group is None:
                    return
                share_now = self._share_with_recovered_fills(closed_pid, [
                    (entry_row, volume_to_units(entry_bridge.filled_cents))
                    for entry_row, entry_bridge in ((row, bridge), *group)])
                promoted: list[OrderEvent] = []
                replay = await self._judge_recovered_close_deals(
                    closed_pid, bridge.position_deals, from_entry=row.pine_entry_id,
                    share_now=share_now, counted=(
                        *bridge.deal_ids,
                        *(deal_id for _row, other in group for deal_id in other.deal_ids)),
                    settle=partial(self._settle_filled_then_closed, row, bridge, open_positions,
                                   now_ts, closed_pid, group, share, share_now, promoted))
                if replay is None:
                    # Who closed the position is not known yet: retire it on a
                    # later pass, once the closing orders could be looked up.
                    return
                for event in promoted:
                    yield event
                return
            for event in self._promote_from_deal(
                    row, bridge, open_positions, now_ts, closes=closes, group=group):
                yield event
            return
        # Conclusive no-fill: stamp the disappearance breadcrumb only on positive
        # no-fill evidence. A row that already linked a ``position_id`` or carries
        # any fill is a live (possibly partial) position — never a cancelled
        # pending — so stamping ``missing_pending_since`` there would later raise
        # a false ``UnexpectedCancelError`` against an open position (surface c).
        if (bridge.conclusive
                and not handed_over
                and not extras.get('position_id')
                and row.filled_qty <= 1e-9
                and 'missing_pending_since' not in extras):
            patched = dict(extras)
            patched['missing_pending_since'] = now_ts
            self.store_ctx.upsert_order(row.client_order_id, extras=patched)

    def _settle_filled_then_closed(
            self, row, bridge: '_DealBridgeResult', open_positions: dict, now_ts: float,
            closed_pid: int, group: Sequence[tuple['OrderRow', '_DealBridgeResult']],
            share: float | None, share_now: float, promoted: list[OrderEvent],
            replay: _ShareReplay,
    ) -> None:
        """Retire a working order that filled and closed, in the transaction of its closes' judgment.

        The judgment records the closing deals as judged; the retirement, the
        delivery records of the trade and the deals taken as seen land with it
        (:meth:`_promote_from_deal`), so a retirement that fails leaves the
        deals unjudged and the next pass judges and delivers them again.
        Closes earlier passes retired without their events, and the part of
        an expiry overlap's closes the expiry retired, are judged but not
        delivered again: the split a vanished row's judgment applies.

        :param row: The working order's journal row.
        :param bridge: The order's deal evidence, widened to the position's history.
        :param open_positions: The snapshot's open positions.
        :param now_ts: The observation time.
        :param closed_pid: The closed position.
        :param group: The other working rows whose orders filled onto the same
            position, with their deal evidence.
        :param share: The journal share of the position before the judgment.
        :param share_now: The share the judgment starts from.
        :param promoted: Receives the events to deliver.
        :param replay: The judgment's verdict.
        """
        closes = replay.booked if share is None else self._book_expiry_overlap(
            closed_pid, replay.booked, bridge.position_deals,
            covered_from=self._closes_covered_until.get(closed_pid, 0),
            shortfall=share_now)[0]
        promoted.extend(self._promote_from_deal(
            row, bridge, open_positions, now_ts, closes=closes, group=group))

    def _promote_from_deal(
            self, row, bridge: '_DealBridgeResult', open_positions: dict, now_ts: float,
            *, closes: Sequence[tuple[_PositionDeal, int]] = (),
            group: Sequence[tuple['OrderRow', '_DealBridgeResult']] = (),
    ) -> list[OrderEvent]:
        """Promote a working row to a filled position from its FILLED deals.

        Closure-aware (closedVolume-primary): when the order's ``positionId`` is
        gone from the open snapshot and the deal history closed it, the order
        filled-then-closed while the stream was down — the row is retired rather
        than promoted, so no open position is left behind (surface d), and the
        trade the stream missed is delivered whole: the entry's recovered fill,
        then the closes of the bot's share (``closes``, judged by
        :meth:`_judge_recovered_close_deals`), each as the PUSH stream would
        have delivered it. When the position is still open the order's own
        cumulative fill is recovered and the missed fill emitted.

        Idempotent against the PUSH path: every recovered ``dealId`` already in
        the shared :attr:`_seen_deal_ids` set was applied live, so the cursor /
        ``position_id`` link are reconciled but nothing is emitted for it.

        :param closes: ``(deal, cents)`` of the closing deals that took part of
            the bot's share, with that part, in execution order.
        :param group: The other working rows whose orders filled onto the same
            closed position (:meth:`_unbooked_position_entries`), with their
            deal evidence: their entry fills are delivered before the closes
            and their rows retired with this one.
        :return: The events to deliver, in execution order.
        """
        deal = bridge.deal
        if self.store_ctx is None or deal is None:
            return []
        position_id: int | None = deal.positionId or None
        position_open = bool(position_id) and position_id in open_positions
        # Closure classification. A vanished order whose ``positionId`` is no
        # longer open filled then closed during the stream gap. With positive
        # closure evidence (a deal on the position carried
        # ``closePositionDetail.closedVolume``) retire the row through the
        # existing terminal-close writer instead of promoting an open
        # position, and deliver the entry fill and its closes so the engine
        # books the whole trade and ends flat like the venue. On ambiguity
        # (position gone but no closing deal observed in the window) do
        # nothing — never fabricate a retirement from missing evidence; the
        # next pass or PUSH resolves it.
        if position_id and not position_open:
            if bridge.closed_cents > 0 and bridge.filled_cents > 0:
                events = self._filled_then_closed_events(
                    row, bridge, closes, now_ts, group=group)
                # The retirement puts the closes into the position a restart
                # starts from: their delivery records land with it.
                with self.store_ctx.transaction():
                    self._retire_filled_then_closed(
                        row, deal, position_id, bridge.closed_cents)
                    for other, other_bridge in group:
                        self._retire_filled_then_closed(
                            other, other_bridge.deal, position_id, bridge.closed_cents)
                    self._journal_recovered_deliveries(events)
                # Record the recovered deals on the shared de-dup channel once the
                # retirement committed: cTrader can replay or push an uncorrelated
                # copy of the same execution after reconnect, and a sibling row
                # sharing this position is retired off the SAME bridge deals —
                # without this the PUSH path would re-emit a recovered deal as a
                # fresh fill. The closing deals usually belong to a DIFFERENT
                # order (native TP/SL, external close), so they are seeded
                # explicitly too. A retirement that failed seeds nothing: the next
                # pass delivers the trade from the same deals.
                self._seen_deal_ids.update(bridge.deal_ids)
                self._seen_deal_ids.update(bridge.closing_deal_ids)
                for other, other_bridge in group:
                    self._seen_deal_ids.update(other_bridge.deal_ids)
                return events
            return []
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
        fill_qty = cumulative - row.filled_qty
        # Commission is booked once per execution, with the fill that advances
        # the cursor: the recovered delta carries its own executions' and the
        # ranges a snapshot-only pass booked without it, never a deal the
        # cursor counts (charged by the push path, possibly in an earlier
        # process whose de-dup set is gone).
        commission = self._recovered_commission(row, bridge.fills, fill_qty)
        emits = bool(new_deal_ids) and fill_qty > 1e-9
        patch: dict = {}
        if link_position:
            patch['position_id'] = position_id
        if emits and commission.patch is not None:
            patch.update(commission.patch)
        # Each event books the executions it stands for at their own price,
        # never the whole order's average, which would re-price the part the
        # cursor already booked. ``avg_price`` (the ExchangeOrder snapshot's
        # display field) still prefers the open position's broker-reported
        # average when present.
        bridge_price = bridge.avg_price or deal.executionPrice or None
        events = self._recovered_entry_fills(
            row, bridge.fills, fill_qty, commission.fees, ranged=commission.ranged,
            event_type='filled', cumulative=cumulative,
            exchange_id=str(position_id or deal.orderId),
            avg_price=(open_positions[position_id].price if position_open
                       else bridge_price) or None,
            fallback_price=bridge_price,
            timestamp=(deal.executionTimestamp / 1000.0
                       if deal.executionTimestamp else now_ts)) if emits else []
        if cumulative > row.filled_qty + 1e-9 or link_position:
            # The cursor and the delivery records of the fills it now counts
            # land together.
            with self.store_ctx.transaction():
                DispatchJournal(self.store_ctx).apply_reconcile_outcome(
                    row.client_order_id,
                    ReconcileOutcome(
                        kind='filled',
                        reason='working_promoted_position',
                        new_state='confirmed',
                        audit_event='reconcile_working_promoted',
                        filled_qty=cumulative,
                        extras_patch=patch or None,
                        audit_payload={'deal_id': deal.dealId, 'position_id': position_id},
                        exchange_order_id=str(position_id or deal.orderId),
                    ),
                )
                if link_position:
                    assert position_id is not None
                    self._link_position_ref(row.client_order_id, position_id)
                self._journal_recovered_deliveries(events)
        if not new_deal_ids:
            # Every recovered deal was already applied by the PUSH path — the
            # cursor / position link are now consistent, so emit nothing.
            return []
        self._record_reconciled_fill(
            deal.orderId, position_id, row.side, fill_qty, bridge.fills, commission.new_ids)
        self._seen_deal_ids.update(new_deal_ids)
        if not emits:
            # The recovered deals are new to ``_seen_deal_ids`` (now seeded) but
            # the cumulative they sum to is already covered by the durable cursor
            # (a prior PUSH / reconcile counted this progress). Emitting here
            # would yield a zero-qty fill that ``record_fill`` ignores while the
            # engine still runs its post-fill side effects — seed the ids and stop.
            # Their commission is unbooked only for the ranges a snapshot-only
            # pass booked: delivered without a fill.
            return self._range_commission_events(
                row, bridge.fills, exchange_id=str(position_id or deal.orderId),
                timestamp=now_ts)
        return events

    def _filled_then_closed_events(
            self, row, bridge: '_DealBridgeResult',
            closes: Sequence[tuple[_PositionDeal, int]], now_ts: float, *,
            group: Sequence[tuple['OrderRow', '_DealBridgeResult']] = (),
    ) -> list[OrderEvent]:
        """The fills of a working order that filled and closed while the stream was down.

        The entry fill the stream never delivered — the order's recovered
        cumulative beyond the row's cursor, priced and charged from exactly
        the executions it stands for, unless every one of them was applied
        live already — then each close of the bot's share
        (:meth:`_recovered_close_event`) the stream never delivered, labelled
        as a vanished row's close is (:meth:`_vanished_close_label`). Read
        before the deals are seeded into :attr:`_seen_deal_ids`.

        :param row: The working order's journal row.
        :param bridge: The order's deal-history evidence.
        :param closes: ``(deal, cents)`` of the closing deals that took part of
            the bot's share, with that part, in execution order.
        :param now_ts: The observation time, for a deal without an execution time.
        :param group: Other working rows whose orders filled onto the same
            position, with their deal evidence: their entry fills too precede
            the closes.
        :return: The events, entry fills first.
        """
        deal = bridge.deal
        entries = [event for entry_row, entry_bridge in ((row, bridge), *group)
                   for event in self._recovered_entry_events(entry_row, entry_bridge, now_ts)]
        entries.sort(key=lambda event: event.timestamp)
        events: list[OrderEvent] = list(entries)
        events.extend(
            self._recovered_close_event(
                row, close, cents, from_entry=(
                    row.pine_entry_id if deal is None else self._vanished_close_label(
                        deal.positionId, close, row.pine_entry_id)))
            for close, cents in closes if close.deal_id not in self._seen_deal_ids)
        return events

    def _recovered_entry_events(
            self, row, bridge: '_DealBridgeResult', now_ts: float,
    ) -> list[OrderEvent]:
        """The entry fill of a working order the stream never delivered, one event per deal.

        The order's recovered cumulative beyond the row's cursor, as the
        executions it stands for (:meth:`_recovered_entry_fills`), each charged
        its own commission (:meth:`~_ReconcileMixin._recovered_commission`);
        empty when every one of them was applied live already. Read before the
        deals are seeded into :attr:`_seen_deal_ids`.
        """
        deal = bridge.deal
        new_ids = {deal_id for deal_id in bridge.deal_ids if deal_id not in self._seen_deal_ids}
        cumulative = min(row.qty, max(row.filled_qty, volume_to_units(bridge.filled_cents)))
        fill_qty = cumulative - row.filled_qty
        if deal is None or not new_ids or fill_qty <= _VOLUME_TOL:
            return []
        commission = self._recovered_commission(row, bridge.fills, fill_qty)
        bridge_price = bridge.avg_price or deal.executionPrice or None
        return self._recovered_entry_fills(
            row, bridge.fills, fill_qty, commission.fees, ranged=commission.ranged,
            event_type='filled', cumulative=cumulative,
            exchange_id=str(deal.positionId or deal.orderId), avg_price=bridge_price,
            fallback_price=bridge_price,
            timestamp=(deal.executionTimestamp / 1000.0
                       if deal.executionTimestamp else now_ts))

    async def _unbooked_position_entries(
            self, row, position_id: int, deals: Sequence[_PositionDeal], *,
            working_orders: Collection[int] = (),
            pending_orders: Mapping[str, tuple[int, int]] | None = None,
    ) -> list[tuple['OrderRow', '_DealBridgeResult']] | None:
        """The run's other entry rows whose orders filled onto a position that closed.

        A recovery that finds an entry order's position filled then closed (a
        working order's, or a pending dispatch's at startup) retires the bot's
        rows of it, so the closes are judged against the share counting every
        entry fill the journal does not count yet: each other live working or
        pending dispatch row (not fully filled) whose recorded order opened
        volume in the position's ``deals``, or which an earlier pass tied to
        the position (its refs name it, and it carries the ``executedVolume``
        floor the order reported, :meth:`~_RecoveryMixin._record_recovered_refs`)
        before its deals show, and each pending dispatch row of
        ``pending_orders`` (which also names an order the order history ties to
        the position before its deals show), with its own deal evidence. A
        runtime pass and a startup recovery thus form the same group.
        ``None`` while the evidence of
        one of them cannot be read in full, or shows less than the fill known
        for its order (:meth:`_fill_history_short`): the recovery waits for a
        later pass.

        :param row: The entry row being recovered.
        :param position_id: The closed position the group retires.
        :param deals: Every deal of the position's history read.
        :param working_orders: Orders the snapshot still shows working; a row
            of one is left to its own reconcile.
        :param pending_orders: Client order id -> ``(venue order, its
            executedVolume or 0 when not read)`` of the pending dispatch rows
            whose order filled onto the position
            (:meth:`~_RecoveryMixin._pending_position_orders`); the journal
            counts no fill of such a row yet.
        """
        if self.store_ctx is None:
            return []
        pending_orders = pending_orders or {}
        opening_orders = {deal.order_id for deal in deals if deal.opened > 0}
        group: list[tuple['OrderRow', _DealBridgeResult]] = []
        for other in list(self.store_ctx.iter_live_orders()):
            pending_id, executed = pending_orders.get(other.client_order_id, (None, 0))
            extras = other.extras or {}
            raw = extras.get('order_id') if pending_id is None else pending_id
            if (other.client_order_id == row.client_order_id or raw is None
                    or (pending_id is None and other.state not in _ENTRY_ROW_STATES
                        and other.state not in PENDING_DISPATCH_STATES)
                    or other.filled_qty >= other.qty - _VOLUME_TOL):
                continue
            other_id = parse_protocol_id(raw, field='order_id')
            tied = (int(extras.get('executed_cents') or 0) > 0
                    and str(extras.get('position_id')) == str(position_id))
            if ((pending_id is None and other_id not in opening_orders and not tied)
                    or other_id in working_orders):
                continue
            evidence = await self._find_fill_deal(
                other_id, self._deal_since_ms(extras),
                deadline=self._reconcile_pass_deadline)
            if (not evidence.conclusive
                    or self._fill_history_short(evidence, other, other_id, executed)
                    or evidence.deal is None):
                return None
            group.append((other, evidence))
        return group

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
        parts = _ReconcileMixin._executions_after(fills, booked, qty)
        taken = sum(part for _deal_id, part, _price, _fee in parts)
        if taken < qty - 1e-9:
            return None
        return sum(price * part for _deal_id, part, price, _fee in parts) / taken

    @staticmethod
    def _executions_after(
            fills: tuple[tuple[int, int, int, float, float], ...],
            booked: float, qty: float,
    ) -> list[tuple[int, float, float, float]]:
        """The executions that make up the ``qty`` units filled after ``booked``.

        An order fills in time order, so the ``booked`` units are the OLDEST
        executions; a deal straddling either boundary contributes only its part,
        with its commission prorated to that part.

        :param fills: The bridge's per-deal terms, oldest first
            (:attr:`_DealBridgeResult.fills`).
        :param booked: Units before the slice (a cursor or a range start).
        :param qty: Units of the slice.
        :return: ``(deal_id, units, price, commission)`` of each execution part,
            oldest first; their units sum to less than ``qty`` when the deals
            do not cover the slice.
        """
        skip = booked
        taken = 0.0
        parts: list[tuple[int, float, float, float]] = []
        for _ts, deal_id, cents, price, fee in fills:
            if taken >= qty - 1e-9:
                break
            units = volume_to_units(cents)
            if skip >= units - 1e-9:
                skip -= units
                continue
            part = min(units - skip, qty - taken)
            skip = 0.0
            parts.append((deal_id, part, price, fee * part / units))
            taken += part
        return parts

    async def _settled_row_range_commission(
            self, row, order_id: int, exchange_id: str, now_ts: float,
    ) -> list[OrderEvent]:
        """The commission a settled position row still owes for its snapshot-booked ranges.

        A snapshot-only pass booked part of the row's fill without commission
        (row extra ``fee_unbooked``). The push of a deal in that range carries
        its commission
        (:meth:`~pynecore_ctrader.events._EventStreamMixin._unbooked_commission_events`),
        and so does the recovery of the order's later progress; when that push
        never came and the order then filled to the end on a push of its own,
        the row is a settled position no recovery reads again. Its order's
        deal history is read on each pass until it covers the ranges, and the
        commission of the deals in them is delivered as fee events
        (:meth:`_range_commission_events`). A history still short of the
        row's fill drops the finished walk, so the next pass reads the whole
        window again (:meth:`_fill_history_short`).

        :param row: The settled row.
        :param order_id: Its entry order.
        :param exchange_id: The id the events' order carries.
        :param now_ts: The observation time.
        :return: The fee events; empty while the history does not cover the ranges.
        """
        bridge = await self._find_fill_deal(
            order_id, self._deal_since_ms(row.extras or {}),
            deadline=self._reconcile_pass_deadline)
        if not bridge.conclusive or self._fill_history_short(bridge, row, order_id):
            return []
        return self._range_commission_events(
            row, bridge.fills, exchange_id=exchange_id, timestamp=now_ts)

    def _range_commission_events(
            self, row, fills: tuple[tuple[int, int, int, float, float], ...], *,
            exchange_id: str, timestamp: float,
    ) -> list[OrderEvent]:
        """The commission of the row's ranges a snapshot-only pass booked, as fee events.

        A recovery whose deals the row's cursor already counts delivers no
        fill, so the commission of the ``fee_unbooked`` ranges the deals cover
        goes as one fee event per snapshot booking (``event_type`` ``'fee'``,
        the booking's fill id, the commission charged on it so far;
        :meth:`_charge_unbooked_ranges`), recorded until the engine accounted
        it (:meth:`~_ProtectionMixin._journal_fee_delivery`). The records and
        the ranges left are written in one transaction, so a range is charged
        either from its records or by a later recovery, never both.

        :param row: The recovered row.
        :param fills: The order's per-deal terms, oldest first.
        :param exchange_id: The id the events' order carries.
        :param timestamp: The observation time.
        :return: The fee events, oldest booking first.
        """
        store = self.store_ctx
        fresh = store.get_order(row.client_order_id) if store is not None else None
        if store is None or fresh is None:
            return []
        ranged, patch = self._charge_unbooked_ranges(fresh, fills)
        if patch is None or not ranged:
            return []
        events = [self._fee_event(fresh, fill_id, fee, exchange_id=exchange_id,
                                  timestamp=timestamp)
                  for fill_id, fee in ranged.items()]
        with store.transaction():
            extras = dict(fresh.extras or {})
            extras.update(patch)
            store.upsert_order(fresh.client_order_id, extras=extras)
            for event in events:
                self._journal_fee_delivery(event)
        return events

    @staticmethod
    def _fee_event(row, fill_id: str, fee: float, *, exchange_id: str,
                   timestamp: float) -> OrderEvent:
        """A commission of an entry row's fill delivered without the fill (``event_type`` ``'fee'``).

        :param row: The entry row the fill belongs to.
        :param fill_id: The fill, the event's fill id.
        :param fee: The fill's total commission (the engine books what its
            receipt does not account yet).
        :param exchange_id: The id the event's order carries.
        :param timestamp: The event time.
        """
        return OrderEvent(
            order=ExchangeOrder(
                id=exchange_id, symbol=row.symbol, side=row.side, order_type=OrderType.MARKET,
                qty=row.qty, filled_qty=row.filled_qty,
                remaining_qty=max(0.0, row.qty - row.filled_qty), price=None, stop_price=None,
                average_fill_price=None, status=OrderStatus.FILLED, timestamp=timestamp,
                fee=0.0, fee_currency='', reduce_only=False,
                client_order_id=row.client_order_id,
            ),
            event_type=FEE_EVENT_TYPE, fill_price=None, fill_qty=None, timestamp=timestamp,
            pine_id=row.pine_entry_id, from_entry=row.from_entry, leg_type=LegType.ENTRY,
            fee=fee, fill_id=fill_id,
        )

    def _recovered_commission(
            self, row, fills: tuple[tuple[int, int, int, float, float], ...], fill_qty: float,
    ) -> '_RecoveredCommission':
        """The commission a recovered fill of a row books, and what it covers.

        Commission is booked with the fill that advances the row's cursor, so a
        deal the cursor counts was charged already, in this process or an
        earlier one of the run: the recovered ``fill_qty`` beyond the cursor is
        charged from exactly its executions (less those the push path applied
        in this process), each on its own deal. Every range of the cursor a
        snapshot-only reconcile booked without commission (row extra
        ``fee_unbooked``) that the deals cover in full is charged on the fill
        id of that booking (:meth:`_charge_unbooked_ranges`): the part of a
        deal straddling the cursor that the booking counts goes to the trade
        the booking opened.

        :param row: The row whose fill is recovered.
        :param fills: The order's per-deal terms, oldest first.
        :param fill_qty: Units of the recovered fill beyond the cursor.
        """
        beyond = self._executions_after(fills, row.filled_qty, fill_qty)
        new_ids = {deal_id for deal_id, _part, _price, _fee in beyond
                   if deal_id not in self._seen_deal_ids}
        fees: dict[int, float] = {}
        for deal_id, _part, _price, part_fee in beyond:
            if deal_id in new_ids:
                fees[deal_id] = fees.get(deal_id, 0.0) + part_fee
        ranged, patch = self._charge_unbooked_ranges(row, fills)
        return _RecoveredCommission(fees=fees, new_ids=new_ids, ranged=ranged, patch=patch)

    def _charge_unbooked_ranges(
            self, row, fills: tuple[tuple[int, int, int, float, float], ...],
    ) -> tuple[dict[str, float], dict | None]:
        """Charge the row's snapshot-booked ranges the deals cover in full.

        :param row: The row, as the journal holds it.
        :param fills: The order's per-deal terms, oldest first.
        :return: ``(fill id of a snapshot booking -> the total commission its
            fee event carries, the extras patch of the ranges and totals
            left)``; the patch is ``None`` when the row lists no range.
        """
        extras = row.extras or {}
        ranges = extras.get(_FEE_UNBOOKED_EXTRA)
        if not ranges:
            return {}, None
        left: list[list] = []
        charges: dict[str, float] = {}
        for entry in ranges:
            start, end, fill_id = _range_terms(row.client_order_id, entry)
            parts = self._executions_after(fills, start, end - start)
            if sum(part for _deal_id, part, _price, _fee in parts) < end - start - 1e-9:
                left.append([start, end, fill_id])
                continue
            charges[fill_id] = charges.get(fill_id, 0.0) + sum(
                part_fee for _deal_id, _part, _price, part_fee in parts)
        charged, totals = _charge_ranged_fees(extras, charges, left)
        return charged, {_FEE_UNBOOKED_EXTRA: left or None, _FEE_RANGED_EXTRA: totals}

    def _recovered_entry_fills(
            self, row, fills: tuple[tuple[int, int, int, float, float], ...], fill_qty: float,
            fees: Mapping[int, float], *, ranged: Mapping[str, float], event_type: str,
            cumulative: float, exchange_id: str, avg_price: float | None,
            fallback_price: float | None, timestamp: float, rest_fill_id: str | None = None,
    ) -> list[OrderEvent]:
        """The events of a row's entry fill recovered beyond its cursor: one per deal.

        Each execution the recovered ``fill_qty`` stands for
        (:meth:`_executions_after`; a deal straddling the cursor contributes
        only its part) is delivered as the push path delivers it: its deal id
        as the fill id, at its own price and time, with the commission it
        books now (``fees``). The engine's fill receipt, the duplicate gate
        and the delivery records (:meth:`_journal_recovered_deliveries`) then
        treat a recovered fill and its push copy as the same execution, in this
        process and after a restart. The commission of the ``fee_unbooked``
        ranges the deals cover (``ranged``) goes first, as a fee event of each
        snapshot booking (:meth:`_fee_event`), so it reaches the trade that
        booking opened. Volume the deals do not cover (the order's cumulative
        ran ahead of the history) goes last, at ``fallback_price``, under
        ``rest_fill_id``.

        :param row: The row whose fill is recovered, as it is before the
            recovery's journal write.
        :param fills: The order's per-deal terms, oldest first; empty when the
            history does not cover the fill.
        :param fill_qty: Units of the recovered fill beyond the cursor.
        :param fees: Deal id -> the commission its part beyond the cursor
            books now (:attr:`_RecoveredCommission.fees`).
        :param ranged: Fill id of a snapshot booking -> the total commission
            its fee event carries (:attr:`_RecoveredCommission.ranged`).
        :param event_type: The last fill event's type; the ones before it are
            ``'partial'``.
        :param cumulative: The order's cumulative fill after the recovery.
        :param exchange_id: The id the events' order carries.
        :param avg_price: The order's average price the events carry.
        :param fallback_price: The price of volume no deal covers.
        :param timestamp: The time of an event whose deal carries none.
        :param rest_fill_id: The fill id of the volume no deal covers: the
            snapshot booking's (:func:`_snapshot_fill_id`) when it is booked
            from the order snapshot alone, ``None`` otherwise.
        :return: The events, fee events first, then the fills in execution order.
        """
        executed = {deal_id: executed_ms for executed_ms, deal_id, _c, _p, _f in fills}
        parts = self._executions_after(fills, row.filled_qty, fill_qty)
        events: list[OrderEvent] = [
            self._fee_event(row, fill_id, fee, exchange_id=exchange_id, timestamp=timestamp)
            for fill_id, fee in ranged.items()]
        slices: list[tuple[str | None, float, float | None, float, float]] = [
            (str(deal_id), part, price or fallback_price, fees.get(deal_id, 0.0),
             executed[deal_id] / 1000.0 if executed.get(deal_id) else timestamp)
            for deal_id, part, price, _fee in parts]
        rest = fill_qty - sum(part for _deal_id, part, _price, _fee in parts)
        if rest > _VOLUME_TOL:
            slices.append((rest_fill_id, rest, fallback_price, 0.0, timestamp))
        booked = row.filled_qty
        for index, (fill_id, qty, price, fee, at) in enumerate(slices):
            last = index == len(slices) - 1
            booked = cumulative if last else min(cumulative, booked + qty)
            events.append(self._fill_event(
                row, event_type=event_type if last else 'partial',
                status=(OrderStatus.FILLED if last and cumulative >= row.qty - 1e-9
                        else OrderStatus.PARTIALLY_FILLED),
                exchange_id=exchange_id, cumulative=booked, fill_qty=qty,
                avg_price=avg_price, fill_price=price, fee=fee, timestamp=at,
                fill_id=fill_id))
        return events

    def _journal_recovered_deliveries(self, events: Iterable[OrderEvent]) -> None:
        """Record recovered fills and commissions in the transaction of their journal write.

        The fill events through :meth:`~_ProtectionMixin._journal_fill_delivery`,
        the fee events through :meth:`~_ProtectionMixin._journal_fee_delivery`.
        """
        events = list(events)
        self._journal_fill_delivery(events)
        for event in events:
            if event.event_type == FEE_EVENT_TYPE:
                self._journal_fee_delivery(event)

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
                audit_event=_FILLED_THEN_CLOSED_EVENT,
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
        missing evidence. A window that lists a fill of the order without
        ``executionPrice`` is not conclusive either (an unpriced fill books
        nothing). Its walk is dropped, so the next call reads it whole again,
        as is the walk of a window holding an unpriced close not judged yet. A truncated page narrows its sub-window to the oldest
        deal timestamp, inclusive, so the deals sharing that timestamp that the
        page cut off are read by the next page (a re-read deal replaces its
        copy by ``dealId``). A page that announces more but cannot narrow the
        sub-window (empty, without timestamps, or already at either bound of
        it) leaves the rest unread: inconclusive, and the sub-window stays owed.

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
                if not res.hasMore:
                    walk.segments.pop(0)
                    continue
                oldest = min(((d.executionTimestamp or d.createTimestamp)
                              for d in res.deal), default=0)
                if not oldest or oldest <= segment[0] or oldest >= segment[1]:
                    logger.warning(
                        "cTrader deal-history bridge for order %d cannot page past %d: "
                        "the window stays unread", order_id, segment[1])
                    return _DealBridgeResult(None, 0, None, False)
                segment[1] = oldest
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
        result = walk.result(close_position_id)
        if not result.conclusive or any(
                deal.price <= 0.0 and deal.closed > 0
                and deal.deal_id not in self._judged_close_deals
                for deal in result.position_deals):
            # A window holding a fill of the order or a close still to judge
            # without a price is read whole again, so the price the venue adds
            # later reaches the evidence.
            self._deal_walks.pop(order_id, None)
        return result

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
        engine's external-intervention quarantine; with an unwired or failing
        quarantine sink it raises
        :class:`~pynecore.core.broker.exceptions.UnexpectedCancelError` through the
        reconcile pass, the fail-safe signal the engine latches the same
        quarantine on). An order the venue itself ended (expired, rejected)
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
        self._load_close_evidence()
        async for event in self._disappearance_tracker().observe(
                {'positions': None}, epoch_time()):
            yield event

    def _cancelled_event(self, row, now_ts: float) -> OrderEvent:
        """Build the synthetic cancelled event for a vanished bot-owned row.

        Carries the row's Pine identity (``pine_entry_id`` / ``LegType.ENTRY``)
        so the sync engine books the disappearance against the originating
        entry, exactly as the PUSH path's ``ORDER_CANCELLED`` events do. Its
        executed quantity is known (``filled_qty_known``): the tracker builds
        it only on a cancel verdict, which a complete deal-history read of the
        order listing no fill beyond the row's cursor backs
        (:meth:`_confirm_missing_via_deal_history`); the quantity is the one
        verdict on the order's execution (:meth:`~_CTraderBase._execution_evidence`).
        """
        order_id = (row.extras or {}).get('order_id')
        executed = self._execution_evidence(row, reported=row.filled_qty).executed
        return OrderEvent(
            order=ExchangeOrder(
                id=str(order_id) if order_id else '', symbol=row.symbol,
                side=row.side, order_type=OrderType.MARKET,
                qty=row.qty, filled_qty=executed,
                remaining_qty=max(0.0, row.qty - executed),
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

    def _recovered_close_event(
            self, row: 'OrderRow', deal: _PositionDeal, cents: int, *,
            from_entry: str | None,
    ) -> OrderEvent:
        """Build the close fill of a closing deal the PUSH stream missed.

        Shaped like the PUSH path's event for the same execution: a close leg
        labelled ``from_entry`` carrying the deal id as its fill id, for the
        part of the bot's share the deal took, at the deal's price, with the
        deal's commission prorated to that part, and the client order id of
        the run's close the deal belongs to (:meth:`_recovered_close_coid`).

        :param row: An entry row of the position the deal closed.
        :param deal: The closing deal from the position's deal history.
        :param cents: The part of the bot's share the deal took (cents).
        :param from_entry: The close's label: the entry it closes, a close
            of the run's own label, or ``None`` for an unbound close.
        """
        qty = volume_to_units(cents)
        fee = deal.fee * cents / deal.filled if deal.filled else 0.0
        price = deal.price or None
        timestamp = deal.executed_ms / 1000.0
        return OrderEvent(
            order=ExchangeOrder(
                id=str(deal.order_id), symbol=row.symbol, side=deal.side,
                order_type=OrderType.MARKET, qty=qty, filled_qty=qty, remaining_qty=0.0,
                price=None, stop_price=None, average_fill_price=price,
                status=OrderStatus.FILLED, timestamp=timestamp, fee=0.0, fee_currency='',
                reduce_only=True,
                client_order_id=self._recovered_close_coid(deal),
            ),
            event_type='filled', fill_price=price, fill_qty=qty, timestamp=timestamp,
            from_entry=from_entry, leg_type=LegType.CLOSE, fee=fee,
            fill_id=str(deal.deal_id), journal_fill_qty=qty,
        )

    @staticmethod
    def _fill_event(
            row, *, event_type: str, status: OrderStatus, exchange_id: str,
            cumulative: float, fill_qty: float, avg_price: float | None,
            fill_price: float | None, fee: float, timestamp: float,
            fill_id: str | None = None,
    ) -> OrderEvent:
        """Build the reconcile-recovered fill event for an entry row.

        Carries the row's Pine identity (``pine_entry_id`` / ``LegType.ENTRY``)
        so the sync engine books it against the originating entry, exactly as
        the PUSH path's entry-fill events do.

        :param fill_id: The deal the fill is, when one deal is all of it
            (:meth:`_recovered_entry_fills`).
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
            leg_type=LegType.ENTRY, fee=fee, fill_id=fill_id,
            # ``fill_qty`` is the rise of the row's ``filled_qty`` cursor.
            journal_fill_qty=fill_qty,
        )
