"""External-intervention detection mix-in for the cTrader Open API plugin.

Someone other than the bot — the operator in a cTrader UI, another bot on the
account — can move or remove the bot's protective levels, or close the bot's
position by hand. Both must latch the external-intervention quarantine through
:meth:`~pynecore.core.plugin.broker.BrokerPlugin.report_external_intervention`;
the bot's own writes and the venue's own mechanics never may.

**Protection levels.** cTrader protection is a set of position attributes
(``stopLoss`` / ``takeProfit`` / ``trailingStopLoss``) that one
``ProtoOAAmendPositionSLTPReq`` overwrites wholesale. Every such amend the
bracket paths send passes through :meth:`_ProtectionMixin._write_protection`,
which records the levels in a core
:class:`~pynecore.core.broker.protection_guard.ProtectionLevelGuard` keyed by
the position; the venue's view — a position pushed on a non-fill execution
event, or the periodic reconcile snapshot — is judged against them. A trailing
stop moves by itself, so under trailing only the flag and the take-profit are
compared. Two writers keep the position out of the guard: the native fail-safe
(the core fail-safe manager judges its levels and quarantines on an external
edit itself) and a bracket clear (an unprotected position has no levels of the
bot left to protect).

**Closing fills.** A closing fill on a position the run owns is classified:

- *venue* — the position's own SL / TP order (``STOP_LOSS_TAKE_PROFIT``), a
  stop-out (``isStopOut``) or any other server-initiated execution
  (``isServerEvent``);
- *own* — a close this run sent (:class:`_OwnClose`, matched by the close
  order id from the response, or by position while the response is
  outstanding or its outcome unknown), or an order carrying one of this run's
  client-order-ids;
- *external* — anything else. The fill event is yielded first and the
  report follows it. Under ``stop`` the quarantine only blocks venue writes
  and the fill is booked as usual; under ``halt`` the report raises right
  after the yield, and the engine books every event already queued — this
  fill included — before the run stops, with venue writes blocked.

``ProtoOAClosePositionReq`` carries no client-order-id or label, so the own
close can only be recognised from this run's in-memory dispatch record.

**Recovered closes.** A close whose PUSH was missed (the stream was down)
surfaces only as deal-history evidence when the reconcile pass retires the
position. Each closing deal of it no process of the run attributed or judged
yet is judged by :meth:`_ProtectionMixin._judge_recovered_closes`: *own* when
its order is a close the run sent (by order id, or by position and time while
the close's outcome is unknown) or one of this run's orders; *venue* when the
order (``ProtoOAOrderDetailsReq``) is the position's ``STOP_LOSS_TAKE_PROFIT``
order or a stop-out; *external* otherwise. The execution event's
``isServerEvent`` flag is not part of the order, so a server-initiated close
that is neither of those two is judged external here. The run's own closes and
the closing deals already attributed or judged are journalled, so a restarted
process judges with the evidence of the earlier ones. A close an earlier
process sent but never saw answered claims only the deals within its grace
after the send. A lookup that failed for now defers the verdict to the next
pass; an order the venue reports as not found is never reported.

**Report delivery.** A queued manual-close report is journalled before its
deals are marked judged, and marked handled once the policy ran; a report an
earlier process queued but never handled is replayed by the next one, as soon
as its order-event stream starts, and no venue write leaves before that.
"""
import asyncio
import logging
import math
import time
import uuid
from abc import ABC
from dataclasses import dataclass, field
from enum import StrEnum
from time import time as epoch_time
from typing import TYPE_CHECKING, cast

from pynecore.core.broker.exceptions import (
    BrokerError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    UnexpectedCancelError,
    VenueWritesQuarantinedError,
)
from pynecore.core.broker.protection_guard import (
    PROTECTION_LEVELS_EXTRA,
    ProtectionLevelGuard,
    ProtectionLevels,
)

from ._base import _CTraderBase
from .exceptions import CTraderProtocolError, is_not_found
from .helpers import parse_protocol_id, volume_to_units
from .messages import OpenApiMessages_pb2 as OpenApiMessages
from .messages import OpenApiModelMessages_pb2 as OpenApiModelMessages
from .wire import CTraderWireError

if TYPE_CHECKING:
    from pynecore.core.broker.storage import RunContext

logger = logging.getLogger(__name__)

#: Seconds a close this run sent keeps claiming closing fills on its position
#: after the venue answered (or the answer was lost). A market close fills
#: within seconds; the margin covers a fill push delayed by a reconnect.
_OWN_CLOSE_GRACE_S = 300.0

#: Margin (ms) between this host's wall clock and the venue's deal timestamps,
#: applied in the direction that claims more closes as the run's own.
_CLOCK_SKEW_MS = 60_000

#: Journal event of one close the run sent, written at each state change.
_OWN_CLOSE_EVENT = 'ctrader_own_close'

#: Journal event naming closing deals whose origin was decided.
_CLOSE_JUDGED_EVENT = 'ctrader_close_judged'

#: Journal event of one queued manual-close report, written before its deals
#: are marked judged, so a report a dead process never delivered is replayed.
_CLOSE_REPORT_PENDING_EVENT = 'ctrader_close_report_pending'

#: Journal event of a manual-close report the intervention policy handled.
_CLOSE_REPORT_DONE_EVENT = 'ctrader_close_report_done'


class CloseOrigin(StrEnum):
    """Who caused a closing fill on a run-owned position."""
    OWN = 'own'
    VENUE = 'venue'
    EXTERNAL = 'external'
    #: The venue reports the closing order as not found; never reported.
    UNKNOWN = 'unknown'


@dataclass(slots=True, eq=False)
class _OwnClose:
    """A ``ProtoOAClosePositionReq`` this run sent.

    :ivar position_id: The position the close reduces.
    :ivar sent_ms: Wall-clock time (ms) the close was sent.
    :ivar order_id: The venue's close ``orderId`` once the response (or the
        first fill) named it.
    :ivar expires_at: Monotonic deadline after which the record stops claiming
        fills; infinite while the request is in flight.
    :ivar answered_ms: Wall-clock time (ms) the venue answered or the answer
        was lost; ``None`` while the request is in flight.
    :ivar key: Identifies the close across its journal events.
    """
    position_id: int
    sent_ms: int
    order_id: int | None = None
    expires_at: float = math.inf
    answered_ms: int | None = None
    key: str = field(default_factory=lambda: uuid.uuid4().hex)

    def claims_recovered(self, position_id: int, order_id: int, executed_ms: int) -> bool:
        """Whether a closing deal found in the deal history may be this close's.

        :param position_id: The position the deal closed volume on.
        :param order_id: The deal's order.
        :param executed_ms: The deal's execution time (venue clock, ms).
        """
        if self.order_id is not None:
            return self.order_id == order_id
        if self.position_id != position_id or executed_ms < self.sent_ms - _CLOCK_SKEW_MS:
            return False
        return (self.answered_ms is None
                or executed_ms <= self.answered_ms + _OWN_CLOSE_GRACE_S * 1000 + _CLOCK_SKEW_MS)


def protection_key(position_id: int) -> str:
    """The guard key of one position's protection."""
    return f"position:{position_id}"


def _write_refused(exc: BaseException) -> bool:
    """Whether a failed protection write provably left the venue unchanged.

    :meth:`~pynecore_ctrader.execution._ExecutionMixin._dispatch_order` raises
    a plain :class:`ExchangeConnectionError` only when the request was not
    written, or the venue refused it before executing it (rate limit, lost
    account authorization, venue-write quarantine); a mapped reject carries
    the venue's error answer as its :class:`CTraderProtocolError` cause.
    Anything else — a disposition-unknown timeout or post-write drop, a
    cancellation, an unexpected response — may have landed.
    """
    if isinstance(exc, ExchangeConnectionError):
        return True
    return (isinstance(exc, ExchangeOrderRejectedError)
            and isinstance(exc.__cause__, CTraderProtocolError))


class _ProtectionMixin(_CTraderBase, ABC):
    """Moved / removed SL-TP and manual-close detection."""

    # --- protection level guard ---------------------------------------------

    def _protection_guard(self) -> ProtectionLevelGuard:
        """The run's protection level guard, bound to the current journal.

        Built on first use because ``store_ctx`` is injected after
        ``__init__``; rebuilt if the journal is replaced.
        """
        guard = self._level_guard
        if guard is None or self._level_guard_store is not self.store_ctx:
            guard = ProtectionLevelGuard(
                self._report_protection_edit, store_ctx=self.store_ctx)
            self._level_guard = guard
            self._level_guard_store = self.store_ctx
            self._protection_baselined.clear()
        return guard

    def _report_protection_edit(self, reason: str, context: dict) -> None:
        """Guard ``report`` hook: apply the external-intervention policy."""
        _prefix, _sep, position_ref = str(context.get('protection_key') or '').partition(':')
        self.report_external_intervention(
            reason, context,
            client_order_id=context.get('client_order_id'),
            exchange_order_id=position_ref or None,
        )

    @staticmethod
    def _requested_protection(
            req: OpenApiMessages.ProtoOAAmendPositionSLTPReq,
    ) -> ProtectionLevels | None:
        """The protection one amend request leaves on the position.

        The amend overwrites the stop and the take-profit (an omitted level is
        removed), while an omitted trailing flag is retained by the venue; a
        trailing flag without a stop means nothing, so ``trailing`` counts only
        with a stop, as in :meth:`_venue_protection`.

        :return: The levels, or ``None`` for a trailing request without an
            anchor stop: the venue seeds that stop itself, so its result cannot
            be predicted and the position is not guarded.
        """
        stop = req.stopLoss if req.HasField('stopLoss') else None
        limit = req.takeProfit if req.HasField('takeProfit') else None
        trailing = req.HasField('trailingStopLoss') and req.trailingStopLoss
        if trailing and stop is None:
            return None
        return ProtectionLevels(stop=stop, limit=limit, trailing=trailing)

    @staticmethod
    def _venue_protection(position: OpenApiModelMessages.ProtoOAPosition) -> ProtectionLevels:
        """The protection a venue position carries.

        The trailing stop's current level rides in ``stopLoss``; cTrader
        reports no trailing distance, so ``trail_distance`` stays ``None``.
        """
        stop = position.stopLoss if position.HasField('stopLoss') else None
        limit = position.takeProfit if position.HasField('takeProfit') else None
        trailing = (stop is not None and position.HasField('trailingStopLoss')
                    and position.trailingStopLoss)
        return ProtectionLevels(stop=stop, limit=limit, trailing=trailing)

    def _protection_row(self, position_id: int) -> str | None:
        """The live entry row a position's protection levels are persisted on.

        One row per position, whichever entry's exit wrote the levels: the row
        already carrying them, else the first live entry row holding the
        position. Pyramid entries share a netted position, and a record left
        behind on a second row would be restored instead of the current one.
        ``None`` without a journal or such a row (a startup-adopted position):
        the levels are then kept in memory only.
        """
        if self.store_ctx is None:
            return None
        key = protection_key(position_id)
        fallback: str | None = None
        for row in self.store_ctx.iter_live_orders():
            extras = row.extras or {}
            raw = extras.get('position_id')
            if raw is None or not row.pine_entry_id:
                continue
            try:
                if parse_protocol_id(raw, field='position_id') != position_id:
                    continue
            except ValueError:
                continue
            saved = extras.get(PROTECTION_LEVELS_EXTRA)
            if isinstance(saved, dict) and saved.get('key') == key:
                return row.client_order_id
            if fallback is None:
                fallback = row.client_order_id
        return fallback

    async def _write_protection(
            self, req: OpenApiMessages.ProtoOAAmendPositionSLTPReq, *,
            position_id: int, row_coid: str | None, coid: str, context: str,
            predecessor_cancel_ids: tuple[str, ...] | None = None,
    ) -> OpenApiMessages.ProtoOAExecutionEvent:
        """Send one SL/TP amend, recording its levels as the bot's own.

        The levels are expected before the send and settled once the venue
        answered, naming the write :meth:`ProtectionLevelGuard.expect` returned,
        so overlapping writes of the same levels answered out of order each
        close their own: confirmed on success; rejected when the venue provably did
        not apply them (:func:`_write_refused`), so they stop being acceptable
        at once; unknown (``None``) on any other failure, so a write that may
        have landed stays acceptable next to the levels it would have replaced
        until a venue observation resolves it.

        Until one write of a position is confirmed (or its levels adopted) the
        guard knows nothing the venue carried before the bot wrote: a failed
        write that leaves no such baseline and no other write of the position
        in flight forgets the position, since a venue that never received the
        writes would match nothing the guard holds. While another write is in
        flight its answer decides, so a write confirmed meanwhile is never
        dropped by an older one failing.
        """
        key = protection_key(position_id)
        guard = self._protection_guard()
        levels = self._requested_protection(req)
        if levels is None:
            self._release_protection(position_id)
            return await self._dispatch_order(
                req, coid=coid, context=context,
                predecessor_cancel_ids=predecessor_cancel_ids)
        in_flight = self._protection_in_flight
        if guard.known(key) and not in_flight.get(key):
            self._protection_baselined.add(key)
        write = guard.expect(key, levels, row_coid=row_coid)
        in_flight[key] = in_flight.get(key, 0) + 1
        try:
            event = await self._dispatch_order(
                req, coid=coid, context=context,
                predecessor_cancel_ids=predecessor_cancel_ids)
        except BaseException as exc:
            self._end_protection_write(key)
            if _write_refused(exc):
                guard.settle(key, None, rejected=levels, write=write)
            else:
                guard.settle(key, None, write=write)
            if key not in self._protection_baselined and not in_flight.get(key):
                guard.forget(key)
            raise
        self._end_protection_write(key)
        guard.settle(key, levels, write=write)
        self._protection_baselined.add(key)
        return event

    def _end_protection_write(self, key: str) -> None:
        """One write of ``key`` got its answer (or lost it)."""
        remaining = self._protection_in_flight.get(key, 0) - 1
        if remaining > 0:
            self._protection_in_flight[key] = remaining
        else:
            self._protection_in_flight.pop(key, None)

    def _release_protection(self, position_id: int) -> None:
        """Stop guarding a position's protection (closed, cleared, fail-safe)."""
        key = protection_key(position_id)
        self._protection_guard().forget(key)
        self._protection_baselined.discard(key)

    def _position_owned(self, position_id: int) -> bool:
        owned = self._owned_position_ids()
        return owned is None or position_id in owned

    def _cached_tick(self, symbol_id: int) -> float | None:
        """Price tick of a symbol whose rules are cached, else ``None``."""
        name = self._symbols_by_id.get(symbol_id)
        rules = self._symbol_rules.get(name) if name is not None else None
        return None if rules is None else 10.0 ** -rules.digits

    async def _resolve_tick(self, symbol_id: int) -> float | None:
        """Price tick of a symbol, loading its rules within the reconcile budget.

        After a restart no order has loaded the rules yet. A failed or
        overrunning read skips this observation (``None``) instead of failing
        the reconcile pass.
        """
        tick = self._cached_tick(symbol_id)
        if tick is not None:
            return tick
        name = self._symbols_by_id.get(symbol_id)
        if name is None:
            return None
        try:
            async with asyncio.timeout_at(self._reconcile_pass_deadline):
                rules = await self._get_symbol_rules(name)
        except (BrokerError, CTraderWireError, TimeoutError) as exc:
            logger.debug("cTrader protection check skipped for %s: %s", name, exc)
            return None
        return 10.0 ** -rules.digits

    def _observe_pushed_protection(self, position: OpenApiModelMessages.ProtoOAPosition) -> None:
        """Judge the protection of a position a non-fill execution event carried.

        Own amend responses arrive this way too, and a push may describe an
        earlier state, so it is never judged alone (``immediate=False``).
        """
        position_id = position.positionId
        if (not position_id or position.positionStatus
                != OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN):
            return
        key = protection_key(position_id)
        guard = self._protection_guard()
        if not guard.known(key) or not self._position_owned(position_id):
            return
        tick = self._cached_tick(position.tradeData.symbolId)
        if tick is None:
            return
        guard.observe(key, self._venue_protection(position), tick=tick)

    def _absorb_fill_protection(self, position: OpenApiModelMessages.ProtoOAPosition) -> None:
        """Accept the protection a position shows right after this run's fill.

        Whatever the venue does to the protection of a position this run just
        added to or reversed is caused by the run, never by someone else.
        """
        position_id = position.positionId
        if (not position_id or position.positionStatus
                != OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN):
            return
        key = protection_key(position_id)
        guard = self._protection_guard()
        if not guard.known(key):
            return
        guard.adopt(key, self._venue_protection(position))
        self._protection_baselined.add(key)

    async def _observe_protection_snapshot(
            self, open_positions: dict[int, OpenApiModelMessages.ProtoOAPosition], *,
            read_started_at: float, filled_positions: set[int],
    ) -> None:
        """Judge every guarded, run-owned open position of a reconcile snapshot.

        :param open_positions: The snapshot's open positions by id.
        :param read_started_at: Wall-clock time the snapshot read started.
        :param filled_positions: Positions this pass booked a fill of the
            run's own order on; their protection is accepted as it is.
        """
        guard = self._protection_guard()
        for position_id, position in open_positions.items():
            key = protection_key(position_id)
            if not guard.known(key) or not self._position_owned(position_id):
                continue
            levels = self._venue_protection(position)
            if position_id in filled_positions:
                guard.adopt(key, levels)
                self._protection_baselined.add(key)
                continue
            tick = await self._resolve_tick(position.tradeData.symbolId)
            if tick is None:
                continue
            guard.observe(key, levels, tick=tick, read_started_at=read_started_at)

    # --- own closes and closing-fill classification --------------------------

    def _open_own_close(self, position_id: int) -> _OwnClose:
        """Record a close of ``position_id`` this run is about to send.

        The record claims PUSH fills until its grace ends, and stays in
        :attr:`_own_close_history` for judging recovered closes.
        """
        record = _OwnClose(position_id=position_id, sent_ms=int(epoch_time() * 1000))
        self._own_closes.append(record)
        self._own_close_history.append(record)
        self._journal_own_close(record)
        return record

    def _settle_own_close(self, record: _OwnClose, order_id: int | None) -> None:
        """The venue answered the close (or the answer is lost): start its grace."""
        if record.order_id is None and order_id:
            record.order_id = order_id
        record.expires_at = time.monotonic() + _OWN_CLOSE_GRACE_S
        record.answered_ms = int(epoch_time() * 1000)
        self._journal_own_close(record)

    def _drop_own_close(self, record: _OwnClose) -> None:
        """The close was provably not executed."""
        if record in self._own_closes:
            self._own_closes.remove(record)
        if record in self._own_close_history:
            self._own_close_history.remove(record)
        self._journal_own_close(record, refused=True)

    def _journal_own_close(self, record: _OwnClose, *, refused: bool = False) -> None:
        """Persist the state of one own close for a restarted process."""
        if self.store_ctx is None:
            return
        self.store_ctx.log_event(
            _OWN_CLOSE_EVENT, exchange_order_id=str(record.position_id),
            payload={'key': record.key, 'position_id': record.position_id,
                     'sent_ms': record.sent_ms, 'order_id': record.order_id,
                     'answered_ms': record.answered_ms, 'refused': refused},
        )

    def _load_close_evidence(self) -> None:
        """Merge the own closes and judged deals earlier processes of the run journalled.

        Once per journal. Records of this process stay as they are; a close an
        earlier process sent counts as the run's own like one of this process.
        One that process never saw answered is no longer in flight: it claims
        the deals of its grace counted from the send. Manual-close reports an
        earlier process queued but never handled are queued again.
        """
        store = self.store_ctx
        if store is None or self._close_evidence_store is store:
            return
        self._close_evidence_store = store
        present = {record.key for record in self._own_close_history}
        loaded: dict[str, _OwnClose | None] = {}
        for _intent, _coid, _ref, payload in store.iter_events_by_kind_for_run_id(
                _OWN_CLOSE_EVENT):
            key = payload.get('key')
            if not isinstance(key, str) or key in present or (
                    key in loaded and loaded[key] is None):
                continue
            if payload.get('refused'):
                loaded[key] = None
                continue
            try:
                loaded[key] = _OwnClose(
                    position_id=int(payload['position_id']),
                    sent_ms=int(payload['sent_ms']),
                    order_id=int(payload['order_id']) if payload.get('order_id') else None,
                    expires_at=-math.inf,
                    answered_ms=(int(payload['answered_ms'])
                                 if payload.get('answered_ms') is not None
                                 else int(payload['sent_ms'])),
                    key=key,
                )
            except (KeyError, TypeError, ValueError):
                continue
        self._own_close_history.extend(r for r in loaded.values() if r is not None)
        for _intent, _coid, _ref, payload in store.iter_events_by_kind_for_run_id(
                _CLOSE_JUDGED_EVENT):
            deal_ids = payload.get('deal_ids')
            if isinstance(deal_ids, list):
                self._judged_close_deals.update(
                    int(d) for d in deal_ids if isinstance(d, int))
        self._replay_close_reports(store)

    def _replay_close_reports(self, store: 'RunContext') -> None:
        """Queue the manual-close reports journalled as pending but never handled."""
        handled = {
            payload.get('position_id') for _intent, _coid, _ref, payload
            in store.iter_events_by_kind_for_run_id(_CLOSE_REPORT_DONE_EVENT)}
        for _intent, coid, position_ref, payload in store.iter_events_by_kind_for_run_id(
                _CLOSE_REPORT_PENDING_EVENT):
            position_id = payload.get('position_id')
            reason = payload.get('reason')
            context = payload.get('context')
            if (not isinstance(position_id, int) or position_id in handled
                    or position_id in self._external_close_reported
                    or not isinstance(reason, str) or not isinstance(context, dict)):
                continue
            self._external_close_reported.add(position_id)
            self._unhandled_close_replays.add(position_id)
            logger.warning("cTrader: replaying an unhandled report: %s", reason)
            self._deferred_interventions.append((reason, context, coid, position_ref))

    def _claim_own_close(self, position_id: int, order_id: int, *, terminal: bool) -> bool:
        """Whether a closing fill belongs to a close this run sent.

        Matches the close order id; a record whose order id is not known yet
        (response outstanding, outcome unknown) claims any closing fill on its
        position. The record is removed on the order's final fill.
        """
        now = time.monotonic()
        self._own_closes = [c for c in self._own_closes if c.expires_at > now]
        match = next((c for c in self._own_closes
                      if order_id and c.order_id == order_id), None)
        if match is None:
            match = next((c for c in self._own_closes
                          if c.position_id == position_id and c.order_id is None), None)
        if match is None:
            return False
        if match.order_id is None and order_id:
            match.order_id = order_id
        if terminal:
            self._own_closes.remove(match)
        return True

    def _close_fill_origin(
            self, message: OpenApiMessages.ProtoOAExecutionEvent,
            order: OpenApiModelMessages.ProtoOAOrder,
    ) -> CloseOrigin:
        """Classify a closing fill on a run-owned position.

        The venue's own executions are checked first, so they never consume a
        pending own-close record.
        """
        if (order.orderType == OpenApiModelMessages.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT
                or order.isStopOut or message.isServerEvent):
            return CloseOrigin.VENUE
        terminal = message.executionType == OpenApiModelMessages.ProtoOAExecutionType.ORDER_FILLED
        if self._claim_own_close(order.positionId, order.orderId, terminal=terminal):
            return CloseOrigin.OWN
        if self.store_ctx is not None:
            if order.clientOrderId and self.store_ctx.get_order(order.clientOrderId) is not None:
                return CloseOrigin.OWN
            if order.orderId and self.store_ctx.find_by_ref(
                    'order_id', str(order.orderId)) is not None:
                return CloseOrigin.OWN
        return CloseOrigin.EXTERNAL

    def _defer_external_close(
            self, message: OpenApiMessages.ProtoOAExecutionEvent,
            order: OpenApiModelMessages.ProtoOAOrder, *,
            from_entry: str | None, fill_qty: float,
    ) -> None:
        """Queue the report of a manual close, once per position.

        Reported by :meth:`_flush_external_interventions` once the stream has
        yielded the fill event, so the event reaches the engine's queue before
        a halting policy raises; the engine books it before the run stops.
        """
        self._queue_external_close(order.positionId, {
            'position_id': order.positionId,
            'order_id': order.orderId,
            'deal_id': message.deal.dealId if message.HasField('deal') else None,
            'closed_qty': fill_qty,
            'order_type': int(order.orderType),
            'from_entry': from_entry,
        })

    def _queue_external_close(self, position_id: int, context: dict) -> None:
        """Queue one manual-close report, unless the position already had one."""
        if position_id in self._external_close_reported:
            return
        self._external_close_reported.add(position_id)
        reason = (
            f"bot position {position_id} was closed on the venue by someone else: "
            f"{context['closed_qty']} units by order {context['order_id']}"
        )
        logger.warning("cTrader: %s", reason)
        coid = self._protection_row(position_id)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _CLOSE_REPORT_PENDING_EVENT, client_order_id=coid,
                exchange_order_id=str(position_id),
                payload={'position_id': position_id, 'reason': reason, 'context': context},
            )
        self._deferred_interventions.append((reason, context, coid, str(position_id)))

    def _mark_close_judged(self, deal_ids: list[int]) -> None:
        """Closing deals whose origin is decided; never judged again by any process."""
        fresh = [deal_id for deal_id in deal_ids if deal_id not in self._judged_close_deals]
        if not fresh:
            return
        self._judged_close_deals.update(fresh)
        if self.store_ctx is not None:
            self.store_ctx.log_event(_CLOSE_JUDGED_EVENT, payload={'deal_ids': fresh})

    async def _judge_recovered_closes(
            self, position_id: int, closing: tuple[tuple[int, int, int, int], ...], *,
            from_entry: str | None,
    ) -> bool:
        """Apply the intervention policy to closes the PUSH stream missed.

        Called when the deal history retires a run-owned position, before the
        closing deals are put on the fill de-dup channel. Every closing deal
        no process of the run attributed or judged yet is classified; an
        external one queues a report like a manual close seen live.

        :param position_id: The run-owned position the deals closed.
        :param closing: ``(deal_id, order_id, executed_ms, closed_cents)`` of
            every deal that closed volume on it.
        :param from_entry: The Pine entry the position belongs to.
        :return: ``False`` when an order lookup failed for now: nothing was
            judged, and the caller must not retire the position this pass.
        """
        if position_id in self._external_close_reported:
            return True
        self._load_close_evidence()
        pending = [deal for deal in closing if deal[0] not in self._judged_close_deals]
        origins: dict[int, CloseOrigin] = {}
        for _deal_id, order_id, executed_ms, _cents in pending:
            if order_id in origins:
                continue
            origin = await self._recovered_close_origin(position_id, order_id, executed_ms)
            if origin is None:
                return False
            origins[order_id] = origin
        external = [deal for deal in pending if origins[deal[1]] is CloseOrigin.EXTERNAL]
        if external:
            self._queue_external_close(position_id, {
                'position_id': position_id,
                'order_id': external[0][1],
                'deal_id': external[0][0],
                'deal_ids': [deal[0] for deal in external],
                'closed_qty': volume_to_units(sum(deal[3] for deal in external)),
                'from_entry': from_entry,
                'recovered': True,
            })
        self._mark_close_judged([deal[0] for deal in pending])
        return True

    async def _recovered_close_origin(
            self, position_id: int, order_id: int, executed_ms: int,
    ) -> CloseOrigin | None:
        """Classify one closing order found in the deal history.

        :return: The origin, or ``None`` when the order lookup failed for now.
        """
        if any(record.claims_recovered(position_id, order_id, executed_ms)
               for record in self._own_close_history):
            return CloseOrigin.OWN
        store = self.store_ctx
        if store is not None and store.find_by_ref('order_id', str(order_id)) is not None:
            return CloseOrigin.OWN
        if self._live_account_id is None:
            return None
        try:
            async with asyncio.timeout_at(self._reconcile_pass_deadline):
                res = cast(OpenApiMessages.ProtoOAOrderDetailsRes, await self._account_request(
                    OpenApiMessages.ProtoOAOrderDetailsReq(
                        ctidTraderAccountId=self._live_account_id, orderId=order_id)))
        except CTraderProtocolError as exc:
            if is_not_found(exc.error_code):
                logger.warning(
                    "cTrader: order %d that closed bot position %d is not found on the "
                    "venue (%s); its origin is unknown, not reported",
                    order_id, position_id, exc)
                return CloseOrigin.UNKNOWN
            if order_id not in self._close_lookup_warned:
                self._close_lookup_warned.add(order_id)
                logger.warning(
                    "cTrader: looking up order %d that closed bot position %d failed "
                    "(%s); the position is retired once the lookup succeeds",
                    order_id, position_id, exc)
            return None
        except (BrokerError, CTraderWireError, TimeoutError) as exc:
            logger.debug("cTrader closing-order lookup for %d deferred: %s", order_id, exc)
            return None
        order = res.order
        if (order.orderType == OpenApiModelMessages.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT
                or order.isStopOut):
            return CloseOrigin.VENUE
        if (store is not None and order.clientOrderId
                and store.get_order(order.clientOrderId) is not None):
            return CloseOrigin.OWN
        return CloseOrigin.EXTERNAL

    def _flush_external_interventions(self) -> None:
        """Report the queued manual closes, and those an earlier process left unhandled.

        Each report is journalled as handled once the policy ran, the halt
        included.

        :raises UnexpectedCancelError: When the policy requires the halt.
        """
        self._load_close_evidence()
        while self._deferred_interventions:
            reason, context, coid, position_ref = self._deferred_interventions.pop(0)
            try:
                self.report_external_intervention(
                    reason, context, client_order_id=coid, exchange_order_id=position_ref)
            except UnexpectedCancelError:
                self._journal_close_report_done(context)
                raise
            self._journal_close_report_done(context)

    def ensure_venue_writes_allowed(self, what: str = "venue write") -> None:
        """Refuse a venue write while the quarantine or an unhandled replayed report forbids it.

        A manual-close report an earlier process queued but never handled is
        handled by the order-event stream, the halt included; until then the
        intervention it reports may require the quarantine, so no write leaves.

        :raises VenueWritesQuarantinedError: While a replayed report is not
            handled yet, or the venue-write quarantine is latched.
        """
        self._load_close_evidence()
        if self._unhandled_close_replays:
            raise VenueWritesQuarantinedError(
                f"{what} (manual-close report of an earlier process not handled yet)")
        super().ensure_venue_writes_allowed(what)

    def _journal_close_report_done(self, context: dict) -> None:
        """Persist that a manual-close report was handled; never replayed again."""
        position_id = context.get('position_id')
        if isinstance(position_id, int):
            self._unhandled_close_replays.discard(position_id)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _CLOSE_REPORT_DONE_EVENT, exchange_order_id=str(context.get('position_id')),
                payload={'position_id': context.get('position_id')})
