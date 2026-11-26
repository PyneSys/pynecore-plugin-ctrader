"""External-intervention detection mix-in for the cTrader Open API plugin.

Someone other than the bot — the operator in a cTrader UI, another bot on the
account — can change what belongs to the bot on the venue: its protective
levels, its pending orders, its share of a position. Every such change latches
the external-intervention quarantine through
:meth:`~pynecore.core.plugin.broker.BrokerPlugin.report_external_intervention`;
the bot's own writes, the venue's own mechanics and another party's separate
activity that leaves the bot's things alone never may. Where the evidence
cannot establish that someone else changed the bot's state, the plugin books
the venue truth, logs an ERROR and writes a durable audit event instead.

**Protection levels.** cTrader protection is a set of position attributes
(``stopLoss`` / ``takeProfit`` / ``trailingStopLoss``) that one
``ProtoOAAmendPositionSLTPReq`` overwrites wholesale. Every such amend the
bracket paths send passes through :meth:`_ProtectionMixin._write_protection`,
which records the levels in a core
:class:`~pynecore.core.broker.protection_guard.ProtectionLevelGuard` keyed by
the position; the venue's view — a position pushed on a non-fill execution
event, or the periodic reconcile snapshot — is judged against them. A trailing
stop moves by itself, so under trailing only the flag and the take-profit are
compared. Two writers keep the position out of the guard: the native
fail-safe (the core fail-safe manager judges its levels and quarantines on an
external edit itself) and a trailing stop without an anchor (the venue seeds
its stop). The bot sends no protection with its orders, so its own fill that
opens or adds volume does not explain a protection change: when the venue
showed the same levels before the fill, the fill's view is judged like any
other, and a change someone made before the fill stays an edit. A fill that
closed volume (a reduce or a reversal on a NETTING account) may have made the
venue change the protection (not measured); its view, a view a reconcile pass
read after an own fill, and a fill view that is the first to show levels are
accepted next to the levels already accepted as an own write whose outcome is
unknown, never instead of them: seen on the venue it is the bot's protection,
two snapshots of another own level drop it, since the view may predate a newer
own write. Levels the bot never set get an ERROR and an audit event
(:meth:`_ProtectionMixin._judge_own_fill_view`).

**Bare positions.** A position the bot left without protection — it cleared
the bracket, or its own fill opened or extended the position without one —
carries the bot's choice of no protection. The guard holds empty levels for it
(:meth:`_ProtectionMixin._guard_bare`), on a HEDGED and a NETTING account
alike, so a stop-loss, take-profit or trailing stop someone else adds to it is
an edit like a move or a removal. The levels the bot set before stay
acceptable until the venue is seen without them, so a late view of what the
bot itself just cleared is never taken for an edit; that includes the
protection a clear of a position the guard did not hold removed, as the
clear's own snapshot read showed it (an earlier process may have written it
outside the guard). Such a clear is an own write in flight until it is
answered: an own fill's view meanwhile is not taken as the position's
protection. The bot's own later writes are recorded as usual. Likewise,
protection the bot's own fill finds on a position the guard does not hold (on
a NETTING account, its entry merged into a position someone else protected)
covers the bot's position from then on: the guard holds it as the bot's, so
changing it is an edit (:meth:`_ProtectionMixin._take_own_fill_protection`).

**Fired protection.** A position's own SL / TP firing is venue mechanics and is
booked as such, but the protection that fired must be the bot's. Every price the
fired ``STOP_LOSS_TAKE_PROFIT`` order reports (``stopPrice`` / ``limitPrice``;
the closed position's own ``stopLoss`` / ``takeProfit`` when the order reports
neither) is compared with every level the bot set on the position while it
held a share of it, as a stop or as a take-profit alike: which field of a fill
push carries which level is not measured, so the field is not trusted. Under
an own trailing stop one price may be where the stop trailed to. A price the
bot never set fired, so someone else moved or added that level. The same holds
for a position the bot left bare (it cleared the bracket, or its fill — pushed
or recovered by the reconcile pass — opened it without one): any protection
firing on it is someone else's. The bare mark is kept on the position's journal
row, so it survives a restart. A close recovered from the deal history is
judged the same way from its order's details.

A position the bot handed to a writer outside the guard (the native fail-safe,
a trailing stop without an anchor) is never bare, whatever an own fill's view of
it shows: that view can predate the write. Its fired prices are judged against
the levels those writes sent once one of them landed in this process; before
that the venue may still carry protection this process never wrote, and
nothing is judged. Those levels are kept in memory: a restarted process judges
from its first landed write on. A position the bot holds no share of any more
carries none of its protection either: its protection is released and nothing
firing on it or changing on it is judged. After an operator resume the venue's
protection as it stood is accepted; a protection firing before a venue view
became the new baseline (and before a write of the bot's was confirmed) is
accepted too, with a WARNING and an audit. A bare mark set before the resume is
dropped from the journal row by the process that applied it, and the position
is marked again when the venue still shows it without any protection.

**Closing fills.** A closing fill on a position the run owns is classified:

- *venue* — the position's own SL / TP order (``STOP_LOSS_TAKE_PROFIT``), a
  stop-out (``isStopOut``) or any other server-initiated execution
  (``isServerEvent``; that last one is booked with a WARNING and an audit);
- *own* — a close this run sent (:class:`_OwnClose`, matched by the close
  order id at any age), or an order carrying one of this run's
  client-order-ids; while a close of the run is outstanding or its outcome is
  unknown, any closing fill on its position is taken as that close's
  (:class:`_ClaimedFill`) until the venue's answer names the close order;
- *external* — anything else. The fill event is yielded first and the
  report follows it.

Only the bot's *share* of a position (:meth:`_ProtectionMixin._own_share`, the
signed net of the exposure its journalled fills built, on the side of that net)
is the bot's: a close is booked — and an external one reported — only for the
part that took the venue below that share, taken as of the deal's execution
(:meth:`_ProtectionMixin._share_bounds`); a close of another party's own
exposure is ignored and audited. On a NETTING account several parties share the
one position; on a HEDGED account the share is the whole position unless
another client's order added to it, so there the rule changes nothing for the
bot's own legs. When the share is not known (no live entry row of the run links
the position: a startup-adopted position the run closes through its dispatch
record), a HEDGED position still has one owner and an external close is booked
and reported in full; on a NETTING account nothing proves the close took the
bot's part, so it is booked as the venue shows it, with an ERROR and an audit
event, never reported. A fill an own close took before its order id was known
is booked for at most the volume the close asked for, and its event is held
until the close is answered. Once the answer names the close order, or the
venue refused the close, the fills taken are judged again in the order they
were taken, each against the share of its moment — less the answer's own fill
when that executed first but is translated later; one that shows to be another
order's is reported only for the part of the share it took. What it booked
beyond that was never the bot's close (another party closed its own
exposure): its held event is delivered without that volume and the journal
gets it back, with a WARNING and an audit event, so the engine and the journal
end with what the venue did — whether the close fills later, is cancelled or
was refused. A close whose outcome stays unknown delivers the held events as
they were booked.

``ProtoOAClosePositionReq`` carries no client-order-id or label, so the own
close can only be recognised from this run's dispatch record.

**Recovered closes.** A close whose PUSH was missed (the stream was down, or the
process was down) surfaces only as deal-history evidence. Each closing deal no
process of the run attributed or judged yet is judged by
:meth:`_ProtectionMixin._judge_recovered_closes`: *own* when its order is a
close the run sent (by order id, or by position and time while the close's
outcome is unknown and the order lookup attributes the order to no one) or one
of this run's orders; *venue* when the order
(``ProtoOAOrderDetailsReq``) is the position's ``STOP_LOSS_TAKE_PROFIT`` order
or a stop-out, unless that protection order fired at a price the bot never set
or on a position it left bare (*external*, as on the live path); *external*
when the order carries another client's client-order-id. The execution event's
``isServerEvent`` flag is not part of the order, so any other close — another
client's or a server-initiated one — is *unattributed*: booked with an ERROR
and an audit event, never reported. When the bot's share is known the deals
are replayed in execution order (:func:`_replay_share_closes`) and only the
deals that took the venue below the bot's share count; what the deals took of
the share is what a shrunk position's journal retires. When the reconcile pass
finds a run-owned position gone, each deal that took part of the share reaches
the engine as a close fill of the entry for that part, whatever its origin — the
fill the PUSH stream would have delivered — so the engine books the exit instead
of reading the flat venue as an unexplained flatten. The run's own closes and
the closing deals already judged, with their origin, are journalled, so a
restarted process judges with the evidence of the earlier ones. A lookup that
failed for now defers the verdict to the next pass; an order the venue reports
as not found is never reported. A close of a row whose disappearance an
operator resume accepted, or a shrink a restart right after the resume found,
is accepted with a WARNING and an audit event: the operator took the venue as
it stands.

**Venue cancels.** Only the venue expires an order, and the bot sends every
order without an expiry; an order the venue flags as cancelled on its own
initiative (``isServerEvent``) or rejected was ended by the venue too. Such a
cancel of one of the bot's orders — pushed, or found by the disappearance
re-check through ``ProtoOAOrderDetailsReq`` — carries a venue-driven
``cancel_reason`` (:meth:`_ProtectionMixin._venue_cancel_reason`), so the
unexpected-cancel policy never runs for it. An expiry the bot never set on its
order is someone else's edit: that cancel stays unexplained. The re-check
cannot see ``isServerEvent``, which only the push carries: an order whose
status reads cancelled, or that the venue no longer knows, may have been
cancelled by the venue's own logic as well as by someone else, so it is
retired with an ERROR and an audit event and no policy
(:meth:`_ProtectionMixin._unattributed_cancel_reason`). An order the re-check
finds executed is no cancel, and one it finds accepted is still working.

**Pending orders.** The terms the bot sends for a working LIMIT / STOP entry
(price, volume) are recorded on its row before each send; the order's other
attributes (stop-loss, take-profit, expiry, time in force) must be the defaults
the bot sends or what the venue's answer to its own placement or amend showed.
An ``ORDER_REPLACED`` push of the order whose terms are none of those is
someone else's edit and is reported at once; one pushed while an own amend of
the order is in flight is kept and judged once the amend is answered. The
reconcile snapshot's view of each working entry is judged the same way, after
two consecutive passes agree, so an edit made while the stream was down is
caught too; in a process that applied an operator resume the first snapshot
view of each working entry that existed at the resume is the venue the
operator accepted, taken as the bot's own with a WARNING and an audit. Whether
the venue reports a partly filled order's whole volume or its remainder is not
measured, so either matches.

**Report delivery.** A queued manual-close report is journalled under its own
key before its deals are marked judged, and marked handled under that key once
the policy ran; a report an earlier process queued but never handled is
replayed by the next one, as soon as its order-event stream starts. No venue
write leaves while a queued report waits for its delivery.
"""
import asyncio
import logging
import math
import sqlite3
import time
import uuid
from abc import ABC
from collections.abc import Callable, Container, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field, fields, is_dataclass, replace
from enum import StrEnum
from time import time as epoch_time
from typing import TYPE_CHECKING, cast

from pynecore.core.broker.disappearance import (
    MISSING_ACCEPTED_BY_RESUME_EXTRA,
    MISSING_PENDING_EXTRA,
)
from pynecore.core.broker.exceptions import (
    BrokerError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    JournalWriteError,
    UnexpectedCancelError,
    VenueWritesQuarantinedError,
)
from pynecore.core.broker.models import (
    CANCEL_REASON_VENUE_REJECTED,
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
)
from pynecore.core.broker.protection_guard import (
    PROTECTION_LEVELS_EXTRA,
    ProtectionLevelGuard,
    ProtectionLevels,
)
from pynecore.core.broker.store_helpers import (
    CLOSE_ENDED_PARTIALLY_EVENT,
    CLOSE_ENDED_UNFILLED_EVENT,
    CLOSE_EXECUTED_QTY_KEY,
    STATE_CANCEL_PENDING,
    STATE_CLOSING,
    STATE_CONFIRMED,
    STATE_DISPOSITION_UNKNOWN,
    STATE_SERVER_REF_SEEN,
    mark_rejected,
)
from pynecore.types.strategy import (
    ADOPTED_STARTUP_EXTRA_KEY,
    JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY,
)

from ._base import _CTraderBase
from .exceptions import CTraderProtocolError, is_not_found
from .helpers import parse_protocol_id, raw_volume, volume_to_units
from .messages import OpenApiMessages_pb2 as OpenApiMessages
from .messages import OpenApiModelMessages_pb2 as OpenApiModelMessages
from .wire import CTraderWireError

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow, RunContext

logger = logging.getLogger(__name__)

#: Seconds a close this run sent keeps claiming closing fills on its position
#: after the venue answered (or the answer was lost). A market close fills
#: within seconds; the margin covers a fill push delayed by a reconnect.
_OWN_CLOSE_GRACE_S = 300.0

#: Margin (ms) between this host's wall clock and the venue's deal timestamps,
#: applied in the direction that claims more closes as the run's own.
_CLOCK_SKEW_MS = 60_000

#: Volume tolerance in units: half of the venue's centi-unit resolution.
_VOLUME_TOL = 0.005

#: Seconds an own fill counted into the journal ahead of its event is kept for
#: ordering the share; the reconcile pass holds queued pushes for at most its
#: 30 s budget, so the event is processed well within this.
_SHARE_EFFECT_TTL_S = 60.0

#: Journal event of one close the run sent, written at each state change.
_OWN_CLOSE_EVENT = 'ctrader_own_close'

#: ``orders.state`` of the journal row of a close
#: :meth:`~_ExecutionMixin.execute_close` sends, written under the close's
#: client order id in the transaction that journals the close record, before
#: the request goes out. The row is live while the close's outcome is open,
#: and it ends with what the venue evidence proves (:meth:`_ProtectionMixin._finish_close_row`):
#: ``confirmed`` with a ``confirmed`` journal event once the close's own fills
#: cover its whole volume or its order's final status shows it executed,
#: ``confirmed`` with the core's ended-partially mark
#: (:data:`~pynecore.core.broker.store_helpers.CLOSE_ENDED_PARTIALLY_EVENT`) once
#: the venue ended its order after a part, ``rejected`` once the venue refused it,
#: closed in this state with the core's ended-unfilled mark
#: (:data:`~pynecore.core.broker.store_helpers.CLOSE_ENDED_UNFILLED_EVENT`) once
#: the venue ended its order with nothing executed, and closed in this state,
#: with a :data:`_CLOSE_UNRESOLVED_EVENT`, when no evidence shows whether it
#: executed and none will. The row holds no venue order id,
#: position link, fill or Pine identity, so no journal walk takes it for an
#: order or a position of the bot's.
_CLOSE_ROW_STATE = 'ctrader_close'

#: Extras key of a close row: ``{'position_id', 'volume_cents'}``, and once it
#: ended, ``'outcome'`` with ``'order_id'`` and ``'executed'`` (units) or
#: ``'reason'``.
_CLOSE_ROW_EXTRA = 'ctrader_close'

#: Audit of a close row ended with no evidence of whether the close executed.
_CLOSE_UNRESOLVED_EVENT = 'ctrader_close_unresolved'

#: Journal event naming closing deals whose origin was decided.
_CLOSE_JUDGED_EVENT = 'ctrader_close_judged'

#: The state a close judgment and the writes applying it (a debit, a
#: retirement, close coverage, an adopted-leg takeover) change in this process
#: beside the journal (:meth:`~_ProtectionMixin._judgment_span`). A rollback
#: puts its contents back in place, so every object in it, and every holder of
#: one outside it, keeps its identity.
_JUDGMENT_STATE = (
    '_seen_deal_ids', '_own_closes', '_own_close_history', '_judged_close_deals',
    '_judged_close_origins', '_external_close_reported', '_unhandled_close_reports',
    '_deferred_interventions', '_ambiguous_closes', '_share_effects',
    '_held_close_verdicts', '_shrink_watch', '_shrink_judged', '_shrink_seen_since',
    '_unexplained_shrinks', '_shrink_covered_from', '_closes_covered_until',
    '_resume_accepted_until', '_expiry_overlaps', '_coverage_journalled',
    '_startup_shrinks', '_resume_accepted_positions', '_shared_position_logged',
    '_inherited_deliveries',
)

#: Journal event of one fill the run journaled before the engine took it, keyed
#: by ``fill:<fill id>`` (:meth:`~_ProtectionMixin._journal_fill_delivery`):
#: written in the transaction of the journal write that puts the fill into the
#: position a restart starts from, and kept until the engine's receipt shows the
#: fill taken (:meth:`~pynecore.core.broker.storage.RunContext.fill_delivered`).
#: A later process of the run delivers a fill it holds no receipt of again,
#: marked journaled before the restart.
_FILL_DELIVERY_EVENT = 'ctrader_fill_delivery'

#: Journal event of one commission delivered without its fill (an
#: ``event_type`` ``'fee'`` event), keyed by ``fee:<fill id>``
#: (:meth:`~_ProtectionMixin._journal_fee_delivery`): kept until the engine's
#: receipt of the fill accounts the whole commission.
_FEE_DELIVERY_EVENT = 'ctrader_fee_delivery'

#: Journal event of one queued manual-close report, written before its deals
#: are marked judged, so a report a dead process never delivered is replayed.
_CLOSE_REPORT_PENDING_EVENT = 'ctrader_close_report_pending'

#: Journal event of a manual-close report the intervention policy handled.
_CLOSE_REPORT_DONE_EVENT = 'ctrader_close_report_done'

#: Audit of a closing fill taken as the bot's own close while that close's
#: outcome is unknown: it may be someone else's.
_CLAIM_AMBIGUOUS_EVENT = 'ctrader_close_claim_ambiguous'

#: Audit of a NETTING close whose effect on the bot's share cannot be decided.
_SHARE_AMBIGUOUS_EVENT = 'ctrader_close_share_ambiguous'

#: Audit of a server-initiated close that is neither the position's SL / TP
#: nor a stop-out (a broker-side action).
_SERVER_CLOSE_EVENT = 'ctrader_server_close'

#: Audit of a protection firing whose levels the venue did not report, after a
#: venue view of the position carried levels the bot did not set.
_PROTECTION_FIRED_UNCONFIRMED_EVENT = 'ctrader_protection_fired_unconfirmed'

#: Audit of a protection that fired after an operator resume, before the
#: venue's accepted protection was taken as the baseline.
_PROTECTION_FIRED_AFTER_RESUME_EVENT = 'ctrader_protection_fired_after_resume'

#: Audit of protection the bot never set that a view of its own fill was the
#: first to show (or that a fill closing volume left, or that a HEDGED entry's
#: new position carried): accepted, since whether the fill, an earlier process
#: or someone else set it cannot be told.
_FILL_PROTECTION_UNEXPLAINED_EVENT = 'ctrader_fill_protection_unexplained'

#: Audit of a working entry order that left the venue without a fill whose
#: end the evidence cannot attribute (a cancel the order's status cannot tell
#: from a server-initiated one, or an order the venue no longer knows).
_CANCEL_UNATTRIBUTED_EVENT = 'ctrader_cancel_unattributed'

#: Audit of a recovered close of the bot's position nothing proves someone
#: else's: it may be a server-initiated close.
_CLOSE_UNATTRIBUTED_EVENT = 'ctrader_close_unattributed'

#: Audit of a close of the bot's position an operator resume accepted.
_CLOSE_ACCEPTED_BY_RESUME_EVENT = 'ctrader_close_accepted_by_resume'

#: Audit of an entry row a reconcile pass or the startup recovery retired
#: because its order filled and the position it opened closed while the stream
#: was down, written with the retirement under the row's client order id.
_FILLED_THEN_CLOSED_EVENT = 'reconcile_filled_then_closed_retired'

#: Audit of volume a fill taken for the bot's own close booked although the
#: bot's share did not lose it: taken off the fill's held event and given back
#: to the journal.
_CLAIM_REBOOKED_EVENT = 'ctrader_close_claim_rebooked'

#: Audit of a NETTING close of a position whose bot share is not known: it is
#: booked as the venue shows it, nothing proves it took the bot's part.
_SHARE_UNKNOWN_EVENT = 'ctrader_close_share_unknown'

#: Journal-row extras key marking a position the bot left without protection:
#: ``{'key': <guard key>, 'at_ms': <wall-clock ms of the mark>}``.
_BARE_PROTECTION_EXTRA = 'protection_bare'

#: Journal-row extras key holding an entry row's place in the fill order: the
#: sequence number the row got when :meth:`~_EventStreamMixin.watch_orders`
#: delivered its first fill, or the first fill after a close spent all of its
#: exposure. The core binds an entry order the same way (one binding per order,
#: appended on its first fill, a new one once a close spent it), so sorting by
#: it walks the rows in the order the core's binding book settles them
#: (:func:`_in_settlement_order`).
_FILL_SEQ_EXTRA = 'entry_fill_seq'

#: Audit of a working entry's terms the bot did not send, taken as its own
#: because an operator resume accepted the venue as it stood.
_ORDER_ACCEPTED_BY_RESUME_EVENT = 'ctrader_order_accepted_by_resume'

#: Journal-row extras key holding every ``[limit, stop, volume]`` the bot sent
#: for its working entry order, extended to ``[limit, stop, volume,
#: view_volume, view_executed]`` once the venue answered the write: the
#: order's ``tradeData.volume`` and ``executedVolume`` as the answer showed them.
_OWN_ORDER_TERMS_EXTRA = 'own_order_terms'

#: Journal-row extras key holding every attribute list (:func:`_order_attrs`)
#: the venue's answer to the bot's own placement or amend showed.
_OWN_ORDER_ATTRS_EXTRA = 'own_order_attrs'

#: Audit of attributes of a working entry the venue's answer to an own amend
#: was the first view to show: taken as the bot's, nothing proves they are.
_ORDER_ATTRS_UNEXPLAINED_EVENT = 'ctrader_order_attrs_unexplained'

#: Journal-row extras key holding the ``operator_resume_ts_ms`` of the resume
#: whose baseline view of the row's working entry order was taken: a process
#: applying the same resume again judges the order against that baseline.
_ORDER_RESUME_BASELINE_EXTRA = 'order_resume_baseline'

#: Consecutive reconcile snapshots that must show the same foreign terms of a
#: working entry before they are reported.
_ORDER_TERMS_CONFIRMATIONS = 2

_GOOD_TILL_CANCEL = OpenApiModelMessages.ProtoOATimeInForce.GOOD_TILL_CANCEL

#: The attributes of a working entry order the bot never sets
#: (:func:`_order_attrs`): no stop-loss, take-profit or expiry, good till
#: cancelled.
_DEFAULT_ORDER_ATTRS: list[float | int] = [0.0, 0.0, 0, 0, 0, int(_GOOD_TILL_CANCEL)]

#: ``orders.state`` values of a real ENTRY dispatch row (as opposed to the
#: book-keeping markers — bracket ownership, close legs, stop-limit watches —
#: that carry no exchange order).
_ENTRY_ROW_STATES = frozenset({
    STATE_CONFIRMED,
    STATE_CLOSING,
    STATE_CANCEL_PENDING,
    STATE_SERVER_REF_SEEN,
    STATE_DISPOSITION_UNKNOWN,
})

_STOP_LOSS_TAKE_PROFIT = OpenApiModelMessages.ProtoOAOrderType.STOP_LOSS_TAKE_PROFIT

#: Put on the execution queue when held close fills are released, so the
#: order-event stream wakes and delivers them; translates to nothing.
_RELEASE_WAKEUP = object()


class CloseOrigin(StrEnum):
    """Who caused a closing fill on a run-owned position."""
    OWN = 'own'
    VENUE = 'venue'
    EXTERNAL = 'external'
    #: Neither the run's nor the venue's own SL / TP or stop-out, but nothing
    #: proves another client sent it either (a server-initiated close looks
    #: the same in the order history); never reported.
    UNATTRIBUTED = 'unattributed'
    #: The venue reports the closing order as not found; never reported, but
    #: a recovered close of it that took the bot's share is logged as an
    #: ERROR and audited like an unattributed one.
    UNKNOWN = 'unknown'


#: Origins of a close that is not proven the bot's and takes the bot's share
#: only where it left the venue below that share.
_FOREIGN_ORIGINS = frozenset({
    CloseOrigin.EXTERNAL, CloseOrigin.UNATTRIBUTED, CloseOrigin.UNKNOWN})


class FiredProtection(StrEnum):
    """Verdict on a position SL / TP that fired."""
    #: The bot's own level, or nothing the bot could judge it against.
    OWN = 'own'
    #: A price the bot never set on the position fired.
    FOREIGN = 'foreign'
    #: A protection fired on a position the bot left without one.
    BARE = 'bare'
    #: The fired order reports no level, after a venue view of the position
    #: carried levels the bot did not set: cannot be proven foreign.
    UNCONFIRMED = 'unconfirmed'
    #: Fired before the protection an operator resume accepted was replaced.
    AFTER_RESUME = 'after_resume'


@dataclass(slots=True)
class _ClaimedFill:
    """A closing fill judged while an own close of its position had no order id yet.

    Either taken as the close's own (until the answer names the close order),
    or — ``venue`` — the venue's own close of the position, kept so the answer
    can judge again what it took of the share.

    :ivar order_id: The order that filled.
    :ivar deal_id: The fill's deal.
    :ivar fill_qty: The closed quantity (units).
    :ivar order_type: The filled order's ``ProtoOAOrderType``.
    :ivar client_order_id: The filled order's client-order-id.
    :ivar from_entry: The Pine entry the position belongs to.
    :ivar terminal: Whether the fill completed its order.
    :ivar executed_ms: The fill's execution time (venue clock, ms), when known.
    :ivar share_before: The lower bound of the bot's journal share before the
        fill; ``None`` when the share is not known.
    :ivar share_high: The upper bound of that share, which capped the
        booking; ``None`` like ``share_before``.
    :ivar venue_after: The position volume the fill left on the bot's side.
    :ivar booked: The volume booked: as the bot's own close, or for a venue
        close, as what it took of the share.
    :ivar venue: Whether the fill is the venue's own close (SL / TP, stop-out,
        server-initiated), never the bot's close nor reported.
    :ivar event: The fill's engine event, held while the close is in flight
        (:meth:`_ProtectionMixin._release_held_claims`); ``None`` once
        delivered, or when nothing was booked.
    :ivar settles_share: The fill left the position open, so the bot's share
        is checked once the event is delivered (a share it no longer holds
        vacates the position).
    :ivar retired: The journal debits the fill's booking took, as
        ``(client_order_id, volume)`` in the order taken
        (:meth:`_ProtectionMixin._retire_partial_close_exposure`).
    :ivar settle_entry: The Pine entry those debits were taken for first
        (:meth:`_ProtectionMixin._settlement_entry`).
    """
    order_id: int
    deal_id: int | None
    fill_qty: float
    order_type: int
    client_order_id: str
    from_entry: str | None
    terminal: bool
    executed_ms: int | None
    share_before: float | None
    share_high: float | None
    venue_after: float
    booked: float = 0.0
    venue: bool = False
    event: OrderEvent | None = None
    settles_share: bool = False
    retired: list[tuple[str, float]] = field(default_factory=list)
    settle_entry: str | None = None


@dataclass(frozen=True, slots=True)
class _AnswerFill:
    """The fill an own close's answer carried (a correlated ``ORDER_FILLED``).

    :ivar deal_id: The fill's deal.
    :ivar executed_ms: Its execution time (venue clock, ms), when known.
    :ivar closed: The position volume (units) it closed.
    """
    deal_id: int
    executed_ms: int | None
    closed: float


@dataclass(slots=True, eq=False)
class _OwnClose:
    """A ``ProtoOAClosePositionReq`` this run sent.

    :ivar position_id: The position the close reduces.
    :ivar sent_ms: Wall-clock time (ms) the close was sent.
    :ivar order_id: The venue's close ``orderId`` once the response named it.
    :ivar expires_at: Monotonic deadline after which the record stops claiming
        fills; infinite while the request is in flight.
    :ivar answered_ms: Wall-clock time (ms) the venue answered or the answer
        was lost; ``None`` while the request is in flight.
    :ivar key: Identifies the close across its journal events.
    :ivar claims: Fills taken for this close before its order id was known.
    :ivar volume: The volume (units) the close asked for; ``None`` when not
        known (a close an earlier process sent).
    :ivar from_entry: The Pine entry the close serves, so its own fills are
        attributed even after the journal closed the position's rows.
    :ivar settle_entry: The Pine entry the core settles the close's fills
        against, so its fills retire that entry's journal rows even after a
        later close of the same position was dispatched.
    :ivar executed: Volume (units) of the close's own fills seen by its order
        id: delivered by the event stream, carried by its answer, taken for it
        before the answer named its order, or found in the deal history.
    :ivar executed_deals: The deals counted in ``executed``.
    :ivar complete: Whether nothing more of the close can execute: its order
        filled for good, or the venue cancelled or rejected it.
    :ivar coid: The client order id the close was sent under, so a restart
        replay asking what that send executed finds its record.
    :ivar volume_cents: The volume (centi-units) the request asked for.
    """
    position_id: int
    sent_ms: int
    order_id: int | None = None
    expires_at: float = math.inf
    answered_ms: int | None = None
    key: str = field(default_factory=lambda: uuid.uuid4().hex)
    claims: list[_ClaimedFill] = field(default_factory=list)
    volume: float | None = None
    from_entry: str | None = None
    settle_entry: str | None = None
    executed: float = 0.0
    executed_deals: set[int] = field(default_factory=set)
    complete: bool = False
    coid: str | None = None
    volume_cents: int | None = None

    def claimed_volume(self) -> float:
        """Volume (units) booked for the fills this close took so far."""
        return sum(claim.booked for claim in self.claims if not claim.venue)

    def count_executed(self, deal_id: int | None, volume: float) -> None:
        """Count one own fill of the close, once per deal."""
        if deal_id is not None:
            if deal_id in self.executed_deals:
                return
            self.executed_deals.add(deal_id)
        self.executed += volume

    def unresolved(self) -> float:
        """Volume (units) of the answered close that may have executed unseen.

        The part of the requested volume no own fill accounts for, while the
        close may still execute or its fills may have been lost; ``0.0`` for
        a close in flight (it takes every closing fill as its own), one of
        unknown volume (an earlier process sent it) and a complete one.
        """
        if self.answered_ms is None or self.volume is None or self.complete:
            return 0.0
        return max(0.0, self.volume - self.executed - self.unfolded_claims())

    def unfolded_claims(self) -> float:
        """Volume (units) of the fills taken for the close not counted in ``executed``.

        The fills taken before the answer named the close's order: the answer
        counts its own ones in :attr:`executed` (``_settle_own_close``), so
        only those of a close whose order is still unknown remain.
        """
        if self.order_id is not None:
            return 0.0
        return sum(claim.fill_qty for claim in self.claims if not claim.venue)

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


@dataclass(slots=True)
class _ShareEffect:
    """An own fill the journal share and its event stream see at different times.

    :ivar position_id: The position the fill executed on.
    :ivar delta: The change (units) the fill makes to the bot's signed net:
        positive buys, negative sells.
    :ivar counted: Whether the journal already counts the fill (an own entry
        answered on its dispatch) or not yet (an own close, retired only once
        its event is translated).
    :ivar executed_ms: The fill's execution time (venue clock, ms), when known.
    :ivar deal_id: The fill's deal, when known.
    :ivar order_id: The fill's order.
    :ivar recorded_at: Monotonic time the effect was recorded.
    """
    position_id: int
    delta: float
    counted: bool
    executed_ms: int | None
    deal_id: int | None
    order_id: int
    recorded_at: float


@dataclass(frozen=True, slots=True)
class _ShareBounds:
    """The bot's share of a position as of one deal's execution.

    :ivar low: Lower bound of the share (units).
    :ivar high: Upper bound of the share (units).
    :ivar side: The side (``'buy'`` / ``'sell'``) of the share, ``None`` when
        it is nil or its side is not decided.
    """
    low: float
    high: float
    side: str | None


def _bounds_of_net(lo: float, hi: float, in_flight: Iterable[float] = ()) -> _ShareBounds:
    """The bot's share from bounds of its signed net (units, buy positive).

    Each own entry in ``in_flight`` may or may not have executed, so it widens
    the net towards its side. The share is the size of the net on its side; a
    net that may lie on either side has a share of at least nothing and no
    decided side.
    """
    for volume in in_flight:
        lo += min(0.0, volume)
        hi += max(0.0, volume)
    if lo >= -_VOLUME_TOL and hi <= _VOLUME_TOL:
        return _ShareBounds(0.0, 0.0, None)
    if lo >= -_VOLUME_TOL:
        return _ShareBounds(max(0.0, lo), hi, 'buy')
    if hi <= _VOLUME_TOL:
        return _ShareBounds(max(0.0, -hi), -lo, 'sell')
    return _ShareBounds(0.0, max(-lo, hi), None)


@dataclass(slots=True)
class _HeldCloseVerdict:
    """Another party's close whose verdict waits for the own entries in flight.

    The close was booked as if none of those entries had executed before it;
    whether it took the bot's share is decided once each entry is answered.

    :ivar position_id: The position the close reduced.
    :ivar deal_id: The close's deal.
    :ivar executed_ms: Its execution time (venue clock, ms), when known.
    :ivar net_low: Lower bound of the bot's signed net (units, buy positive)
        as of the deal, without the entries in flight.
    :ivar net_high: Upper bound of that net.
    :ivar venue_after: The position volume the close left on the bot's side.
    :ivar fill_qty: The closed quantity (units).
    :ivar booked: The volume the close's event booked.
    :ivar context: The report context of the close.
    :ivar waiting: Signed volume of each entry not answered yet that the
        journal did not count when the close was judged, by client-order-id.
    :ivar counted: Volume (units) of each waiting entry the journal already
        counted then (fills pushed ahead of the entry's answer), by
        client-order-id; the answer's cumulative fill beyond it is the part
        that may have executed before the close.
    :ivar applied: Signed volume of the answered entries that executed on the
        position before the close.
    :ivar unknown: Signed volumes of the answered entries whose effect on the
        position before the close stayed unknown.
    """
    position_id: int
    deal_id: int | None
    executed_ms: int | None
    net_low: float
    net_high: float
    venue_after: float
    fill_qty: float
    booked: float
    context: dict
    waiting: dict[str, float]
    counted: dict[str, float] = field(default_factory=dict)
    applied: float = 0.0
    unknown: list[float] = field(default_factory=list)


def _net_side(net: float | None) -> str | None:
    """The side of a signed net, ``None`` when it is nil."""
    if net is None or abs(net) <= _VOLUME_TOL:
        return None
    return 'buy' if net > 0.0 else 'sell'


def _fill_seq(row: 'OrderRow') -> int | None:
    """An entry row's place in the fill order (:data:`_FILL_SEQ_EXTRA`), ``None`` when unset."""
    seq = (row.extras or {}).get(_FILL_SEQ_EXTRA)
    return seq if isinstance(seq, int) and not isinstance(seq, bool) else None


def _in_settlement_order(rows: Iterable['OrderRow'], entry_id: str | None) -> list['OrderRow']:
    """Journal rows in the order the core settles a close against their entries.

    The core's binding book holds one binding per entry order, appended when
    the order's first fill is booked, and settles a close oldest binding
    first; one that names an entry settles that entry's bindings first
    (``BrokerPosition._settle_bindings``). So the rows of ``entry_id`` come
    first, and within each group the rows go by their place in the fill order
    (:data:`_FILL_SEQ_EXTRA`). A row with no place (its fill was never
    delivered as an event, as with a position a startup adopted) follows the
    ones with a place, and such rows keep their journal order among
    themselves.

    :param rows: The journal rows, in journal order.
    :param entry_id: The Pine entry the close is settled against first, or
        ``None`` for a close that names no entry.
    :return: The rows, sorted.
    """
    def key(item: tuple[int, 'OrderRow']) -> tuple[bool, bool, int, int]:
        index, row = item
        seq = _fill_seq(row)
        return (entry_id is not None and row.pine_entry_id != entry_id,
                seq is None, seq if seq is not None else 0, index)

    return [row for _index, row in sorted(enumerate(rows), key=key)]


def _accepted_by_resume(row: 'OrderRow') -> bool:
    """Whether an operator resume accepted a row's disappearance or shrink.

    The resume marks every live row; the mark holds for the row's first
    absence after the resume, or for the absence it was stamped with when the
    operator resumed (:func:`~pynecore.core.broker.disappearance.accept_missing_before_resume`).
    A mark naming a stamp holds only while that stamp does: a row whose stamp
    was cleared came back, and its acceptance is gone.
    """
    extras = row.extras or {}
    accepted = extras.get(MISSING_ACCEPTED_BY_RESUME_EXTRA)
    if accepted is None:
        return False
    since = extras.get(MISSING_PENDING_EXTRA)
    if since is None:
        return not isinstance(accepted, (int, float))
    return accepted == since


@dataclass(frozen=True, slots=True)
class _PositionDeal:
    """One deal that executed volume on a position, from the deal history.

    :ivar deal_id: The deal.
    :ivar order_id: The deal's order.
    :ivar executed_ms: Execution time (venue clock, ms).
    :ivar filled: Executed volume (cents).
    :ivar closed: Part of it that closed position volume (cents).
    :ivar side: The deal's trade side (``'buy'`` / ``'sell'``).
    :ivar price: The deal's execution price.
    :ivar fee: The deal's commission, in account currency.
    """
    deal_id: int
    order_id: int
    executed_ms: int
    filled: int
    closed: int
    side: str
    price: float
    fee: float

    @property
    def opened(self) -> int:
        """Part of the executed volume that opened position volume (cents)."""
        return self.filled - self.closed


class _UnpricedCloseDeferred(Exception):
    """A recovered close judgment that must wait for its closes' prices.

    Raised inside the judgment's store transaction
    (:meth:`~_ProtectionMixin._judgment_span`) so the judgment rolls back:
    a close of the bot's share the deal history lists without
    ``executionPrice`` cannot be delivered as a fill (an unpriced fill books
    nothing), and judging it now would retire the share it took without one.

    :ivar deal_ids: The unpriced deals the judgment would deliver.
    """

    def __init__(self, deal_ids: list[int]) -> None:
        super().__init__(deal_ids)
        self.deal_ids = deal_ids


@dataclass(slots=True)
class _ShareReplay:
    """Verdict of :func:`_replay_share_closes`.

    :ivar reported: ``(deal, cents)`` of every external or unattributed deal
        that took the venue below the bot's share, with the part of the share
        it took.
    :ivar ambiguous: ``(deal, low, high)`` of every external or unattributed
        deal that may have taken the venue below the share; the bounds of the
        part taken.
    :ivar taken: The part of the share (cents) the judged deals of any origin
        provably took.
    :ivar booked: ``(deal, cents)`` of every judged deal of any origin that
        provably took part of the share, with that part, in execution order:
        the closes of the bot's position the deals stand for.
    """
    reported: list[tuple[_PositionDeal, int]] = field(default_factory=list)
    ambiguous: list[tuple[_PositionDeal, int, int]] = field(default_factory=list)
    taken: int = 0
    booked: list[tuple[_PositionDeal, int]] = field(default_factory=list)


def _replay_share_closes(
        deals: Sequence[_PositionDeal], *,
        pending: Mapping[int, CloseOrigin],
        judged: Mapping[int, CloseOrigin],
        own_opening: Callable[[_PositionDeal], bool],
        seen: Container[int],
        venue_now: int,
        share_now: int,
        share_low_now: int | None = None,
) -> _ShareReplay:
    """Replay a position's deals in execution order against the bot's share.

    The venue volume around each deal is rebuilt backwards from the volume the
    position holds now. When that reconstruction starts from an empty position
    and the origin of every closing deal is known, the window holds the whole
    life of the position and the share is replayed exactly from zero: own
    opening volume adds to it, an own close takes its volume from it, any
    other close takes only what it left the venue below the share; an
    external or unattributed one that took part of it is listed. Otherwise
    the share at each pending deal is bounded from the journal share now and
    the deals after it, and only a deal whose lower bound proves the venue went
    below the share is reported. Either way the verdict carries what the
    pending deals of every origin took of the share: exactly, or its proven
    lower bound. An own close that may have executed without its deal in the
    window lowers the share's lower bound (``share_low_now``) and rules out
    the exact replay: the window does not hold the whole life of the
    position then.

    :param deals: Every deal on the position in the window, in execution order.
    :param pending: The origin of every closing deal to judge, by deal id.
    :param judged: The recorded origin of closing deals judged earlier.
    :param own_opening: Whether a deal's opening part is the bot's own entry.
    :param seen: Deal ids whose fill the journal already counts.
    :param venue_now: The volume (cents) the position holds now.
    :param share_now: The bot's journal share (cents) now.
    :param share_low_now: The least the bot's share (cents) may be now: the
        journal share less what the bot's own closes may have executed beyond
        the window's deals; ``share_now`` when ``None``.
    :return: The verdict.
    """
    share_low = share_now if share_low_now is None else min(share_now, share_low_now)
    after: dict[int, int] = {}
    venue = venue_now
    for deal in reversed(deals):
        after[deal.deal_id] = max(0, venue)
        venue += deal.closed - deal.opened
    replay = _ShareReplay()
    exact = share_low == share_now and venue == 0 and all(
        deal.deal_id in pending or deal.deal_id in judged
        for deal in deals if deal.closed > 0)
    if exact:
        share = 0
        for deal in deals:
            if deal.closed > 0:
                origin = pending.get(deal.deal_id) or judged[deal.deal_id]
                remaining = max(0, after[deal.deal_id] - deal.opened)
                if origin is CloseOrigin.OWN:
                    taken = min(deal.closed, share)
                else:
                    taken = max(0, min(deal.closed, share - remaining))
                share -= taken
                if deal.deal_id in pending:
                    replay.taken += taken
                    if taken > 0:
                        replay.booked.append((deal, taken))
                    if origin in _FOREIGN_ORIGINS and taken > 0:
                        replay.reported.append((deal, taken))
            if deal.opened > 0 and own_opening(deal):
                share += deal.opened
        return replay
    taken_high = 0
    taken_low = 0
    for index, deal in enumerate(deals):
        origin = pending.get(deal.deal_id)
        if origin is None:
            continue
        later = deals[index + 1:]
        earlier = deals[:index]
        own_after = sum(d.opened for d in later if d.opened > 0 and own_opening(d))
        seen_own_after = sum(d.opened for d in later
                             if d.opened > 0 and own_opening(d) and d.deal_id in seen)
        judged_after = sum(d.closed for d in later if d.closed > 0 and d.deal_id not in pending)
        unseen_own_before = sum(d.opened for d in earlier
                                if d.opened > 0 and own_opening(d) and d.deal_id not in seen)
        low = max(0, share_low - own_after - taken_high)
        high = max(0, share_now - seen_own_after + judged_after + unseen_own_before - taken_low)
        remaining = max(0, after[deal.deal_id] - deal.opened)
        if origin is CloseOrigin.OWN:
            low_take, high_take = min(deal.closed, low), min(deal.closed, high)
        else:
            low_take = max(0, min(deal.closed, low - remaining))
            high_take = max(0, min(deal.closed, high - remaining))
        taken_high += high_take
        taken_low += low_take
        replay.taken += low_take
        if low_take > 0:
            replay.booked.append((deal, low_take))
        if origin in _FOREIGN_ORIGINS:
            if low_take > 0:
                replay.reported.append((deal, low_take))
            elif high_take > 0:
                replay.ambiguous.append((deal, low_take, high_take))
    return replay


def _foreign_claims_taken(
        claims: Sequence[_ClaimedFill], own_orders: Container[int],
) -> tuple[float, float]:
    """What the fills an own close took that were not its own took of the bot's share.

    The claims are walked in the order they were taken, as an answer naming
    the close's order would judge them (:meth:`_ProtectionMixin._rejudge_claims`):
    a claim of an order in ``own_orders`` is the bot's close, any other one
    took only what it left the venue below the bot's share, which every
    earlier claim had retired by what it booked; what a claim booked beyond
    the share it took is credited. A claim taken when the share was not known
    took all it booked (a HEDGED position has one owner).

    :param claims: The fills the close took, in the order it took them.
    :param own_orders: The orders taken as the bot's own close.
    :return: ``(low, high)``: the part of the share the other orders' fills
        provably took, and the most they may have taken.
    """
    taken_low = taken_high = credit = 0.0
    for claim in claims:
        if not claim.venue and claim.order_id in own_orders:
            if claim.share_high is not None:
                owed = min(claim.fill_qty, claim.share_high + credit)
                credit -= max(0.0, owed - claim.booked)
            continue
        if claim.share_before is None or claim.share_high is None:
            low = high = claim.booked
        else:
            share_low = max(0.0, claim.share_before + credit)
            share_high = max(0.0, claim.share_high + credit)
            low = min(claim.fill_qty, max(0.0, share_low - claim.venue_after))
            high = min(claim.fill_qty, max(0.0, share_high - claim.venue_after))
        if not claim.venue:
            taken_low += low
            taken_high += high
        credit += claim.booked - low
    return taken_low, taken_high


def _fired_prices_explained(
        own: Sequence[ProtectionLevels], prices: Sequence[float], tolerance: float, *,
        trailing: bool,
) -> bool:
    """Whether every price a fired SL / TP order reported is one the bot set.

    A price counts as the bot's when it equals any stop or take-profit level
    the bot set, whichever order field carried it: which field of a fill push
    carries which level is not measured. Under an own trailing stop in force
    when the protection fired (``trailing``) the venue moves the stop by
    itself, so one price may be where it trailed to.
    """
    known = [level for own_levels in own
             for level in (own_levels.stop, own_levels.limit) if level is not None]
    unexplained = sum(1 for price in prices
                      if not any(abs(price - level) <= tolerance for level in known))
    return unexplained <= (1 if trailing else 0)


def _order_attrs(order: OpenApiModelMessages.ProtoOAOrder) -> list[float | int]:
    """The attributes of a working order the bot never sets, normalized.

    ``[stopLoss, takeProfit, relativeStopLoss, relativeTakeProfit,
    expirationTimestamp, timeInForce]``; an absent field is its default (no
    level, no expiry, good till cancelled), so a view that omits a field and
    one that echoes its default compare equal.
    """
    return [
        order.stopLoss if order.HasField('stopLoss') else 0.0,
        order.takeProfit if order.HasField('takeProfit') else 0.0,
        order.relativeStopLoss if order.HasField('relativeStopLoss') else 0,
        order.relativeTakeProfit if order.HasField('relativeTakeProfit') else 0,
        order.expirationTimestamp if order.HasField('expirationTimestamp') else 0,
        int(order.timeInForce) if order.HasField('timeInForce') else int(_GOOD_TILL_CANCEL),
    ]


def _order_terms(order: OpenApiModelMessages.ProtoOAOrder) -> tuple[float, float, int]:
    """A working order's ``(limitPrice, stopPrice, volume)`` as a view shows them."""
    return order.limitPrice, order.stopPrice, order.tradeData.volume


def _same_price(own: float | None, venue: float | None, tolerance: float) -> bool:
    """Whether two optional prices are the same within ``tolerance``."""
    if own is None or venue is None:
        return own is None and venue is None
    return abs(own - venue) <= tolerance


def _price_tolerance(tick: float | None, *prices: float | None) -> float:
    """Half a price tick, plus a relative float margin."""
    tolerance = 0.5 * tick if tick is not None and tick > 0.0 and math.isfinite(tick) else 0.0
    return tolerance + 1e-12 * max([1.0, *(abs(p) for p in prices if p is not None)])


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


def _nothing_to_amend(exc: BaseException) -> bool:
    """Whether a protection write failed only because the venue already carries it.

    cTrader answers an amend that would leave the position's protection as it
    is with ``INVALID_REQUEST: Nothing to amend.`` (measured live on a clear of
    a position without protection). Only that answer counts: any other
    ``INVALID_REQUEST`` is a real reject.
    """
    cause = exc.__cause__
    return (isinstance(exc, ExchangeOrderRejectedError)
            and isinstance(cause, CTraderProtocolError)
            and cause.error_code == 'INVALID_REQUEST'
            and 'nothing to amend' in str(cause).lower())


def _delivery_payload(event: OrderEvent) -> dict:
    """The journal payload of a delivered event, read back by :func:`_delivered_event`."""
    order = event.order
    return {
        'order': {
            'id': order.id, 'symbol': order.symbol, 'side': order.side,
            'order_type': str(order.order_type), 'qty': order.qty,
            'filled_qty': order.filled_qty, 'remaining_qty': order.remaining_qty,
            'price': order.price, 'stop_price': order.stop_price,
            'average_fill_price': order.average_fill_price, 'status': str(order.status),
            'timestamp': order.timestamp, 'reduce_only': order.reduce_only,
            'client_order_id': order.client_order_id,
        },
        'event_type': event.event_type, 'fill_price': event.fill_price,
        'fill_qty': event.fill_qty, 'timestamp': event.timestamp, 'pine_id': event.pine_id,
        'from_entry': event.from_entry,
        'leg_type': str(event.leg_type) if event.leg_type is not None else None,
        'fee': event.fee, 'fee_currency': event.fee_currency, 'fill_id': event.fill_id,
        'journal_fill_qty': event.journal_fill_qty,
    }


def _delivered_event(payload: Mapping) -> OrderEvent | None:
    """The event a :func:`_delivery_payload` stands for; ``None`` for a malformed one."""
    try:
        order = payload['order']
        leg_type = payload['leg_type']
        return OrderEvent(
            order=ExchangeOrder(
                id=str(order['id']), symbol=str(order['symbol']), side=str(order['side']),
                order_type=OrderType(order['order_type']), qty=float(order['qty']),
                filled_qty=float(order['filled_qty']),
                remaining_qty=float(order['remaining_qty']), price=order['price'],
                stop_price=order['stop_price'],
                average_fill_price=order['average_fill_price'],
                status=OrderStatus(order['status']), timestamp=float(order['timestamp']),
                fee=0.0, fee_currency='', reduce_only=bool(order['reduce_only']),
                client_order_id=order['client_order_id'],
            ),
            event_type=str(payload['event_type']), fill_price=payload['fill_price'],
            fill_qty=payload['fill_qty'], timestamp=float(payload['timestamp']),
            pine_id=payload['pine_id'], from_entry=payload['from_entry'],
            leg_type=LegType(leg_type) if leg_type is not None else None,
            fee=float(payload['fee']), fee_currency=str(payload['fee_currency']),
            fill_id=str(payload['fill_id']), journal_fill_qty=payload['journal_fill_qty'],
        )
    except (KeyError, TypeError, ValueError):
        return None


def _snapshot_contents(roots: Iterable[object]) -> list[tuple[object, object]]:
    """The contents of every mutable object reachable from ``roots``, one level deep.

    Lists, dicts and sets keep their items, dataclass instances their field
    values; the items and values themselves are not copied, only walked in
    turn. Tuples and frozensets are walked but have no contents to keep. Any
    other object is left as it is. :func:`_restore_contents` puts the contents
    back into the same objects, so every holder of one sees it restored.

    :param roots: The objects to walk from.
    :return: ``(object, contents)`` pairs, each reachable object once.
    """
    saved: list[tuple[object, object]] = []
    seen: set[int] = set()
    stack: list[object] = list(roots)
    while stack:
        obj = stack.pop()
        if id(obj) in seen:
            continue
        seen.add(id(obj))
        if isinstance(obj, list):
            items: list[object] = list(obj)
            saved.append((obj, items))
            stack.extend(items)
        elif isinstance(obj, dict):
            entries: dict[object, object] = dict(obj)
            saved.append((obj, entries))
            stack.extend(entries.keys())
            stack.extend(entries.values())
        elif isinstance(obj, set):
            members: set[object] = set(obj)
            saved.append((obj, members))
            stack.extend(members)
        elif isinstance(obj, (tuple, frozenset)):
            stack.extend(cast(Iterable[object], obj))
        elif is_dataclass(obj) and not isinstance(obj, type):
            # PyCharm does not narrow ``obj`` by ``is_dataclass``.
            # noinspection PyDataclass
            values = {f.name: getattr(obj, f.name) for f in fields(obj)}
            saved.append((obj, values))
            stack.extend(values.values())
    return saved


def _restore_contents(saved: list[tuple[object, object]]) -> None:
    """Put the contents :func:`_snapshot_contents` kept back into their objects."""
    for obj, contents in saved:
        if isinstance(obj, list):
            obj[:] = cast(list[object], contents)
        elif isinstance(obj, dict):
            obj.clear()
            obj.update(cast(dict[object, object], contents))
        elif isinstance(obj, set):
            obj.clear()
            obj.update(cast(set[object], contents))
        else:
            for name, value in cast(dict[str, object], contents).items():
                object.__setattr__(obj, name, value)


def _close_record_outcome(
        *, refused: bool, order_id: int | None, executed: float, answered: bool,
        complete: bool, volume: float | None,
) -> tuple[str, int | None, float] | None:
    """The outcome a close's own record proves, for its journal row.

    The record is the close's latest state (:class:`_OwnClose`, as this
    process holds it or as the journal does). ``executed`` counts only fills
    matched to the close by its order id once the answer named that order, so
    with the order known, own fills covering the whole requested volume, or
    the order's final fill (``complete``), prove the close executed. A part of
    the volume on an order still working proves nothing yet: the order may
    execute more, or the venue may end it after that part. A venue cancel or
    reject of the order ends the row from the order's own final figures in the
    transaction that marks the record complete (:func:`_ended_close_verdict`),
    so a record complete with a part executed and its row still live was
    completed by its final fill. Without the order, the answer was lost: no
    fill can ever be matched to the close, so nothing will show whether it
    executed.

    :param refused: The venue provably refused the close.
    :param order_id: The close's venue order, once the answer named it.
    :param executed: Units of the close's own fills seen by its order id.
    :param answered: The venue answered the close, or its answer was lost.
    :param complete: Nothing more of the close can execute
        (:attr:`_OwnClose.complete`).
    :param volume: Units the close asked for; ``None`` when not recorded.
    :return: ``(outcome, order_id, executed)`` for
        :meth:`_ProtectionMixin._finish_close_row`, or ``None`` when the record
        proves nothing yet (the close is in flight, or answered and its working
        order has not shown the whole volume executed).
    """
    if refused:
        return 'refused', order_id, 0.0
    if order_id is not None:
        if executed > _VOLUME_TOL and (
                complete or (volume is not None and executed >= volume - _VOLUME_TOL)):
            return 'executed', order_id, executed
        return None
    return ('answer_lost', None, 0.0) if answered else None


def _ended_close_verdict(
        *, rejected: bool, order_id: int, executed_cents: int | None, proven: float,
        volume_cents: int | None,
) -> tuple[str, int | None, float] | None:
    """The outcome of an own close order the venue cancelled, expired or rejected.

    From the order's own final executed volume, never below what the close's
    own fills already proved executed: none executed is a refusal (rejected)
    or an end with nothing executed (cancelled or expired); a part of the
    close's volume is an end after that part (``'ended_partially'``); the
    whole volume, or a volume the close did not record, is an execution. A
    message without the order's executed volume proves only that the close
    executed in whole when its own fills cover the whole volume; otherwise it
    proves nothing (fills of the order may not have reached the bot), and the
    row waits for a message that carries the volume.

    :param rejected: The venue rejected the order.
    :param order_id: The close's venue order.
    :param executed_cents: The order's final executed volume (centi-units);
        ``None`` when the message does not carry it.
    :param proven: Units the close's own fills, matched by its order id,
        executed (:attr:`_OwnClose.executed`).
    :param volume_cents: The volume the close was sent for, when recorded.
    :return: ``(outcome, order_id, executed units)`` for
        :meth:`_ProtectionMixin._finish_close_row`, or ``None`` when the
        message proves nothing.
    """
    if executed_cents is None:
        if (volume_cents is not None and proven > _VOLUME_TOL
                and proven >= volume_to_units(volume_cents) - _VOLUME_TOL):
            return 'executed', order_id, proven
        return None
    executed = max(volume_to_units(executed_cents), proven)
    if executed <= _VOLUME_TOL:
        return ('refused' if rejected else 'ended_unfilled'), order_id, 0.0
    if volume_cents is not None and executed < volume_to_units(volume_cents) - _VOLUME_TOL:
        return 'ended_partially', order_id, executed
    return 'executed', order_id, executed


class _ProtectionMixin(_CTraderBase, ABC):
    """Moved / removed SL-TP, foreign order edits and manual-close detection."""

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
        if position_ref.isdigit():
            self._protection_reported.add(int(position_ref))
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
            nothing_to_amend_lands: bool = True,
    ) -> OpenApiMessages.ProtoOAExecutionEvent | None:
        """Send one SL/TP amend, recording its levels as the bot's own.

        The levels are expected before the send and settled once the venue
        answered, naming the write :meth:`ProtectionLevelGuard.expect` returned,
        so overlapping writes of the same levels answered out of order each
        close their own: confirmed on success, and on a ``Nothing to amend``
        answer (:func:`_nothing_to_amend`), which says the position already
        carries exactly the requested protection; rejected when the venue provably did
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

        Any write makes the position no longer one the bot left bare; a write
        whose result cannot be predicted leaves it unguarded, and bare again
        only when the venue refused it (:meth:`_write_unguarded_protection`).

        :param nothing_to_amend_lands: ``False`` makes a ``Nothing to amend``
            answer a reject like any other, for a caller that judges what the
            venue carries itself (the clear of a guarded position:
            :meth:`~_ExecutionMixin._clear_position_bracket`).
        :return: The venue's answer, or ``None`` when it had nothing to amend.
        """
        key = protection_key(position_id)
        guard = self._protection_guard()
        levels = self._requested_protection(req)
        if levels is None:
            return await self._write_unguarded_protection(
                req, position_id=position_id, coid=coid, context=context,
                predecessor_cancel_ids=predecessor_cancel_ids)
        self._unmark_bare(position_id)
        in_flight = self._protection_in_flight
        if guard.known(key) and not in_flight.get(key):
            self._protection_baselined.add(key)
        self._unguarded_protection.discard(position_id)
        write = guard.expect(key, levels, row_coid=row_coid)
        self._remember_own_levels(key, levels)
        in_flight[key] = in_flight.get(key, 0) + 1
        self._protection_written_at[position_id] = epoch_time()
        try:
            event = await self._dispatch_order(
                req, coid=coid, context=context,
                predecessor_cancel_ids=predecessor_cancel_ids)
        except BaseException as exc:
            self._protection_written_at[position_id] = epoch_time()
            self._end_protection_write(key)
            if nothing_to_amend_lands and _nothing_to_amend(exc):
                guard.settle(key, levels, write=write)
                self._protection_baselined.add(key)
                return None
            if _write_refused(exc):
                guard.settle(key, None, rejected=levels, write=write)
            else:
                guard.settle(key, None, write=write)
            if key not in self._protection_baselined and not in_flight.get(key):
                guard.forget(key)
            raise
        self._protection_written_at[position_id] = epoch_time()
        self._end_protection_write(key)
        guard.settle(key, levels, write=write)
        self._protection_baselined.add(key)
        return event

    async def _write_unguarded_protection(
            self, req: OpenApiMessages.ProtoOAAmendPositionSLTPReq, *,
            position_id: int, coid: str, context: str,
            predecessor_cancel_ids: tuple[str, ...] | None = None,
    ) -> OpenApiMessages.ProtoOAExecutionEvent | None:
        """Send one SL/TP amend the level guard does not judge.

        The native fail-safe (whose levels the core fail-safe manager judges)
        and a trailing stop without an anchor (whose stop the venue seeds)
        write outside the guard, and the position is never bare while such a
        write is in flight or after it landed. The guard keeps what it holds
        until the venue answered, but judges no view of the position
        meanwhile; it is released once the write landed or its outcome is
        unknown. The levels the write sends are still the bot's (the seeded
        stop is left open): a protection firing on the position is judged
        against them (:meth:`_fired_protection`) once one such write landed in
        this process, which makes the venue's protection one the bot wrote. A
        write whose outcome is unknown leaves the venue with either the
        protection the bot knew it carried before (guarded, bare, or written
        by an earlier landed write outside the guard) or the levels it sent,
        so the position is judged the same way then; without such knowledge
        nothing is judged. A refused write left the venue as it was: the
        position is guarded (or bare) as before (:meth:`_unguarded_write_refused`).
        A ``Nothing to amend`` answer (:func:`_nothing_to_amend`) says the venue
        already carries the protection sent: the write counts as landed.

        :return: The venue's answer, or ``None`` when it had nothing to amend.
        """
        key = protection_key(position_id)
        guard = self._protection_guard()
        was_bare = self._is_bare(position_id)
        was_unguarded = position_id in self._unguarded_protection
        was_baseline = position_id in self._unguarded_baseline
        venue_known = was_bare or was_baseline or (guard.known(key) and (
                key in self._protection_baselined or not self._protection_in_flight.get(key)))
        levels = ProtectionLevels(
            stop=req.stopLoss if req.HasField('stopLoss') else None,
            limit=req.takeProfit if req.HasField('takeProfit') else None,
            trailing=req.HasField('trailingStopLoss') and req.trailingStopLoss)
        new_levels = levels not in self._own_protection_levels.get(key, ())
        self._unmark_bare(position_id)
        self._unguarded_protection.add(position_id)
        self._remember_own_levels(key, levels)
        self._begin_unguarded_write(position_id)
        try:
            event = await self._dispatch_order(
                req, coid=coid, context=context,
                predecessor_cancel_ids=predecessor_cancel_ids)
        except BaseException as exc:
            self._end_unguarded_write(position_id)
            if _nothing_to_amend(exc):
                self._release_protection(position_id)
                self._unguarded_baseline.add(position_id)
                return None
            if _write_refused(exc):
                self._unguarded_write_refused(
                    position_id, levels if new_levels else None, was_bare=was_bare,
                    was_unguarded=was_unguarded, was_baseline=was_baseline)
            else:
                self._release_protection(position_id)
                if venue_known and position_id in self._unguarded_protection:
                    self._unguarded_baseline.add(position_id)
            raise
        self._end_unguarded_write(position_id)
        self._release_protection(position_id)
        self._unguarded_baseline.add(position_id)
        return event

    def _unguarded_write_refused(
            self, position_id: int, new_levels: ProtectionLevels | None, *,
            was_bare: bool, was_unguarded: bool, was_baseline: bool,
    ) -> None:
        """The venue refused an unguarded protection write: it left the venue as it was.

        The guard kept what it held through the write, so it judges the
        position on; the levels the write sent are not the bot's, the
        position is written outside the guard only if it was before, and bare
        again if the bot had left it bare. Nothing is restored once the
        position is no longer the one the write left: vacated or cleared
        meanwhile, guarded by a newer own write, or with another own write of
        it in flight or landed outside the guard.

        :param new_levels: The levels the write sent, when no earlier own
            write had set them.
        :param was_bare: The bot had left the position bare before the write.
        :param was_unguarded: The position was written outside the guard before.
        :param was_baseline: A write outside the guard had landed on the
            position before.
        """
        key = protection_key(position_id)
        if (position_id not in self._unguarded_protection
                or (position_id in self._unguarded_baseline and not was_baseline)
                or self._protection_in_flight.get(key)
                or self._unguarded_in_flight.get(position_id)):
            return
        if new_levels is not None:
            known = self._own_protection_levels.get(key)
            if known is not None and new_levels in known:
                known.remove(new_levels)
        if not was_unguarded:
            self._unguarded_protection.discard(position_id)
        if was_bare:
            self._mark_bare(position_id)

    def _end_protection_write(self, key: str) -> None:
        """One write of ``key`` got its answer (or lost it)."""
        remaining = self._protection_in_flight.get(key, 0) - 1
        if remaining > 0:
            self._protection_in_flight[key] = remaining
        else:
            self._protection_in_flight.pop(key, None)

    def _begin_unguarded_write(self, position_id: int) -> None:
        """A protection write of a position the guard does not hold is about to be sent.

        Until it is answered, an own fill's view of the position is not taken
        as its protection (:meth:`_take_own_fill_protection`), a snapshot read
        that started before the write is not taken for an own fill's view
        (:meth:`_read_predates_own_write`) and a protection firing on the
        position is not judged (:meth:`_fired_protection`).
        """
        in_flight = self._unguarded_in_flight
        in_flight[position_id] = in_flight.get(position_id, 0) + 1
        self._protection_written_at[position_id] = epoch_time()

    def _end_unguarded_write(self, position_id: int) -> None:
        """A write :meth:`_begin_unguarded_write` announced got its answer (or lost it)."""
        self._protection_written_at[position_id] = epoch_time()
        in_flight = self._unguarded_in_flight
        remaining = in_flight.get(position_id, 0) - 1
        if remaining > 0:
            in_flight[position_id] = remaining
        else:
            in_flight.pop(position_id, None)

    def _release_protection(self, position_id: int) -> None:
        """Stop guarding a position's protection (gone, vacated, written outside the guard).

        The levels the bot set on the position stay its own for judging a
        protection that fires (:meth:`_own_levels`), the ones only the
        journal held included: the venue may still carry them while a write
        that replaces them is in flight or its outcome is unknown. They are
        forgotten with the position (:meth:`_vacate_position`).
        """
        key = protection_key(position_id)
        for levels in self._own_levels(key, position_id):
            self._remember_own_levels(key, levels)
        self._protection_guard().forget(key)
        self._protection_baselined.discard(key)
        self._last_venue_protection.pop(key, None)

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
        earlier state, so it is never judged alone (``immediate=False``). A
        position the bot holds nothing of any more is released instead, and
        one whose own fill view waits for the next snapshot read
        (:meth:`_observe_protection_snapshot`) is not judged until then.
        """
        position_id = position.positionId
        if (not position_id or position.positionStatus
                != OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN):
            return
        key = protection_key(position_id)
        guard = self._protection_guard()
        if not guard.known(key) or not self._position_owned(position_id):
            return
        if self._share_vacated(position_id):
            self._vacate_position(position_id)
            return
        levels = self._venue_protection(position)
        self._last_venue_protection[key] = levels
        tick = self._cached_tick(position.tradeData.symbolId)
        if (tick is None or position_id in self._fill_views_deferred
                or self._unguarded_in_flight.get(position_id)):
            return
        guard.observe(key, levels, tick=tick)

    def _absorb_fill_protection(
            self, position: OpenApiModelMessages.ProtoOAPosition, *, closed_volume: bool,
    ) -> None:
        """Take the protection a position shows right after this run's fill.

        A guarded position's view is judged by :meth:`_judge_own_fill_view`.
        On a position whose protection the bot wrote outside the guard, the
        view joins the levels a firing protection is judged against.

        :param closed_volume: The fill closed volume of the position (a
            reduce or a reversal on a NETTING account).
        """
        position_id = position.positionId
        if (not position_id or position.positionStatus
                != OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN):
            return
        key = protection_key(position_id)
        if not self._protection_guard().known(key):
            if position_id in self._unguarded_baseline:
                self._remember_own_levels(key, self._venue_protection(position))
            return
        self._judge_own_fill_view(
            position, may_have_changed=closed_volume,
            tick=self._cached_tick(position.tradeData.symbolId))

    def _judge_own_fill_view(
            self, position: OpenApiModelMessages.ProtoOAPosition, *, may_have_changed: bool,
            tick: float | None, read_started_at: float | None = None,
    ) -> None:
        """Take the protection a view of the bot's own fill shows on a guarded position.

        The bot sends no protection with its orders, so a fill that opened or
        added volume leaves the protection as it was: when the venue showed
        the same levels before the fill, the view is judged like any other
        view, and a change someone made before the fill stays an edit. A
        pushed view of such a fill that shows levels no own write explains,
        while no own write of the position is in flight, is judged the same
        way: someone else set them. A fill that closed volume
        (``may_have_changed``: a reduce or a reversal on a NETTING account)
        may have made the venue change the protection, which is not measured,
        and a pushed view that shows other levels the bot set, or any levels
        while an own write is in flight, may predate a newer own write.

        Such a view is accepted as an own write whose outcome is unknown
        (``settle`` with ``None``), next to the levels already accepted: the
        venue seen carrying it makes it the bot's protection, two snapshots
        carrying another own level drop it, since the view may predate a newer
        own write. Levels the bot never set are logged as an ERROR with an
        audit event: whether the bot's fill or someone else set them cannot be
        told.

        :param may_have_changed: The fill may have changed the protection:
            it closed volume, or the caller cannot tell (a reconcile pass).
        :param tick: The symbol's price tick, when known.
        :param read_started_at: Wall-clock time a snapshot read started;
            ``None`` for a pushed view.
        """
        position_id = position.positionId
        key = protection_key(position_id)
        guard = self._protection_guard()
        levels = self._venue_protection(position)
        tolerance = _price_tolerance(tick, levels.stop, levels.limit)
        seen = self._last_venue_protection.get(key)
        self._last_venue_protection[key] = levels
        if not may_have_changed and seen is not None and seen.matches(levels, tolerance):
            if tick is not None and position_id not in self._fill_views_deferred:
                guard.observe(key, levels, tick=tick, read_started_at=read_started_at)
            return
        explained = any(own.matches(levels, tolerance)
                        for own in self._own_levels(key, position_id))
        if (not may_have_changed and not explained and read_started_at is None
                and not self._protection_in_flight.get(key)
                and not self._unguarded_in_flight.get(position_id)
                and tick is not None and position_id not in self._fill_views_deferred):
            # The fill left the protection as it was and no own write explains
            # these levels: someone else set them, the view is judged.
            guard.observe(key, levels, tick=tick)
            return
        write = guard.expect(key, levels)
        guard.settle(key, None, write=write)
        self._protection_baselined.add(key)
        self._remember_own_levels(key, levels)
        if explained:
            return
        logger.error(
            "cTrader: the bot's own fill on position %d shows protection the bot never set "
            "(%s; the venue showed %s before); accepted, not reported: whether the fill or "
            "someone else set it cannot be told",
            position_id, levels, seen)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _FILL_PROTECTION_UNEXPLAINED_EVENT, exchange_order_id=str(position_id),
                payload={'position_id': position_id, 'levels': levels.as_list(),
                         'seen': seen.as_list() if seen is not None else None,
                         'pushed': read_started_at is None,
                         'may_have_changed': may_have_changed})

    async def _observe_protection_snapshot(
            self, open_positions: dict[int, OpenApiModelMessages.ProtoOAPosition], *,
            read_started_at: float, filled_positions: set[int],
    ) -> None:
        """Judge every guarded, run-owned open position of a reconcile snapshot.

        The view of a position an own fill was booked on is taken as the
        fill left it only when the read started after the bot's last
        protection write of the position: the pass yields the fill before it
        reaches this view, and the engine may book it and write the bracket
        while the pass still reads deal histories. An older view proves
        neither the levels the bot owns nor that the position is bare; the
        first read after the write stands in for it.

        :param open_positions: The snapshot's open positions by id.
        :param read_started_at: Wall-clock time the snapshot read started.
        :param filled_positions: Positions this pass booked a fill of the
            run's own order on. The pass cannot tell whether the fill closed
            volume, so a guarded one's view is taken as one the fill may have
            changed (:meth:`_judge_own_fill_view`), and one the bot holds no
            levels for is guarded from this view on — bare when it carries
            none (:meth:`_take_own_fill_protection`).
        """
        guard = self._protection_guard()
        filled = filled_positions | (self._fill_views_deferred & open_positions.keys())
        self._fill_views_deferred = set()
        for position_id, position in open_positions.items():
            key = protection_key(position_id)
            fill_view = position_id in filled
            if fill_view and self._read_predates_own_write(position_id, read_started_at):
                self._fill_views_deferred.add(position_id)
                continue
            if not guard.known(key):
                if fill_view and self._position_owned(position_id):
                    self._take_own_fill_protection(position)
                continue
            if not self._position_owned(position_id):
                continue
            if self._share_vacated(position_id):
                self._vacate_position(position_id)
                continue
            tick = await self._resolve_tick(position.tradeData.symbolId)
            if fill_view:
                self._judge_own_fill_view(position, may_have_changed=True, tick=tick,
                                          read_started_at=read_started_at)
                continue
            levels = self._venue_protection(position)
            self._last_venue_protection[key] = levels
            if tick is None or self._unguarded_in_flight.get(position_id):
                continue
            guard.observe(key, levels, tick=tick, read_started_at=read_started_at)

    async def _observe_resume_baseline(self) -> None:
        """Take the venue as the operator left it right after a resume.

        An operator resume makes the next observation of every guarded key
        its new baseline (:func:`rebaseline_after_resume`). Pushes queue from
        the moment the connection opens, so waiting for the first reconcile
        pass would let an edit someone makes in between become that baseline;
        a snapshot read before the stream judges any push closes the window.
        A no-op unless a resume flagged a persisted protection; a failed read
        leaves the push / reconcile path to set the baseline (WARNING).
        """
        if self.store_ctx is None or not any(
                isinstance(saved := (row.extras or {}).get(PROTECTION_LEVELS_EXTRA), dict)
                and saved.get('rebaseline')
                for row in self.store_ctx.iter_live_orders()):
            return
        read_started_at = epoch_time()
        try:
            res = await self._reconcile()
        except (BrokerError, CTraderWireError, TimeoutError) as exc:
            logger.warning(
                "cTrader: the resume baseline snapshot failed (the next view sets it): %s", exc)
            return
        await self._observe_protection_snapshot(
            {position.positionId: position for position in res.position
             if position.positionStatus
             == OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN},
            read_started_at=read_started_at, filled_positions=set())

    def _read_predates_own_write(self, position_id: int, read_started_at: float) -> bool:
        """Whether a snapshot read may show a position before an own protection write of it."""
        return bool(self._protection_in_flight.get(protection_key(position_id))
                    or self._unguarded_in_flight.get(position_id)
                    or self._protection_written_at.get(position_id, -math.inf)
                    > read_started_at)

    def _remember_own_levels(self, key: str, levels: ProtectionLevels) -> None:
        """Record levels the bot set (or accepted after its own fill) on a key."""
        known = self._own_protection_levels.setdefault(key, [])
        if levels not in known:
            known.append(levels)

    def _saved_protection(self, key: str, position_id: int) -> dict | None:
        """The guard's persisted state of a position's protection, when the
        journal holds it (:data:`PROTECTION_LEVELS_EXTRA`)."""
        if self.store_ctx is None:
            return None
        row_coid = self._protection_row(position_id)
        row = self.store_ctx.get_order(row_coid) if row_coid is not None else None
        saved = (row.extras or {}).get(PROTECTION_LEVELS_EXTRA) if row is not None else None
        return saved if isinstance(saved, dict) and saved.get('key') == key else None

    def _bare_explained_by_own_write(self, key: str, position_id: int) -> bool:
        """Whether the bot's own protection explains a guarded position without any.

        An own write of no protection the guard still accepts (landed, or its
        outcome unknown) explains it, and so does an operator resume whose
        accepted venue protection no view has been taken of yet. The guard's
        persisted state tells; without a journal it is not known (``False``).
        """
        if self._resume_baseline_pending(key, position_id):
            return True
        saved = self._saved_protection(key, position_id)
        for entry in (saved or {}).get('writes') or []:
            try:
                levels = ProtectionLevels.from_list(entry[1])
            except (IndexError, TypeError, ValueError):
                continue
            if levels.stop is None and levels.limit is None and not levels.trailing:
                return True
        return False

    def _note_clear_found_bare(self, position_id: int) -> None:
        """An own clear found a guarded position without protection no own write explains.

        The guard keeps the bot's levels: the snapshots that follow judge the
        removal like any other view, own fills booked first.
        """
        logger.warning(
            "cTrader: clearing the protection of bot position %d found none on the venue "
            "although no own write removed it; the bot's levels stay guarded and the next "
            "snapshots judge the removal", position_id)

    def _own_levels(self, key: str, position_id: int) -> list[ProtectionLevels]:
        """Every level the bot set on a position: this process's and the persisted ones."""
        levels = list(self._own_protection_levels.get(key, ()))
        saved = self._saved_protection(key, position_id)
        for entry in (saved or {}).get('writes') or []:
            try:
                levels.append(ProtectionLevels.from_list(entry[1]))
            except (IndexError, TypeError, ValueError):
                continue
        return levels

    def _trailing_in_force(
            self, key: str, position_id: int, own: Sequence[ProtectionLevels],
    ) -> bool:
        """Whether a trailing stop of the bot's may be a position's protection now.

        For a guarded position, the guard's persisted state tells: the newest
        write the venue confirmed, and every write whose outcome is still
        unknown. A trailing stop the bot replaced by a confirmed static one
        is no longer on the venue. Without that state (the levels are kept in
        memory only, or the protection was written outside the guard) any own
        trailing stop counts.

        :param own: Every level the bot set on the position (:meth:`_own_levels`).
        """
        saved = self._saved_protection(key, position_id)
        if saved is not None:
            try:
                writes = {int(gen): ProtectionLevels.from_list(levels)
                          for gen, levels in saved.get('writes') or []}
                gens = [int(gen) for gen in saved.get('unresolved') or []]
                if saved.get('confirmed') is not None:
                    gens.append(int(saved['confirmed']))
            except (TypeError, ValueError):
                pass
            else:
                return any(writes[gen].trailing for gen in gens if gen in writes)
        return any(levels.trailing for levels in own)

    # --- fired protection ---------------------------------------------------

    def _judge_fired_protection(
            self, message: OpenApiMessages.ProtoOAExecutionEvent,
            order: OpenApiModelMessages.ProtoOAOrder, *,
            from_entry: str | None, fill_qty: float,
    ) -> None:
        """Report a position SL / TP that fired at a level the bot did not set.

        Runs before the fill releases the position's protection; the verdict
        is :meth:`_fired_protection`'s. Booking is unaffected: the fill is
        venue truth either way. The report follows the yielded fill. The
        caller skips a position the bot held nothing of when the protection
        fired: it protected someone else's exposure only.
        """
        position_id = order.positionId
        if not position_id or position_id in self._protection_reported:
            return
        stop, limit = self._fired_levels(message, order)
        context = {
            'position_id': position_id,
            'order_id': order.orderId,
            'deal_id': message.deal.dealId if message.HasField('deal') else None,
            'closed_qty': fill_qty,
            'order_type': int(order.orderType),
            'from_entry': from_entry,
            'fired_stop': stop,
            'fired_limit': limit,
        }
        verdict, own = self._fired_protection(
            position_id, stop, limit, tick=self._cached_tick(order.tradeData.symbolId))
        if verdict is FiredProtection.AFTER_RESUME:
            self._note_fired_after_resume(position_id, context)
        elif verdict is FiredProtection.UNCONFIRMED:
            self._note_fired_unconfirmed(position_id, context)
        elif verdict is FiredProtection.FOREIGN:
            context['own_levels'] = [level.as_list() for level in own]
            self._queue_external_close(position_id, context, reason=(
                f"bot position {position_id} was closed by a protection level someone else "
                f"set: stop {stop}, take-profit {limit} fired ({fill_qty} units by order "
                f"{order.orderId}); the bot never set it"))
        elif verdict is FiredProtection.BARE:
            context['bare'] = True
            self._queue_external_close(position_id, context, reason=(
                f"a protection the bot did not set closed bot position {position_id}: "
                f"stop {stop}, take-profit {limit} fired ({fill_qty} units by order "
                f"{order.orderId}) on a position the bot left unprotected"))

    def _fired_protection(
            self, position_id: int, stop: float | None, limit: float | None, *,
            tick: float | None,
    ) -> tuple[FiredProtection, list[ProtectionLevels]]:
        """Judge a position SL / TP that fired against the bot's own protection.

        A guarded position's fired prices are compared with every level the
        bot set on it (:func:`_fired_prices_explained`); a position the bot
        left bare carried no protection of the bot, so any SL / TP firing on
        it is someone else's, whatever the guard holds for it (its empty
        levels, after a clear the levels the clear replaced too: the clear
        landed, so none of them can fire). A position whose protection the bot wrote
        outside the guard (the native fail-safe, a trailing stop without an
        anchor) is judged the same way once one such write landed in this
        process (:meth:`_write_unguarded_protection`): from then on the venue
        carries only protection the bot wrote, unless someone else changed it.
        Nothing is judged while an own write of the position is in flight, nor
        for a position whose protection the bot never wrote in this process
        or through the guard.

        After an operator resume the venue's protection as it stood was
        accepted; until a venue view becomes the baseline or one of the bot's
        later writes is confirmed, that protection may be what fired
        (:attr:`FiredProtection.AFTER_RESUME`).

        :param stop: The fired order's stop price, when it reports one.
        :param limit: The fired order's limit price, when it reports one.
        :param tick: The symbol's price tick, when known.
        :return: The verdict, and the bot's own levels it was judged against.
        """
        key = protection_key(position_id)
        if self._is_bare(position_id):
            return FiredProtection.BARE, []
        guarded = self._protection_guard().known(key)
        if self._protection_in_flight.get(key) or self._unguarded_in_flight.get(position_id):
            return FiredProtection.OWN, []
        if not guarded:
            if position_id not in self._unguarded_baseline:
                return FiredProtection.OWN, []
        elif self._resume_baseline_pending(key, position_id):
            return FiredProtection.AFTER_RESUME, []
        own = self._own_levels(key, position_id)
        if not own:
            return FiredProtection.OWN, own
        prices = [price for price in (stop, limit) if price is not None]
        tolerance = _price_tolerance(tick, *prices)
        if not prices:
            seen = self._last_venue_protection.get(key)
            if seen is not None and not any(level.matches(seen, tolerance) for level in own):
                return FiredProtection.UNCONFIRMED, own
            return FiredProtection.OWN, own
        if _fired_prices_explained(own, prices, tolerance,
                                   trailing=self._trailing_in_force(key, position_id, own)):
            return FiredProtection.OWN, own
        return FiredProtection.FOREIGN, own

    def _note_fired_unconfirmed(self, position_id: int, context: dict) -> None:
        """A protection fired without reporting its level after a foreign venue view.

        ERROR + audit: the venue showed levels the bot did not set, but the
        protection that fired cannot be proven to be one of them.
        """
        seen = self._last_venue_protection.get(protection_key(position_id))
        logger.error(
            "cTrader: the SL / TP of bot position %d fired after the venue showed "
            "levels the bot did not set (%s); the fired order reports no level, "
            "so the protection that fired cannot be proven foreign",
            position_id, seen)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _PROTECTION_FIRED_UNCONFIRMED_EVENT,
                exchange_order_id=str(position_id),
                payload={**context, 'seen': seen.as_list() if seen is not None else None})

    def _is_bare(self, position_id: int) -> bool:
        """Whether the bot left a position without protection (this process or an earlier one).

        A mark an earlier process left before an operator resume no longer
        counts: the resume accepted whatever protection the position carried
        then. The process that applied the resume drops such marks from the
        journal (:meth:`_rebaseline_bare_marks`).
        """
        if position_id in self._bare_positions:
            return True
        key = protection_key(position_id)
        resume_ts_ms = self.operator_resume_ts_ms
        for row in self._position_entry_rows(position_id):
            mark = (row.extras or {}).get(_BARE_PROTECTION_EXTRA)
            if not isinstance(mark, dict) or mark.get('key') != key:
                continue
            marked_at_ms = mark.get('at_ms')
            if resume_ts_ms is not None and (not isinstance(marked_at_ms, int)
                                             or marked_at_ms <= resume_ts_ms):
                continue
            self._bare_positions.add(position_id)
            return True
        return False

    def _rebaseline_bare_marks(self, res: OpenApiMessages.ProtoOAReconcileRes) -> None:
        """Re-take the bare marks from the venue an operator resume accepted.

        Runs once, on the startup adoption snapshot, in a process that applied
        an operator resume. A mark set before the resume no longer counts:
        the operator accepted whatever protection the position carried. It is
        dropped from the journal row, so a later plain restart cannot revive
        it. A position the snapshot still shows without any protection, and
        that the bot does not protect otherwise, is bare from now on and is
        marked again: the operator accepted it without protection, so the
        empty levels replace the ones the resume voided, and protection
        someone adds to it later is an edit.

        :param res: The startup adoption snapshot.
        """
        resume_ts_ms = self.operator_resume_ts_ms
        if self.store_ctx is None or resume_ts_ms is None:
            return
        stale: set[int] = set()
        for row in list(self.store_ctx.iter_live_orders()):
            extras = row.extras or {}
            mark = extras.get(_BARE_PROTECTION_EXTRA)
            if _BARE_PROTECTION_EXTRA not in extras or (
                    isinstance(mark, dict) and isinstance(mark.get('at_ms'), int)
                    and mark['at_ms'] > resume_ts_ms):
                continue
            self.store_ctx.upsert_order(row.client_order_id, extras={
                name: value for name, value in extras.items()
                if name != _BARE_PROTECTION_EXTRA})
            _prefix, _sep, position_ref = str(
                mark.get('key') if isinstance(mark, dict) else '').partition(':')
            if position_ref.isdigit():
                stale.add(int(position_ref))
        open_positions = {
            position.positionId: position for position in res.position
            if position.positionStatus
            == OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN}
        for position_id in stale:
            self._bare_positions.discard(position_id)
            position = open_positions.get(position_id)
            if (position is not None and self._position_owned(position_id)
                    and self._shows_no_protection(position)):
                self._protection_guard().forget(protection_key(position_id))
                self._mark_bare(position_id)

    def _mark_bare(self, position_id: int) -> None:
        """Remember a position the bot left without protection, on its journal row too.

        The guard then holds the bot's empty protection for it
        (:meth:`_guard_bare`).
        """
        self._bare_positions.add(position_id)
        rows = self._position_entry_rows(position_id)
        if self.store_ctx is not None and rows:
            key = protection_key(position_id)
            self._unmark_bare_rows(position_id)
            row = self.store_ctx.get_order(rows[0].client_order_id) or rows[0]
            self.store_ctx.upsert_order(row.client_order_id, extras={
                **(row.extras or {}),
                _BARE_PROTECTION_EXTRA: {'key': key, 'at_ms': int(epoch_time() * 1000)}})
        self._guard_bare(position_id)

    def _guard_bare(self, position_id: int) -> None:
        """Hold no protection as the bot's own for a position it left bare.

        The bot chose no protection, so a stop-loss, take-profit or trailing
        stop someone else adds to its position changes the bot's protection:
        an edit the guard reports like a move or a removal. A guard that holds
        the position already keeps it: the bot's clear went through the guard
        as an own write of no levels. A clear of a position the guard does not
        hold is an own write in flight until it is answered, so nothing takes
        the position up meanwhile (:meth:`_take_own_fill_protection`).
        """
        self._guard_as_own(position_id, ProtectionLevels())

    def _guard_as_own(self, position_id: int, levels: ProtectionLevels) -> None:
        """Start guarding a position the guard does not hold with ``levels`` as the bot's.

        The levels are accepted as an own write, after every level the bot
        set on the position before (outside the guard too): those stay
        acceptable until the venue is seen without them, so a late view of
        what the bot itself wrote is not taken for an edit, and a snapshot
        read before this moment is not judged. A guard that holds the
        position already keeps what it holds.
        """
        key = protection_key(position_id)
        guard = self._protection_guard()
        if guard.known(key):
            return
        row_coid = self._protection_row(position_id)
        for own in (*self._own_protection_levels.get(key, ()), levels):
            write = guard.expect(key, own, row_coid=row_coid)
            guard.settle(key, own, write=write)
        self._protection_baselined.add(key)
        self._remember_own_levels(key, levels)

    def _unmark_bare(self, position_id: int) -> None:
        """The bot protects a position again (or it is gone): it is no longer bare."""
        self._bare_positions.discard(position_id)
        self._unmark_bare_rows(position_id)

    def _unmark_bare_rows(self, position_id: int) -> None:
        """Drop a position's bare mark from its journal rows."""
        if self.store_ctx is None or not position_id:
            return
        for row in self._position_entry_rows(position_id):
            extras = row.extras or {}
            if _BARE_PROTECTION_EXTRA not in extras:
                continue
            self.store_ctx.upsert_order(row.client_order_id, extras={
                name: value for name, value in extras.items() if name != _BARE_PROTECTION_EXTRA})

    def _resume_baseline_pending(self, key: str, position_id: int) -> bool:
        """Whether the venue may still carry the protection an operator resume accepted.

        The resume voids the persisted levels and flags them for a new
        baseline; the guard clears the flag with the first venue view it
        judges. A confirmed write of the bot's after the resume replaced the
        accepted protection on the venue.
        """
        saved = self._saved_protection(key, position_id)
        return (saved is not None and bool(saved.get('rebaseline'))
                and saved.get('confirmed') is None)

    def _note_fired_after_resume(self, position_id: int, context: dict) -> None:
        """A protection fired before the resume's baseline was taken: WARNING + audit."""
        logger.warning(
            "cTrader: the SL / TP of bot position %d fired (stop %s, take-profit %s) "
            "after the operator resumed the run and before a venue view of its "
            "protection was taken as the baseline; the resume accepted the venue's "
            "protection as it stood, not reported",
            position_id, context.get('fired_stop'), context.get('fired_limit'))
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _PROTECTION_FIRED_AFTER_RESUME_EVENT, exchange_order_id=str(position_id),
                payload=context)

    @staticmethod
    def _fired_levels(
            message: OpenApiMessages.ProtoOAExecutionEvent,
            order: OpenApiModelMessages.ProtoOAOrder,
    ) -> tuple[float | None, float | None]:
        """``(stop, limit)`` a fired ``STOP_LOSS_TAKE_PROFIT`` order reports.

        The protection order of the snapshot's ``order[]`` carries the stop in
        ``stopPrice`` and the take-profit in ``limitPrice`` (live-measured,
        probe114); the shape of a fill push is not measured, so the verdict
        (:func:`_fired_prices_explained`) does not rely on which field carries
        which level. The closed position's own ``stopLoss`` / ``takeProfit``
        stand in when the order reports neither. ``(None, None)`` when nothing
        reports a level.
        """
        stop = order.stopPrice or None
        limit = order.limitPrice or None
        if stop is None and limit is None and message.HasField('position'):
            position = message.position
            stop = position.stopLoss if position.HasField('stopLoss') else None
            limit = position.takeProfit if position.HasField('takeProfit') else None
        return stop, limit

    # --- the bot's share of a position --------------------------------------

    def _position_entry_rows(self, position_id: int) -> list['OrderRow']:
        """Live entry rows of this run linked to a position.

        Rows a plugin synthesized at startup for an untracked leg
        (``adopted_startup``) are another run's slice and never count.
        """
        if self.store_ctx is None or not position_id:
            return []
        pid_str = str(position_id)
        rows: list['OrderRow'] = []
        for row in self.store_ctx.iter_live_orders():
            if row.state not in _ENTRY_ROW_STATES or not row.pine_entry_id:
                continue
            extras = row.extras or {}
            if extras.get(ADOPTED_STARTUP_EXTRA_KEY):
                continue
            if 'position_id' in extras:
                raw = extras.get('position_id')
                try:
                    linked = raw is not None and parse_protocol_id(
                        raw, field='position_id') == position_id
                except ValueError:
                    linked = False
            else:
                linked = row.exchange_order_id == pid_str
            if linked:
                rows.append(row)
        return rows

    @staticmethod
    def _row_exposure(row: 'OrderRow') -> float:
        """A row's unretired filled exposure (units)."""
        retired_raw = (row.extras or {}).get(JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY)
        retired = float(retired_raw) if isinstance(retired_raw, (int, float)) else 0.0
        return max(0.0, row.filled_qty - min(row.filled_qty, retired))

    @classmethod
    def _signed_exposure(cls, row: 'OrderRow') -> float:
        """A row's unretired filled exposure, signed by its side (buy positive)."""
        exposure = cls._row_exposure(row)
        return exposure if row.side == 'buy' else -exposure

    def _signed_share(self, position_id: int) -> float | None:
        """The bot's signed net in a position (units), from its journal.

        The signed unretired filled exposure of every live entry row linked
        to the position — the same signed quantity the core's startup
        ownership clamp counts. An own opposite entry on a NETTING position
        (a reversal, a reducing order) keeps the positionId, so it nets
        against the rows it reduced. ``None`` when no such row exists (a
        startup-adopted position the bot closes through its dispatch record):
        the share is not known.
        """
        rows = self._position_entry_rows(position_id)
        if not rows:
            return None
        return sum(self._signed_exposure(row) for row in rows)

    def _own_share(self, position_id: int) -> float | None:
        """The bot's share of a position (units): the size of its signed net."""
        net = self._signed_share(position_id)
        return None if net is None else abs(net)

    def _share_side(self, position_id: int) -> str | None:
        """The side (``'buy'`` / ``'sell'``) of the bot's share of a position;
        ``None`` when the share is nil or not known."""
        return _net_side(self._signed_share(position_id))

    def _share_vacated(self, position_id: int) -> bool:
        """Whether the bot is linked to an open position it holds nothing of.

        Whatever protection the position carries then protects only someone
        else's exposure, and nothing on it is the bot's.
        """
        share = self._own_share(position_id)
        return share is not None and share <= _VOLUME_TOL

    def _vacate_position(self, position_id: int) -> None:
        """Forget everything the bot keeps about a position it holds nothing of."""
        self._release_protection(position_id)
        self._own_protection_levels.pop(protection_key(position_id), None)
        self._unmark_bare(position_id)
        self._unguarded_protection.discard(position_id)
        self._unguarded_baseline.discard(position_id)
        self._protection_written_at.pop(position_id, None)
        self._fill_views_deferred.discard(position_id)
        self._close_dispatch_pine_by_position.pop(position_id, None)
        self._close_settle_entry_by_position.pop(position_id, None)
        self._shrink_watch.pop(position_id, None)

    def _record_share_effect(
            self, position_id: int, delta: float, *, counted: bool,
            executed_ms: int | None, deal_id: int | None, order_id: int,
    ) -> None:
        """Record an own fill the journal share and the event stream see at different times.

        A fill whose deal the event stream already delivered is not recorded:
        its event settled the journal already.
        """
        if (not position_id or abs(delta) <= _VOLUME_TOL
                or (deal_id and deal_id in self._seen_deal_ids)):
            return
        self._share_effects.append(_ShareEffect(
            position_id=position_id, delta=delta, counted=counted,
            executed_ms=executed_ms or None, deal_id=deal_id or None, order_id=order_id,
            recorded_at=time.monotonic()))

    def _settle_share_effects(self, order_id: int, deal_id: int | None) -> None:
        """Forget the effects a translated fill event accounts for."""
        self._share_effects = [
            effect for effect in self._share_effects
            if not ((deal_id is not None and effect.deal_id == deal_id)
                    or (effect.deal_id is None and effect.order_id == order_id))
        ]

    def _live_share_effects(self, position_id: int) -> list[_ShareEffect]:
        """Unexpired share effects of a position."""
        now = time.monotonic()
        self._share_effects = [effect for effect in self._share_effects
                               if now - effect.recorded_at < _SHARE_EFFECT_TTL_S]
        return [effect for effect in self._share_effects if effect.position_id == position_id]

    def _share_bounds(
            self, position_id: int, executed_ms: int | None, *, in_flight: bool = True,
    ) -> _ShareBounds | None:
        """Bounds of the bot's share of a position as of a deal's execution.

        The journal share runs ahead of the event stream: an own fill answered
        on a dispatch is counted before its event is translated, and an own
        close executed before the judged deal is retired only once its event
        is. The bot's signed net is bounded first (:meth:`_net_bounds`), and an
        own entry still in flight may already have executed
        (:meth:`_entries_in_flight`). The share is the size of that net on its
        side (:func:`_bounds_of_net`). Equal bounds are an exact share.

        :param position_id: The position the deal executed on.
        :param executed_ms: The deal's execution time (venue clock, ms).
        :param in_flight: Widen the bounds by the own entries in flight;
            ``False`` takes each of them as executed after the deal.
        :return: The bounds, or ``None`` when the share is not known.
        """
        rows = self._position_entry_rows(position_id)
        if not rows:
            return None
        lo, hi = self._net_bounds(position_id, rows, executed_ms)
        return _bounds_of_net(
            lo, hi, self._entries_in_flight(rows).values() if in_flight else ())

    def _net_bounds(
            self, position_id: int, rows: Sequence['OrderRow'], executed_ms: int | None,
    ) -> tuple[float, float]:
        """Bounds of the bot's signed net in a position (units) as of a deal.

        The journal net of ``rows``, corrected by the own fills it counts
        ahead of their events (:meth:`_live_share_effects`): a counted fill
        known to have executed after the deal is taken out, an uncounted close
        known to have executed before it is applied, and one whose order
        against the deal is not known may or may not apply. An answered own
        close whose fills the journal never saw (:meth:`_unresolved_own_close`:
        the answer was lost, or the fill push was) may have executed before
        the deal: the journal share may run that much above the bot's real
        share, which no reconcile pass corrects while the venue holds at
        least the journal share (another party's exposure on top).

        :param position_id: The position the deal executed on.
        :param rows: The bot's live entry rows linked to the position.
        :param executed_ms: The deal's execution time (venue clock, ms).
        """
        lo = hi = sum(self._signed_exposure(row) for row in rows)
        for effect in self._live_share_effects(position_id):
            known = effect.executed_ms is not None and executed_ms is not None
            later = known and cast(int, effect.executed_ms) > cast(int, executed_ms)
            earlier = known and cast(int, effect.executed_ms) < cast(int, executed_ms)
            if effect.counted:
                certain, shift = later, -effect.delta
                excluded = earlier
            else:
                certain, shift = earlier, effect.delta
                excluded = later
            if certain:
                lo += shift
                hi += shift
            elif not excluded:
                lo += min(0.0, shift)
                hi += max(0.0, shift)
        unresolved = self._unresolved_own_close(position_id)
        if unresolved > _VOLUME_TOL:
            # An own close the journal never saw execute may have, at any
            # time: it takes the net toward zero, never past it.
            if lo > 0.0:
                lo = max(0.0, lo - unresolved)
            elif hi < 0.0:
                hi = min(0.0, hi + unresolved)
        return lo, hi

    def _unresolved_own_close(self, position_id: int) -> float:
        """Volume (units) of the run's answered closes of a position that may have
        executed without the event stream delivering their fills
        (:meth:`_OwnClose.unresolved`)."""
        return sum(record.unresolved() for record in self._own_close_history
                   if record.position_id == position_id)

    def _own_close_missing_from(self, position_id: int, deals: Sequence[_PositionDeal]) -> float:
        """Units the journal share may count above the bot's real share while a shortfall waits.

        While a shortfall of the position waits for the close that explains it
        (:attr:`~_CTraderBase._unexplained_shrinks`), the journal keeps
        counting the shortfall, and an own close that executed unseen may hold
        more of the share than the venue shows missing (another party's
        exposure on a NETTING position hides the rest). Each of the run's
        closes of the position answered since the wait's watermark
        (:attr:`~_CTraderBase._shrink_covered_from`; the passes before it
        retired an earlier one) counts its unresolved volume
        (:meth:`_OwnClose.unresolved`) less what its deals in ``deals`` it has
        not counted yet closed: matched by its order once the answer named it,
        else by the position and time it claims
        (:meth:`_OwnClose.claims_recovered`). Without a running wait the
        journal share is the share the passes reconciled, and nothing counts.

        :param deals: The deals of the position's history read now.
        """
        if position_id not in self._unexplained_shrinks:
            return 0.0
        watermark = self._shrink_covered_from.get(position_id, 0)
        missing = 0.0
        for record in self._own_close_history:
            if (record.position_id != position_id or record.answered_ms is None
                    or record.answered_ms < watermark):
                continue
            unresolved = record.unresolved()
            if unresolved <= _VOLUME_TOL:
                continue
            shown = volume_to_units(sum(
                deal.closed for deal in deals
                if deal.closed > 0 and deal.deal_id not in record.executed_deals and (
                    deal.order_id == record.order_id if record.order_id is not None
                    else record.claims_recovered(position_id, deal.order_id,
                                                 deal.executed_ms))))
            missing += max(0.0, unresolved - shown)
        return missing

    def _entries_in_flight(self, rows: Sequence['OrderRow']) -> dict[str, float]:
        """Signed volumes of the own entries in flight that may still add to a position.

        Only an entry on the symbol of the position's rows can, and only on a
        NETTING account: a HEDGED entry always opens a position of its own.
        A fill of the entry the journal already counts (its push overtook the
        dispatch answer, and its row is linked to the position) is part of
        the share: only the rest of the entry's volume may still execute.

        :param rows: The bot's live entry rows linked to the position.
        :return: The signed volume (units, buy positive) by client-order-id.
        """
        if self._hedging_enabled:
            return {}
        symbols = {row.symbol for row in rows}
        counted = {row.client_order_id: row.filled_qty for row in rows}
        pending: dict[str, float] = {}
        for coid, (symbol, volume) in self._entry_dispatches_in_flight.items():
            rest = abs(volume) - counted.get(coid, 0.0)
            if symbol in symbols and rest > _VOLUME_TOL:
                pending[coid] = math.copysign(rest, volume)
        return pending

    def _hold_close_verdict(self, verdict: _HeldCloseVerdict) -> None:
        """Wait for the answers of the own entries in flight before judging a close.

        The close may have taken the bot's share or only another party's
        exposure, depending on whether one of those entries executed on the
        position before it; each entry's answer tells
        (:meth:`_settle_entry_answer`).
        """
        logger.warning(
            "cTrader: deal %s closed %s units of bot position %d while own entries were in "
            "flight; whether it took the bot's share is judged once they are answered",
            verdict.deal_id, verdict.fill_qty, verdict.position_id)
        self._held_close_verdicts.append(verdict)

    def _settle_entry_answer(
            self, coid: str, answer: OpenApiMessages.ProtoOAExecutionEvent | None, *,
            refused: bool,
    ) -> None:
        """An own entry in flight was answered: judge the closes that waited for it.

        The entry executed on a waiting close's position before the close when
        its answer shows a fill on that position dated before the close's
        deal, beyond what the journal already counted when the close was held
        (a fill pushed ahead of the answer is in the held net already). An
        answer without a fill means the entry had not executed when
        the venue answered: a fill of it before the close would have reached
        the event stream ahead of the close. A provably refused dispatch never
        executed; any other failure leaves its effect unknown.

        :param coid: The entry's client-order-id.
        :param answer: The venue's answer to the dispatch, ``None`` when it
            failed.
        :param refused: Whether the failed dispatch provably left the venue
            unchanged (:func:`_write_refused`).
        """
        for verdict in list(self._held_close_verdicts):
            volume = verdict.waiting.pop(coid, None)
            if volume is None:
                continue
            if answer is None:
                if not refused:
                    verdict.unknown.append(volume)
            else:
                order = answer.order
                filled = (volume_to_units(order.executedVolume)
                          - verdict.counted.get(coid, 0.0))
                if filled > _VOLUME_TOL and order.positionId == verdict.position_id:
                    signed = math.copysign(min(filled, abs(volume)), volume)
                    deal_ms = (answer.deal.executionTimestamp or None
                               if answer.HasField('deal') else None)
                    close_ms = verdict.executed_ms
                    if deal_ms is None or close_ms is None or deal_ms == close_ms:
                        verdict.unknown.append(signed)
                    elif deal_ms < close_ms:
                        verdict.applied += signed
            if not verdict.waiting:
                self._held_close_verdicts.remove(verdict)
                self._judge_held_close(verdict)

    def _judge_held_close(self, verdict: _HeldCloseVerdict) -> None:
        """Judge a held close once every entry it waited for is answered.

        Reported when it took the venue below the bot's share; logged as an
        ERROR and audited when the evidence still cannot decide (an entry's
        outcome stayed unknown); otherwise it closed only another party's
        exposure.
        """
        bounds = _bounds_of_net(verdict.net_low + verdict.applied,
                                verdict.net_high + verdict.applied, verdict.unknown)
        taken = min(verdict.fill_qty, max(0.0, bounds.low - verdict.venue_after))
        taken_high = min(verdict.fill_qty, max(0.0, bounds.high - verdict.venue_after))
        position_id = verdict.position_id
        deal_ids = [verdict.deal_id] if verdict.deal_id is not None else []
        if taken > _VOLUME_TOL:
            self._queue_external_close(position_id, {
                **verdict.context,
                'share': bounds.low if bounds.low == bounds.high else [bounds.low, bounds.high],
                'venue_after': verdict.venue_after,
                'booked_qty': verdict.booked,
            })
            self._mark_close_judged(deal_ids, {d: CloseOrigin.EXTERNAL for d in deal_ids})
        elif taken_high > _VOLUME_TOL:
            self._note_share_ambiguous(
                position_id, deal_ids, low=taken, high=taken_high, closed_qty=verdict.fill_qty)
        else:
            logger.info(
                "cTrader: deal %s closed only another party's exposure on bot position %d",
                verdict.deal_id, position_id)
            self._mark_close_judged(deal_ids, {d: CloseOrigin.EXTERNAL for d in deal_ids})

    def _share_settled(self, position_id: int) -> bool:
        """Whether the journal share of a position is final: nothing of the bot's
        that could change it is in flight or counted ahead of its event."""
        return (not self._entries_in_flight(self._position_entry_rows(position_id))
                and not any(record.position_id == position_id and record.answered_ms is None
                            for record in self._own_closes)
                and not self._live_share_effects(position_id))

    def _retire_partial_close_exposure(
            self, position_id: int, closed_qty: float, *,
            source: str = 'partial_close_fill', entry_id: str | None = None,
    ) -> list[tuple[str, float]]:
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
        never reaches this — the flat branch retires the rows wholesale.

        Rows are matched by the ``extras['position_id']`` mirror
        (``exchange_order_id`` fallback) and reduced in the order the core
        settles the close against its binding book
        (:func:`_in_settlement_order`): the entry that filled first absorbs
        first, whichever was journaled first. Each row absorbs at
        most its own unretired remainder; an overshoot (venue closed more than
        the journal attributes to this run) is clamped rather than driven
        negative. When the bot's share is known, only the rows on the side of
        its net absorb, and at most the share: retiring a row of the bot's own
        opposite entry would grow the net instead of shrinking it.

        A close the event names an entry for is settled by the core against
        that entry's bindings first, and only the rest against the other
        entries in fill order. The rows of that entry therefore absorb first,
        and the other rows take the remainder, so the per-entry exposure a
        restart rebuilds from the journal is the one the engine held. Every
        path that debits a close (the PUSH fill, a held fill replayed once its
        close is answered, a close recovered from the deal history, a shrink a
        restart found) takes its rows from here.

        :param position_id: The netted ``positionId`` the close reduced.
        :param closed_qty: The quantity of the bot's share the close took.
        :param source: The audit event's ``source``.
        :param entry_id: The Pine entry the core settles the close against
            (:meth:`_settlement_entry`), or ``None`` for an unbound close.
        :return: The ``(client_order_id, volume)`` debits, in the order they
            were taken, so a fill found later not to be the bot's close gives
            back exactly its own (:meth:`_restore_retired_exposure`).
        """
        debits: list[tuple[str, float]] = []
        if self.store_ctx is None or not position_id or closed_qty <= 0.0:
            return debits
        pid_str = str(position_id)
        remaining = closed_qty
        net = self._signed_share(position_id)
        side = _net_side(net)
        if net is not None:
            if side is None:
                return debits
            remaining = min(remaining, abs(net))
        rows = _in_settlement_order(self.store_ctx.iter_live_orders(), entry_id)
        for row in rows:
            if remaining <= 1e-9:
                break
            if side is not None and row.side != side:
                continue
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
                         'source': source},
            )
            debits.append((row.client_order_id, take))
            remaining -= take
        return debits

    def _note_entry_fill_order(self, event: OrderEvent) -> None:
        """Give an entry row its place in the fill order as its fill is delivered.

        Called for every event the stream delivers, just before the engine
        gets it, so the places follow the order in which the core books the
        fills. The core opens one binding per entry order on its first booked
        fill and grows it with every further slice; a slice that comes after a
        close spent the whole binding opens a new one, at the end of the book.
        The row gets the next place (:data:`_FILL_SEQ_EXTRA`) on the same
        occasions: on its first delivered fill, and on a fill delivered while
        none of what the row filled before is unretired. The place is
        journaled, so a later process settles in the same order.

        Only a fill the core books counts: an entry fill with a quantity and a
        price.

        :param event: The event about to be delivered.
        """
        store = self.store_ctx
        fill_qty = event.fill_qty or 0.0
        if (store is None or event.leg_type is not LegType.ENTRY
                or event.event_type not in ('filled', 'partial')
                or fill_qty <= 0.0 or (event.fill_price or 0.0) <= 0.0):
            return
        order = event.order
        row = store.get_order(order.client_order_id) if order.client_order_id else None
        if row is None and order.id:
            row = store.find_by_ref('order_id', order.id)
        if row is None or row.closed_ts_ms is not None:
            return
        extras = dict(row.extras or {})
        retired_raw = extras.get(JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY)
        retired = float(retired_raw) if isinstance(retired_raw, (int, float)) else 0.0
        if _fill_seq(row) is not None and order.filled_qty - fill_qty - retired > 1e-9:
            return
        last = max((seq for seq in map(_fill_seq, store.iter_live_orders()) if seq is not None),
                   default=0)
        extras[_FILL_SEQ_EXTRA] = last + 1
        store.upsert_order(row.client_order_id, extras=extras)

    def _settlement_entry(
            self, position_id: int, order_id: int, from_entry: str | None, *,
            venue: bool,
    ) -> str | None:
        """The Pine entry the core settles a run-owned close fill against.

        A fill of a close this run sent carries that close's label as
        ``from_entry``; for an engine-made close that label is the synthetic
        intent id, and the core settles the entry the close targets instead.
        A fill of a close the venue already named by its order id settles the
        entry recorded with that close (:attr:`_OwnClose.settle_entry`), even
        when a later close of the same position was dispatched since. A fill
        a close whose order id is not known yet took settles the entry
        recorded with that close; the venue's own close never is one. Any
        other ``from_entry`` is already an entry id (the owner of the position
        a fill no own close claims reduced), or ``None``.

        :param position_id: The position the fill closed volume of.
        :param order_id: The venue's ``orderId`` of the fill.
        :param from_entry: The fill event's ``from_entry``.
        :param venue: Whether the fill is the venue's own close
            (:meth:`_is_venue_close`).
        """
        record = self._own_close_of_order(order_id, position_id)
        if record is not None:
            return record.settle_entry
        pending = None if venue else self._own_close_wildcard(position_id)
        if pending is not None and pending.from_entry == from_entry:
            return pending.settle_entry
        return from_entry

    @staticmethod
    def _venue_after(
            message: OpenApiMessages.ProtoOAExecutionEvent, side: str | None,
    ) -> float:
        """The volume (units) a fill left on the bot's side of a position.

        ``0.0`` when the event carries no open position, or the position now
        holds the side opposite ``side`` (someone reversed it). With no side
        (the share is nil or its side undecided) the whole position counts.
        """
        if not message.HasField('position'):
            return 0.0
        position = message.position
        if (position.positionStatus
                == OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_CLOSED):
            return 0.0
        venue_side = ('buy' if position.tradeData.tradeSide
                      == OpenApiModelMessages.ProtoOATradeSide.BUY else 'sell')
        if side is not None and venue_side != side:
            return 0.0
        return volume_to_units(position.tradeData.volume)

    def _take_own_fill_protection(self, position: OpenApiModelMessages.ProtoOAPosition) -> None:
        """Take the protection the bot's own fill left on a position it holds no levels for.

        Whatever the venue shows right after the bot's own fill is the bot's
        protection state from then on. A position that carries none is bare:
        any SL / TP that fires on it later, or that someone adds to it, was
        set by someone else (:meth:`_mark_bare`). Protection the bot did not
        write (on a NETTING account its entry merged into a position someone
        else protected) covers the bot's position now: the guard holds it as
        the bot's, so a later change of it is an edit. Only a merge explains
        it: a NETTING position larger than the bot's share (:meth:`_merged_view`).
        Otherwise (a HEDGED account, or a NETTING position that holds only the
        bot's exposure) the entry opened a position of its own and the bot
        sends no protection with its orders. Someone may have attached it to
        the bot's working entry, or an earlier process wrote it outside the
        guard; which one cannot be told, so it is still taken as the venue
        truth, with an ERROR and an audit event, unless it is a level the bot
        set. A position whose
        protection the bot wrote outside the guard (the native fail-safe, a
        trailing stop without an anchor) is left alone: the fill's view of it
        may predate that write. So is a position with an own protection write
        in flight, a clear of a position the guard does not hold included: the
        write's outcome is the bot's newest protection, and the clear guards
        the position itself once it is done.
        """
        position_id = position.positionId
        if (not position_id or position.positionStatus
                != OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN
                or position_id in self._unguarded_protection
                or self._unguarded_in_flight.get(position_id)):
            return
        key = protection_key(position_id)
        if self._protection_guard().known(key) or self._protection_in_flight.get(key):
            return
        if self._shows_no_protection(position):
            self._mark_bare(position_id)
            return
        levels = self._venue_protection(position)
        tick = self._cached_tick(position.tradeData.symbolId)
        tolerance = _price_tolerance(tick, levels.stop, levels.limit)
        explained = (self._merged_view(position)
                     or any(own.matches(levels, tolerance)
                            for own in self._own_levels(key, position_id)))
        self._guard_as_own(position_id, levels)
        if explained:
            return
        logger.error(
            "cTrader: the bot's own fill opened position %d (%s account) carrying "
            "protection the bot never set (%s); taken as the venue truth, not reported: "
            "whether someone else attached it to the bot's entry or an earlier process "
            "wrote it cannot be told",
            position_id, 'HEDGED' if self._hedging_enabled else 'NETTING', levels)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _FILL_PROTECTION_UNEXPLAINED_EVENT, exchange_order_id=str(position_id),
                payload={'position_id': position_id, 'levels': levels.as_list(),
                         'hedged': self._hedging_enabled})

    def _merged_view(self, position: OpenApiModelMessages.ProtoOAPosition) -> bool:
        """Whether the bot's own fill merged into a position someone else holds too.

        Only a NETTING position merges, and the merge shows as more volume
        than the bot's share. A position the bot's journal holds no share of
        (a startup-adopted one) is not the bot's own to judge, so it counts
        as merged.
        """
        if self._hedging_enabled:
            return False
        share = self._own_share(position.positionId)
        return (share is None
                or volume_to_units(position.tradeData.volume) > share + _VOLUME_TOL)

    def _shows_no_protection(self, position: OpenApiModelMessages.ProtoOAPosition) -> bool:
        """Whether an open position carries no protection and the bot writes
        none for it outside the guard."""
        return (bool(position.positionId)
                and position.positionStatus
                == OpenApiModelMessages.ProtoOAPositionStatus.POSITION_STATUS_OPEN
                and not position.HasField('stopLoss') and not position.HasField('takeProfit')
                and position.positionId not in self._unguarded_protection)

    def _is_run_client_order_id(self, coid: str) -> bool:
        """Whether a client-order-id is one this run sent.

        A journal row of this run instance names it, or it carries the run's
        tag: a row an earlier process of the run closed belongs to that
        process's instance, which this process's lookups do not see.
        """
        store = self.store_ctx
        if store is None or not coid:
            return False
        return (store.get_order(coid) is not None
                or coid.startswith(f"{store.run_tag}-"))

    def _is_own_entry_order(self, order_id: int) -> bool:
        """Whether an order is one of this run's entry orders."""
        return (self.store_ctx is not None and bool(order_id)
                and self.store_ctx.find_by_ref('order_id', str(order_id)) is not None)

    # --- own closes and closing-fill classification --------------------------

    def _open_own_close(
            self, position_id: int, volume: float | None = None, *,
            coid: str | None = None, volume_cents: int | None = None,
            row: tuple[str, str] | None = None,
    ) -> _OwnClose:
        """Record a close of ``position_id`` this run is about to send.

        The record claims PUSH fills until its grace ends, and stays in
        :attr:`~_CTraderBase._own_close_history` for judging recovered closes. It is
        journalled before the request is written, so a send without a journal
        record never reached the wire; the process tracks it only once the
        journal holds it.

        :param position_id: The position the close reduces.
        :param volume: The volume (units) the close asks for, when known.
        :param coid: The client order id the close is sent under.
        :param volume_cents: The volume (centi-units) of the request.
        :param row: ``(symbol, side)`` of the close's journal row
            (:data:`_CLOSE_ROW_STATE`), written with the record in one
            transaction under ``coid``; ``None`` writes no row.
        :raises JournalWriteError: When the row and the record could not be
            journalled: nothing was written, and the close is not sent.
        """
        record = _OwnClose(position_id=position_id, sent_ms=int(epoch_time() * 1000),
                           volume=volume, coid=coid, volume_cents=volume_cents,
                           from_entry=self._close_dispatch_pine_by_position.get(position_id),
                           settle_entry=self._close_settle_entry_by_position.get(position_id))
        if row is None or self.store_ctx is None or coid is None:
            self._journal_own_close(record)
        else:
            self._journal_close_row(record, symbol=row[0], side=row[1])
        self._own_closes.append(record)
        self._own_close_history.append(record)
        return record

    def _journal_close_row(self, record: _OwnClose, *, symbol: str, side: str) -> None:
        """Write a close's journal row and its record before the request goes out.

        One transaction: the row (:data:`_CLOSE_ROW_STATE`) under the close's
        client order id and the record (:meth:`_journal_own_close`). A row a
        refused or ended earlier send left under the same id is reopened for
        this send. A storage failure rolls both back and is raised as
        :class:`~pynecore.core.broker.exceptions.JournalWriteError`: the close
        was not sent, and no row of it exists.

        :param record: The close about to be sent; its ``coid`` is set.
        :param symbol: The instrument the close trades.
        :param side: The side of the close order.
        """
        store = self.store_ctx
        coid = record.coid
        assert store is not None and coid is not None
        try:
            with store.transaction():
                existing = store.get_order(coid)
                if existing is not None and existing.closed_ts_ms is not None:
                    store.reopen_order(coid)
                store.upsert_order(
                    coid, symbol=symbol, side=side,
                    qty=volume_to_units(record.volume_cents or 0), filled_qty=0.0,
                    state=_CLOSE_ROW_STATE,
                    extras={_CLOSE_ROW_EXTRA: {'position_id': record.position_id,
                                               'volume_cents': record.volume_cents}})
                self._journal_own_close(record)
        except sqlite3.Error as exc:
            raise JournalWriteError("close", exc) from exc

    def _settle_own_close(
            self, record: _OwnClose, order_id: int | None, *,
            answer_fill: _AnswerFill | None = None, complete: bool = False,
    ) -> None:
        """The venue answered the close (or the answer is lost): start its grace.

        The answer's order id is authoritative: the fills taken for the close
        before the answer are judged again in the order they were taken
        (:meth:`_rejudge_claims`). One the answer shows to be another order's
        is judged as the close it is; what it booked that the bot's share did
        not lose comes off its held event and back to the journal
        (:meth:`_release_held_claims`). Without an order id the outcome is
        unknown, and every fill the close took may be someone else's: their
        events are delivered as booked. Until its fills account for the volume
        it asked for, the close may have executed unseen
        (:meth:`_OwnClose.unresolved`).

        :param answer_fill: The fill the answer carried, when it carried one.
        :param complete: Whether the answer is the close order's final fill.
        """
        if order_id:
            record.order_id = order_id
        record.expires_at = time.monotonic() + _OWN_CLOSE_GRACE_S
        record.answered_ms = int(epoch_time() * 1000)
        record.complete = record.complete or complete
        if record.order_id is not None:
            # What the answer shows the close executed, its own fills taken
            # before it included, is journalled with the answer itself: a
            # restarted process knows the close as answered only together
            # with what it executed.
            own = [claim for claim in record.claims
                   if not claim.venue and claim.order_id == record.order_id]
            for claim in own:
                record.count_executed(claim.deal_id, claim.fill_qty)
            if answer_fill is not None and not any(
                    claim.deal_id == answer_fill.deal_id for claim in own):
                record.count_executed(answer_fill.deal_id, answer_fill.closed)
            record.complete = record.complete or any(claim.terminal for claim in own)
        self._journal_own_close(record)
        claims = list(record.claims)
        if record.order_id is None:
            for claim in claims:
                if not claim.venue:
                    self._flag_ambiguous_claim(record, claim)
            self._release_held_claims(record, claims)
            return
        cuts = self._rejudge_claims(record, answer_fill)
        self._release_held_claims(record, claims, cuts)
        if any(claim.terminal for claim in record.claims) and record in self._own_closes:
            self._own_closes.remove(record)

    def _drop_own_close(self, record: _OwnClose) -> None:
        """The venue refused the close: every fill it took was someone else's.

        They are judged like the fills of an answered close
        (:meth:`_rejudge_claims`); what they booked that the bot's share did
        not lose comes off their held events and back to the journal.
        """
        if record in self._own_closes:
            self._own_closes.remove(record)
        if record in self._own_close_history:
            self._own_close_history.remove(record)
        self._journal_own_close(record, refused=True)
        claims = list(record.claims)
        self._release_held_claims(record, claims, self._rejudge_claims(record, None))

    def _rejudge_claims(
            self, record: _OwnClose, answer_fill: _AnswerFill | None,
    ) -> list[tuple[_ClaimedFill, float]]:
        """Judge the fills an own close took before its order id was known.

        The claims are walked in the order they were taken, keeping the
        close's credit: the volume booked — and retired from the journal — that
        the bot's share did not lose. Each claim was booked against the journal
        share of its moment, which every earlier claim had already retired by
        what it booked, so the bot's share then is that journal share plus the
        credit so far. A claim of the close's own order is the bot's close: it
        took the share up to that share, and whatever the close's requested
        volume held back of that is booked short and comes off the credit. Any
        other claim is judged as the close it is (:meth:`_rejudge_claim`); a
        venue close only for what it took of the share, never reported. What a
        claim booked beyond the share it took is credited.

        The answer's own fill, when it came back as the answer rather than as
        a fill taken for the close, is counted by no claim's journal share: a
        claimed fill that executed after it is judged against the share less
        what that fill closed; one whose order against it is not known, as
        either.

        Every claim of another order is dropped from the close. A refused close
        (no order of its own) has none of its own.

        :param answer_fill: The fill the answer carried, when it carried one.
        :return: The volume to take off each claim's held event: the credit at
            the end, over the claims that booked beyond the share they took,
            in the order they were taken.
        """
        credit = 0.0
        own: list[_ClaimedFill] = []
        excess: list[tuple[_ClaimedFill, float]] = []
        ahead = answer_fill
        if ahead is not None and any(claim.deal_id == ahead.deal_id for claim in record.claims):
            ahead = None
        for claim in record.claims:
            if record.order_id is not None and claim.order_id == record.order_id:
                own.append(claim)
                if claim.share_high is not None:
                    owed = min(claim.fill_qty, claim.share_high + credit)
                    credit -= max(0.0, owed - claim.booked)
                continue
            lead_low = lead_high = 0.0
            if ahead is not None:
                after = (ahead.executed_ms is not None and claim.executed_ms is not None
                         and claim.executed_ms > ahead.executed_ms)
                before = (ahead.executed_ms is not None and claim.executed_ms is not None
                          and claim.executed_ms < ahead.executed_ms)
                if not before:
                    lead_high = ahead.closed
                    lead_low = ahead.closed if after else 0.0
            if claim.share_before is None or claim.share_high is None:
                share = None
            else:
                share = (max(0.0, claim.share_before + credit - lead_high),
                         max(0.0, claim.share_high + credit - lead_low))
            if claim.venue:
                taken = (claim.booked if share is None
                         else min(claim.fill_qty, max(0.0, share[0] - claim.venue_after)))
            else:
                taken = self._rejudge_claim(record.position_id, claim, share)
            credit += claim.booked - taken
            if claim.booked - taken > _VOLUME_TOL:
                excess.append((claim, claim.booked - taken))
        record.claims = own
        cuts: list[tuple[_ClaimedFill, float]] = []
        left = max(0.0, credit)
        for claim, extra in excess:
            cut = min(extra, left)
            if cut > _VOLUME_TOL:
                cuts.append((claim, cut))
                left -= cut
        return cuts

    def _release_held_claims(
            self, record: _OwnClose, claims: Sequence[_ClaimedFill],
            cuts: Sequence[tuple[_ClaimedFill, float]] | None = None,
            *, retry: bool = False,
    ) -> None:
        """Deliver the events of the fills an own close took while it was in flight.

        Each event goes to the order-event stream, in the order the fills were
        taken, ahead of anything the stream receives later (the close's own
        answer fill included). A cut is volume a fill booked as the bot's close
        that the bot's share did not lose — another party closed its own
        exposure: it comes off the fill's event, so the engine never books it,
        and the journal gets it back (:meth:`_restore_retired_exposure`). That
        is the bot's own misattribution corrected, not an intervention: a
        WARNING and an audit event, never a report. The share check a held
        partial close skipped runs once its event is delivered.

        A fill the close took as its own carries the close's label and
        settlement entry only provisionally. When the answer (or the refusal)
        shows it to be another order's, it is attributed as if no own close
        had been pending (:meth:`_reattributed_identity`): its event gets that
        label, and what it still debits from the journal moves to the rows of
        that settlement entry. A fill of a close whose outcome stays unknown
        keeps the close's identity. The client order id is never lent with the
        label: each released event carries the id of the close the answer
        proves its order to be (:meth:`_released_claim_coid`), and the
        delivery record of an event whose id changed is written again. Once any released fill's quantity or
        settlement entry changed, the journal debits of all of them are taken
        again in release order (:meth:`_replay_claim_debits`), as the engine
        settles their events.

        A storage failure of those journal writes rolls them back, and the
        fills are put back as they were held, their events included: nothing
        of the release is lost or half applied. The release is kept with the
        verdict it was judged by and retried before the stream delivers
        anything else (:meth:`_retry_failed_releases`). Releases are applied
        in the order they were judged: one judged while an earlier one is
        still pending waits behind it, held as it is.

        :param claims: The fills the close took, as they were before judging.
        :param cuts: The volume to take off each claim's event, once the
            close's fills are judged (:meth:`_rejudge_claims`); ``None`` when
            the close's outcome is unknown.
        :param retry: Whether this is the pending release at the head of
            the queue being retried (:meth:`_retry_failed_releases`).
        """
        if self._failed_releases and not retry:
            self._failed_releases.append((record, list(claims), cuts))
            return
        cut_of = {id(claim): cut for claim, cut in cuts or ()}
        total_cut = 0.0
        restored = 0.0
        released = False
        settle_share = False
        replay = False
        delivered: list[tuple[_ClaimedFill, OrderEvent]] = []
        withdrawn: list[str] = []
        revised: list[OrderEvent] = []
        store = self.store_ctx
        saved = _snapshot_contents(claims)
        # The debits given back and taken again, and the delivery records of the
        # fills they concern, land together: the record of a fill always holds
        # what the position a restart starts from booked for it.
        try:
            with store.transaction() if store is not None else nullcontext():
                for claim in claims:
                    event = claim.event
                    if event is None:
                        continue
                    claim.event = None
                    released = True
                    settle_share = settle_share or claim.settles_share
                    booked = event.fill_qty or 0.0
                    cut = min(cut_of.get(id(claim), 0.0), booked)
                    total_cut += cut
                    if cut > _VOLUME_TOL:
                        restored += self._restore_retired_exposure(record.position_id, claim, cut)
                        replay = True
                    disproved = (cuts is not None and not claim.venue
                                 and not any(own is claim for own in record.claims))
                    if disproved:
                        from_entry, settle_entry = self._reattributed_identity(
                            record.position_id, claim)
                        replay = replay or (
                            settle_entry != claim.settle_entry and bool(claim.retired))
                        claim.settle_entry = settle_entry
                        claim.from_entry = from_entry
                        event = replace(event, from_entry=from_entry)
                    coid = self._released_claim_coid(
                        record, claim, event.order.client_order_id, judged=cuts is not None)
                    recoded = coid != event.order.client_order_id
                    if recoded:
                        event = replace(event, order=replace(event.order, client_order_id=coid))
                    if booked - cut > _VOLUME_TOL:
                        if cut > 0.0:
                            event = replace(event, fill_qty=booked - cut,
                                            journal_fill_qty=booked - cut)
                        if cut > 0.0 or disproved or recoded:
                            revised.append(event)
                        delivered.append((claim, event))
                    elif event.fill_id:
                        # Given back to the journal whole: nothing of it is delivered.
                        withdrawn.append(event.fill_id)
                if replay:
                    self._replay_claim_debits(
                        [claim for claim, _event in delivered], record.position_id)
                self._forget_fill_delivery(withdrawn)
                self._journal_fill_delivery(revised)
        except sqlite3.Error as exc:
            _restore_contents(saved)
            self._failed_releases.append((record, list(claims), cuts))
            logger.error(
                "cTrader: releasing the fills held for the own close of position %d "
                "failed to journal (%s); retried before the next event",
                record.position_id, exc)
            return
        self._released_close_events.extend(event for _claim, event in delivered)
        if total_cut > _VOLUME_TOL:
            logger.warning(
                "cTrader: %s units booked for the bot's own close of position %d were "
                "another party's close of its own exposure; not booked, %s units given "
                "back to the journal", total_cut, record.position_id, restored)
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    _CLAIM_REBOOKED_EVENT, exchange_order_id=str(record.position_id),
                    payload={'position_id': record.position_id, 'volume': total_cut,
                             'restored': restored, 'own_close_key': record.key,
                             'order_id': record.order_id,
                             'claims': [[claim.order_id, claim.deal_id, cut]
                                        for claim, cut in cuts or ()]})
        if settle_share:
            self._settle_own_partial_close(record.position_id)
        if released and self._exec_events is not None:
            # Wakes the stream, which delivers the events before the next message.
            self._exec_events.put_nowait(_RELEASE_WAKEUP)

    def _retry_failed_releases(self) -> None:
        """Release again the held close fills whose release failed to journal.

        Each is released with the verdict it was judged by
        (:meth:`_release_held_claims`), in the order they were judged. The
        first one that fails again stops the round: it and every release
        behind it are kept, in that order, for the next try, so a later
        release never overtakes an earlier one.
        """
        pending, self._failed_releases = self._failed_releases, []
        for index, (record, claims, cuts) in enumerate(pending):
            self._release_held_claims(record, claims, cuts, retry=True)
            if self._failed_releases:
                self._failed_releases.extend(pending[index + 1:])
                return

    def _restore_retired_exposure(
            self, position_id: int, claim: _ClaimedFill, qty: float, *,
            source: str = 'close_claim_rebooked',
    ) -> float:
        """Give back a held fill's journal debits, the last taken first.

        The inverse of :meth:`_retire_partial_close_exposure` for volume
        retired by a fill that turned out not to have closed the bot's share:
        only the rows that fill debited get volume back, each at most what the
        fill took from it, so a debit another close made on a different entry
        stays. A closed row (the venue went flat) gets nothing back.

        :param position_id: The position the fill closed volume of.
        :param claim: The held fill whose booking is cut.
        :param qty: The volume (units) cut off the fill's booking.
        :param source: The audit event's ``source``.
        :return: The volume (units) given back.
        """
        if self.store_ctx is None or not position_id or qty <= 0.0:
            return 0.0
        pid_str = str(position_id)
        remaining = qty
        kept: list[tuple[str, float]] = []
        for index in range(len(claim.retired) - 1, -1, -1):
            coid, taken = claim.retired[index]
            if remaining <= 1e-9:
                kept.insert(0, (coid, taken))
                continue
            row = self.store_ctx.get_order(coid)
            if row is None or row.closed_ts_ms is not None:
                continue
            extras = dict(row.extras or {})
            already_raw = extras.get(JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY)
            already = float(already_raw) if isinstance(already_raw, (int, float)) else 0.0
            give = min(already, taken, remaining)
            if taken - give > 1e-9:
                kept.insert(0, (coid, taken - give))
            if give <= 1e-9:
                continue
            extras[JOURNAL_EXPOSURE_RETIRED_EXTRA_KEY] = already - give
            self.store_ctx.upsert_order(row.client_order_id, extras=extras)
            self.store_ctx.log_event(
                'journal_exposure_restored',
                client_order_id=row.client_order_id, exchange_order_id=pid_str,
                payload={'restored': give, 'total_retired': already - give,
                         'source': source})
            remaining -= give
        claim.retired = kept
        return qty - remaining

    def _reattributed_identity(
            self, position_id: int, claim: _ClaimedFill,
    ) -> tuple[str | None, str | None]:
        """The label and settlement entry of a fill an own close took that was
        another order's.

        The identity the fill gets with no own close pending: a close of the
        run the venue named by the fill's order id gives its own; otherwise
        the run's exposure in the position does, naming its entry only when
        one entry holds it (:meth:`_run_owned_position_entry`), as for a
        close the run did not send.

        :param position_id: The position the fill closed volume of.
        :param claim: The fill.
        :return: ``(from_entry, settle_entry)``.
        """
        other = self._own_close_of_order(claim.order_id, position_id)
        if other is not None:
            return other.from_entry, other.settle_entry
        owners = self._run_owned_position_entry(position_id)
        owner = owners[0] if owners is not None and len(owners) == 1 else None
        return owner, owner

    def _replay_claim_debits(self, claims: Sequence[_ClaimedFill], position_id: int) -> None:
        """Take the journal debits of released fills again, in release order.

        Each fill debited the journal as of its moment, with the identity and
        quantity it was held under; the fills after it took the rows it left.
        Once one of them changed, every one's debits are given back (the last
        taken first), and each takes what it got back again, in the order the
        engine settles their events, for its final settlement entry
        (:attr:`_ClaimedFill.settle_entry`).

        :param claims: The released fills, in release order.
        :param position_id: The position the fills closed volume of.
        """
        volumes: list[float] = []
        for claim in reversed(claims):
            left = sum(volume for _coid, volume in claim.retired)
            volumes.append(self._restore_retired_exposure(
                position_id, claim, left, source='close_claim_reattributed')
                           if left > 1e-9 else 0.0)
        volumes.reverse()
        for claim, left in zip(claims, volumes):
            claim.retired = (
                self._retire_partial_close_exposure(
                    position_id, left, source='close_claim_reattributed',
                    entry_id=claim.settle_entry)
                if left > 1e-9 else [])

    def _settle_own_partial_close(self, position_id: int) -> None:
        """The bot's own close left a position open: check what it holds of it."""
        share = self._own_share(position_id)
        if share is None:
            if not self._hedging_enabled:
                # A startup-adopted NETTING position: a later close of it is
                # no longer one this run sent.
                self._close_dispatch_pine_by_position.pop(position_id, None)
                self._close_settle_entry_by_position.pop(position_id, None)
        elif share <= _VOLUME_TOL:
            # The bot closed its whole share of a position someone else still
            # holds: nothing on it is the bot's any more, its protection
            # included.
            self._vacate_position(position_id)

    def _journal_own_close(
            self, record: _OwnClose, *, refused: bool = False,
            ended: tuple[str, int | None, float] | None = None,
    ) -> None:
        """Persist the state of one own close for a restarted process.

        Journalled when it is sent, answered or refused, and again whenever
        what it executed (:attr:`_OwnClose.executed`) or its completion
        changes: the judgment of an expiry's overlap counts a close confirmed
        filled whose deal the history does not show yet
        (:meth:`~_ReconcileMixin._post_expiry_close_pending`).

        A close with a live journal row (:data:`_CLOSE_ROW_STATE`) has the row
        ended in the same transaction once the record proves its outcome
        (:func:`_close_record_outcome`), or with ``ended`` when the venue
        ended the close's order (:func:`_ended_close_verdict`).

        :param ended: The outcome the venue's end of the close order proves,
            from the order's own final figures.
        """
        store = self.store_ctx
        if store is None:
            return
        with store.transaction():
            store.log_event(
                _OWN_CLOSE_EVENT, client_order_id=record.coid,
                exchange_order_id=str(record.position_id),
                payload={'key': record.key, 'position_id': record.position_id,
                         'sent_ms': record.sent_ms, 'order_id': record.order_id,
                         'answered_ms': record.answered_ms, 'refused': refused,
                         'from_entry': record.from_entry,
                         'settle_entry': record.settle_entry,
                         'coid': record.coid, 'volume_cents': record.volume_cents,
                         'executed': record.executed,
                         'executed_deals': sorted(record.executed_deals),
                         'complete': record.complete},
            )
            if record.coid is None:
                return
            row = store.get_order(record.coid)
            if row is None or row.state != _CLOSE_ROW_STATE or row.closed_ts_ms is not None:
                return
            outcome = ended if ended is not None else _close_record_outcome(
                refused=refused, order_id=record.order_id, executed=record.executed,
                answered=record.answered_ms is not None, complete=record.complete,
                volume=(volume_to_units(record.volume_cents)
                        if record.volume_cents is not None else None))
            if outcome is not None:
                self._finish_close_row(row, *outcome)

    def _finish_close_row(
            self, row: 'OrderRow', outcome: str, order_id: int | None = None,
            executed: float = 0.0,
    ) -> None:
        """End a close's journal row (:data:`_CLOSE_ROW_STATE`) with its proven outcome.

        One transaction (joining the caller's):

        * ``'executed'`` — the close's own fills cover its whole volume, or
          its order's final status shows it executed ``executed`` units: the
          row lands ``confirmed`` and a ``confirmed``
          event names the close's client order id. The row carries no fill
          quantity: the close's fills reach the engine as close fills, and no
          journal walk may take the row for an entry's exposure;
        * ``'refused'`` — the venue refused the close, nothing executed: the
          row lands ``rejected``;
        * ``'ended_partially'`` — the venue reports the close order cancelled
          or expired after executing ``executed`` units, less than the close:
          the row lands ``confirmed`` with the core's ended-partially mark
          (:data:`~pynecore.core.broker.store_helpers.CLOSE_ENDED_PARTIALLY_EVENT`,
          the executed units in the row's quantity unit), which the core reads
          as the leg re-arming for the rest;
        * ``'ended_unfilled'`` — the venue reports the close order cancelled
          or expired with zero executed volume: nothing of it executed and
          nothing can, so the row is closed in its state with the core's
          ended-unfilled mark
          (:data:`~pynecore.core.broker.store_helpers.CLOSE_ENDED_UNFILLED_EVENT`),
          which the core reads as safe to re-arm;
        * ``'answer_lost'``, ``'send_interrupted'``, ``'no_record'`` — no
          evidence shows whether the close executed, and none will: the row is
          closed in its state with an ERROR and a
          :data:`_CLOSE_UNRESOLVED_EVENT`.

        :param row: The live close row.
        :param outcome: The verdict (:func:`_close_record_outcome`).
        :param order_id: The close's venue order, when known.
        :param executed: Units the close executed, for ``'executed'``.
        """
        store = self.store_ctx
        assert store is not None
        coid = row.client_order_id
        extras = dict(row.extras or {})
        detail = dict(extras.get(_CLOSE_ROW_EXTRA) or {})
        detail['outcome'] = outcome
        detail['order_id'] = order_id
        if outcome in ('executed', 'ended_partially'):
            detail['executed'] = executed
        extras[_CLOSE_ROW_EXTRA] = detail
        with store.transaction():
            if outcome == 'executed':
                store.upsert_order(coid, state=STATE_CONFIRMED, extras=extras)
                store.log_event(
                    'confirmed', client_order_id=coid,
                    exchange_order_id=str(order_id) if order_id else None,
                    payload={'executed': executed, 'order_id': order_id,
                             'position_id': detail.get('position_id')})
                store.close_order(coid)
                return
            if outcome == 'refused':
                store.upsert_order(coid, extras=extras)
                mark_rejected(store, coid=coid)
                store.log_event(
                    'rejected', client_order_id=coid,
                    exchange_order_id=str(order_id) if order_id else None,
                    payload={'order_id': order_id, 'position_id': detail.get('position_id')})
                return
            if outcome == 'ended_partially':
                store.upsert_order(coid, state=STATE_CONFIRMED, extras=extras)
                store.log_event(
                    CLOSE_ENDED_PARTIALLY_EVENT, client_order_id=coid,
                    exchange_order_id=str(order_id) if order_id else None,
                    payload={CLOSE_EXECUTED_QTY_KEY: executed, 'order_id': order_id,
                             'position_id': detail.get('position_id')})
                store.close_order(coid)
                return
            if outcome == 'ended_unfilled':
                store.upsert_order(coid, extras=extras)
                store.log_event(
                    CLOSE_ENDED_UNFILLED_EVENT, client_order_id=coid,
                    exchange_order_id=str(order_id) if order_id else None,
                    payload={'order_id': order_id, 'position_id': detail.get('position_id')})
                store.close_order(coid)
                return
            store.upsert_order(coid, extras=extras)
            store.log_event(
                _CLOSE_UNRESOLVED_EVENT, client_order_id=coid,
                exchange_order_id=str(order_id) if order_id else None,
                payload={'reason': outcome, 'order_id': order_id,
                         'position_id': detail.get('position_id')})
            store.close_order(coid)
        logger.error(
            "cTrader: close %s of position %s ended without evidence of whether it "
            "executed (%s)", coid, detail.get('position_id'), outcome)

    def _load_close_evidence(self) -> None:
        """Merge the own closes and judged deals earlier processes of the run journalled.

        Once per journal. Records of this process stay as they are; a close an
        earlier process sent counts as the run's own like one of this process.
        One that process never saw answered is no longer in flight: it claims
        the deals of its grace counted from the send. Manual-close reports an
        earlier process queued but never handled are queued again, and the
        close coverage of every position is taken over
        (:meth:`~_ReconcileMixin._restore_close_coverage`).
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
                    from_entry=(payload['from_entry']
                                if isinstance(payload.get('from_entry'), str) else None),
                    settle_entry=(payload['settle_entry']
                                  if isinstance(payload.get('settle_entry'), str) else None),
                    executed=float(payload.get('executed') or 0.0),
                    executed_deals={int(deal_id) for deal_id in payload.get('executed_deals') or ()
                                    if isinstance(deal_id, int)},
                    complete=bool(payload.get('complete')),
                    coid=payload['coid'] if isinstance(payload.get('coid'), str) else None,
                    volume_cents=(int(payload['volume_cents'])
                                  if isinstance(payload.get('volume_cents'), int) else None),
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
            origins = payload.get('origins')
            if isinstance(origins, dict):
                for deal_ref, origin in origins.items():
                    try:
                        self._judged_close_origins[int(deal_ref)] = CloseOrigin(origin)
                    except (TypeError, ValueError):
                        continue
        self._restore_close_coverage(store)
        self._replay_close_reports(store)

    def _replay_close_reports(self, store: 'RunContext') -> None:
        """Queue the manual-close reports journalled as pending but never handled.

        Pending and handled reports are matched by their own key: a position
        can be reported more than once in a run (an operator resume between).
        """
        handled = {
            payload.get('key') for _intent, _coid, _ref, payload
            in store.iter_events_by_kind_for_run_id(_CLOSE_REPORT_DONE_EVENT)}
        for _intent, coid, position_ref, payload in store.iter_events_by_kind_for_run_id(
                _CLOSE_REPORT_PENDING_EVENT):
            key = payload.get('key')
            position_id = payload.get('position_id')
            reason = payload.get('reason')
            context = payload.get('context')
            if (not isinstance(key, str) or key in handled
                    or key in self._unhandled_close_reports
                    or not isinstance(position_id, int)
                    or not isinstance(reason, str) or not isinstance(context, dict)):
                continue
            self._external_close_reported.add(position_id)
            self._unhandled_close_reports.add(key)
            logger.warning("cTrader: replaying an unhandled report: %s", reason)
            self._deferred_interventions.append((reason, context, coid, position_ref, key))

    def _own_close_by_order(self, order_id: int, *, terminal: bool) -> _OwnClose | None:
        """The close of this run (this process's or an earlier one's) that ``order_id`` is.

        A match by order id is conclusive at any age. A record of this process
        is removed from the live claims on its order's final fill.
        """
        if not order_id:
            return None
        self._load_close_evidence()
        now = time.monotonic()
        self._own_closes = [c for c in self._own_closes if c.expires_at > now]
        match = next((c for c in self._own_closes if c.order_id == order_id), None)
        if match is not None:
            if terminal:
                self._own_closes.remove(match)
            return match
        return next((c for c in self._own_close_history if c.order_id == order_id), None)

    def _own_close_of_order(self, order_id: int, position_id: int) -> _OwnClose | None:
        """The close of this run on ``position_id`` the venue named ``order_id``.

        Attribution only, without the side effects of
        :meth:`_own_close_by_order`: the fills of a close the run sent are its
        own even after the journal closed the position's rows (a fill taken
        for the close that was someone else's flattened the position first).
        """
        if not order_id:
            return None
        self._load_close_evidence()
        return next((c for c in self._own_close_history
                     if c.order_id == order_id and c.position_id == position_id), None)

    def _own_close_of_deal(
            self, position_id: int, deal: _PositionDeal, origin: CloseOrigin | None,
    ) -> _OwnClose | None:
        """The run's close a closing deal of the deal history belongs to: the
        one the venue named its order, else, for a deal judged the bot's own
        (:meth:`_recovered_close_origin`), one of unknown order id whose window
        holds the deal.

        :param origin: The deal's judged origin; a deal of the venue's or
            another client's order never belongs to a close by its window.
        """
        exact = next(
            (record for record in self._own_close_history if record.order_id == deal.order_id),
            None)
        if exact is not None or origin is not CloseOrigin.OWN:
            return exact
        return next(
            (record for record in self._own_close_history
             if record.order_id is None
             and record.claims_recovered(position_id, deal.order_id, deal.executed_ms)),
            None)

    def _own_close_fill_coid(self, order_id: int, position_id: int) -> str | None:
        """The client order id a closing PUSH fill of the run's own close carries.

        ``ProtoOAClosePositionReq`` has no client-order-id field, so the venue
        echoes none on the close's fills; the id the close was sent under (the
        ``coid`` :meth:`~pynecore_ctrader.execution._ExecutionMixin.close_leg`
        or :meth:`~pynecore_ctrader.execution._ExecutionMixin.execute_close`
        received) is kept on its own-close record, which is
        journalled before the request and reloaded by a restarted process. The
        core tells which close a fill reports by it (a netting close from a
        strategy close, a defensive or surplus correction from the rest), so
        a fill carries it only when the venue named the close's order as the
        fill's: the close whose order id is ``order_id``. A fill a close in
        flight takes before its order id is known carries none; it gets the
        id once the answer proves the close's order is the fill's
        (:meth:`_release_held_claims`).

        :return: The close's ``coid``; ``None`` when no close of the run whose
            order is ``order_id`` was sent under one.
        """
        record = self._own_close_of_order(order_id, position_id)
        return record.coid if record is not None else None

    def _released_claim_coid(
            self, record: _OwnClose, claim: _ClaimedFill, current: str | None, *,
            judged: bool,
    ) -> str | None:
        """The client order id the event of a released held fill carries.

        A fill held for a close in flight is delivered with the id of the
        close the answer proves its order to be: ``record``'s own when the
        venue named the fill's order as the close's, another close of the
        run's when that one's order filled it, none when no close of the run
        did. A release whose close outcome stays unknown (no order id) proves
        nothing, so the event keeps what the push gave it
        (:meth:`_own_close_fill_coid`), as does a venue close.

        :param current: The id the held event carries.
        :param judged: Whether the close's answer (or refusal) judged the
            claims.
        """
        if not judged or claim.venue:
            return current
        if record.order_id is not None and claim.order_id == record.order_id:
            return record.coid
        other = self._own_close_of_order(claim.order_id, record.position_id)
        return other.coid if other is not None else None

    def _own_close_wildcard(self, position_id: int) -> _OwnClose | None:
        """A close of this run on the position whose order id is not known yet."""
        now = time.monotonic()
        self._own_closes = [c for c in self._own_closes if c.expires_at > now]
        return next((c for c in self._own_closes
                     if c.position_id == position_id and c.order_id is None), None)

    @staticmethod
    def _is_venue_close(
            message: OpenApiMessages.ProtoOAExecutionEvent,
            order: OpenApiModelMessages.ProtoOAOrder,
    ) -> bool:
        """Whether a closing fill is the venue's own close (SL / TP, stop-out,
        server-initiated): never a close the run sent."""
        return bool(order.orderType == _STOP_LOSS_TAKE_PROFIT
                    or order.isStopOut or message.isServerEvent)

    def _close_fill_origin(
            self, message: OpenApiMessages.ProtoOAExecutionEvent,
            order: OpenApiModelMessages.ProtoOAOrder,
    ) -> tuple[CloseOrigin, _OwnClose | None]:
        """Classify a closing fill on a run-owned position.

        The venue's own executions are checked first, so they never consume a
        pending own-close record; then the run's closes by order id and the
        run's own orders; last a close of the run whose order id is not known
        yet takes the fill as its own.

        :return: The origin, and the own close the fill belongs to: by its
            order id, or taken without knowing it (``order_id`` still
            ``None``; the caller records the claim); else ``None``.
        """
        if self._is_venue_close(message, order):
            return CloseOrigin.VENUE, None
        terminal =message.executionType == OpenApiModelMessages.ProtoOAExecutionType.ORDER_FILLED
        record = self._own_close_by_order(order.orderId, terminal=terminal)
        if record is not None:
            return CloseOrigin.OWN, record
        if self.store_ctx is not None:
            if self._is_run_client_order_id(order.clientOrderId):
                return CloseOrigin.OWN, None
            if self._is_own_entry_order(order.orderId):
                return CloseOrigin.OWN, None
        wildcard = self._own_close_wildcard(order.positionId)
        if wildcard is not None:
            return CloseOrigin.OWN, wildcard
        return CloseOrigin.EXTERNAL, None

    def _record_claim(self, record: _OwnClose, claim: _ClaimedFill) -> None:
        """Remember a fill an own close took before its order id was known."""
        record.claims.append(claim)
        if record.answered_ms is not None and record.order_id is None and not claim.venue:
            self._flag_ambiguous_claim(record, claim)

    def _flag_ambiguous_claim(self, record: _OwnClose, claim: _ClaimedFill) -> None:
        """A fill was taken for an own close whose outcome is unknown.

        Not proof of an intervention — the close may be the one that filled —
        so it is booked as the bot's own, logged as an ERROR and audited. On a
        HEDGED account the closing orders of the fills taken can settle it
        later (``_judge_ambiguous_close_claims``).
        """
        logger.error(
            "cTrader: order %d closed %s units of bot position %d while the bot's own "
            "close of it has an unknown outcome; booked as the bot's own close, but it "
            "may be someone else's", claim.order_id, claim.fill_qty, record.position_id)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _CLAIM_AMBIGUOUS_EVENT, exchange_order_id=str(record.position_id),
                payload={'position_id': record.position_id, 'order_id': claim.order_id,
                         'deal_id': claim.deal_id, 'closed_qty': claim.fill_qty,
                         'own_close_key': record.key, 'sent_ms': record.sent_ms})
        self._ambiguous_closes[record.position_id] = record

    def _rejudge_claim(
            self, position_id: int, claim: _ClaimedFill,
            share: tuple[float, float] | None,
    ) -> float:
        """Judge a fill an own close took that turned out to be another order's.

        The fill was already booked as the bot's own close; a report is added
        when it was someone else's close of the bot's position: one that took
        the venue below the bot's share; one that may have, as the evidence
        cannot decide, is logged as an ERROR and audited instead. When the
        share is not known, a HEDGED position has one owner and the close is
        reported; on a NETTING account it is logged as an ERROR and audited
        (:meth:`_note_share_unknown`).

        :param share: ``(low, high)`` bounds of the bot's share just before
            the fill; ``None`` when the share is not known.
        :return: The volume (units) of the bot's share the fill provably took;
            all it booked when the share is not known or the order is own.
        """
        store = self.store_ctx
        if store is not None and (self._is_run_client_order_id(claim.client_order_id)
                                  or self._is_own_entry_order(claim.order_id)):
            return claim.booked
        if any(c.order_id == claim.order_id for c in self._own_close_history):
            return claim.booked
        context = {
            'position_id': position_id,
            'order_id': claim.order_id,
            'deal_id': claim.deal_id,
            'closed_qty': claim.fill_qty,
            'order_type': claim.order_type,
            'from_entry': claim.from_entry,
            'claimed_as_own': True,
        }
        if share is None:
            taken = claim.booked
            if self._hedging_enabled:
                self._queue_external_close(position_id, context)
            else:
                self._note_share_unknown(
                    position_id, order_id=claim.order_id, deal_ids=[claim.deal_id],
                    closed_qty=claim.fill_qty, booked_qty=taken, from_entry=claim.from_entry)
        else:
            low, high = share
            taken = min(claim.fill_qty, max(0.0, low - claim.venue_after))
            taken_high = min(claim.fill_qty, max(0.0, high - claim.venue_after))
            context.update({'share': low if low == high else [low, high],
                            'venue_after': claim.venue_after})
            if taken > _VOLUME_TOL:
                context['booked_qty'] = taken
                self._queue_external_close(position_id, context)
            elif taken_high > _VOLUME_TOL:
                self._note_share_ambiguous(
                    position_id, [claim.deal_id], low=taken, high=taken_high,
                    closed_qty=claim.fill_qty)
            else:
                logger.info(
                    "cTrader: order %d closed only another party's exposure on bot "
                    "position %d", claim.order_id, position_id)
        # Queued (and journalled) before the deal is marked judged.
        if claim.deal_id is not None:
            self._mark_close_judged([claim.deal_id], {claim.deal_id: CloseOrigin.EXTERNAL})
        return taken

    def _note_server_close(
            self, message: OpenApiMessages.ProtoOAExecutionEvent,
            order: OpenApiModelMessages.ProtoOAOrder, fill_qty: float,
    ) -> None:
        """Book a server-initiated close that is neither the SL / TP nor a stop-out.

        Venue-side (a broker action), so never an intervention; booked with a
        WARNING and a durable audit event.
        """
        deal_id = message.deal.dealId if message.HasField('deal') else None
        logger.warning(
            "cTrader: the venue closed %s units of bot position %d on its own (order %d, "
            "deal %s); booked as a venue close", fill_qty, order.positionId,
            order.orderId, deal_id)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _SERVER_CLOSE_EVENT, exchange_order_id=str(order.positionId),
                payload={'position_id': order.positionId, 'order_id': order.orderId,
                         'deal_id': deal_id, 'closed_qty': fill_qty,
                         'order_type': int(order.orderType)})

    def _note_share_unknown(
            self, position_id: int, *, order_id: int, deal_ids: Iterable[int | None],
            closed_qty: float, booked_qty: float, from_entry: str | None,
            recovered: bool = False,
    ) -> None:
        """A NETTING close of a position whose bot share is not known: ERROR + audit.

        No live entry row of the run links the position (a startup-adopted
        position the run closes through its dispatch record), so nothing
        proves the close took the bot's part of it rather than another
        party's: it is booked as the venue shows it, never reported.
        """
        ids = [deal_id for deal_id in deal_ids if deal_id is not None]
        logger.error(
            "cTrader: order %d closed %s units of bot position %d, whose share of the bot is "
            "not known (no entry row of the run links it); booked as the venue shows it, "
            "not reported: nothing proves it took the bot's part", order_id, closed_qty,
            position_id)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _SHARE_UNKNOWN_EVENT, exchange_order_id=str(position_id),
                payload={'position_id': position_id, 'order_id': order_id, 'deal_ids': ids,
                         'closed_qty': closed_qty, 'booked_qty': booked_qty,
                         'from_entry': from_entry, 'recovered': recovered})

    def _note_share_ambiguous(
            self, position_id: int, deal_ids: Iterable[int | None], *,
            low: float, high: float, closed_qty: float,
    ) -> None:
        """A close's effect on the bot's share cannot be decided: ERROR + audit."""
        ids = [deal_id for deal_id in deal_ids if deal_id is not None]
        logger.error(
            "cTrader: deals %s closed %s units of bot position %d; they took between "
            "%s and %s units of the bot's share, which the evidence cannot decide — not "
            "reported", ids, closed_qty, position_id, low, high)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _SHARE_AMBIGUOUS_EVENT, exchange_order_id=str(position_id),
                payload={'position_id': position_id, 'deal_ids': ids, 'low': low,
                         'high': high, 'closed_qty': closed_qty})

    def _defer_external_close(
            self, message: OpenApiMessages.ProtoOAExecutionEvent,
            order: OpenApiModelMessages.ProtoOAOrder, *,
            from_entry: str | None, fill_qty: float, share: dict | None = None,
    ) -> None:
        """Queue the report of a manual close, once per position.

        Reported by :meth:`_flush_external_interventions` once the stream has
        yielded the fill event, so the event reaches the engine's queue before
        a halting policy raises; the engine books it before the run stops.

        :param share: ``share`` / ``venue_after`` / ``booked_qty`` when the share
            is known.
        """
        self._queue_external_close(order.positionId, {
            'position_id': order.positionId,
            'order_id': order.orderId,
            'deal_id': message.deal.dealId if message.HasField('deal') else None,
            'closed_qty': fill_qty,
            'order_type': int(order.orderType),
            'from_entry': from_entry,
            **(share or {}),
        })

    def _queue_external_close(
            self, position_id: int, context: dict, *, reason: str | None = None,
    ) -> None:
        """Queue one manual-close report, unless one of the position waits already.

        Venue writes wait until the report is handled
        (:meth:`ensure_venue_writes_allowed`). Once it is, a later close of
        the position is judged and reported on its own: under the ``ignore``
        and ``re_place`` policies the run goes on trading.
        """
        if position_id in self._external_close_reported:
            return
        self._external_close_reported.add(position_id)
        if reason is None:
            reason = (
                f"bot position {position_id} was closed on the venue by someone else: "
                f"{context['closed_qty']} units by order {context['order_id']}"
            )
        logger.warning("cTrader: %s", reason)
        coid = self._protection_row(position_id)
        key = uuid.uuid4().hex
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _CLOSE_REPORT_PENDING_EVENT, client_order_id=coid,
                exchange_order_id=str(position_id),
                payload={'key': key, 'position_id': position_id, 'reason': reason,
                         'context': context},
            )
        self._unhandled_close_reports.add(key)
        self._deferred_interventions.append((reason, context, coid, str(position_id), key))

    def _mark_close_judged(
            self, deal_ids: list[int], origins: Mapping[int, CloseOrigin] | None = None,
    ) -> None:
        """Closing deals whose origin is decided; never judged again by any process.

        The origin is recorded with the deal, so a later replay of the position
        knows what the deal did to the bot's share.
        """
        origins = origins or {}
        fresh = [deal_id for deal_id in deal_ids
                 if deal_id not in self._judged_close_deals
                 or (deal_id in origins
                     and self._judged_close_origins.get(deal_id) is not origins[deal_id])]
        if not fresh:
            return
        self._judged_close_deals.update(fresh)
        recorded = {deal_id: origins[deal_id] for deal_id in fresh if deal_id in origins}
        self._judged_close_origins.update(recorded)
        if self.store_ctx is not None:
            self.store_ctx.log_event(_CLOSE_JUDGED_EVENT, payload={
                'deal_ids': fresh,
                'origins': {str(deal_id): str(origin) for deal_id, origin in recorded.items()},
            })

    async def _judge_recovered_closes(
            self, position_id: int, deals: Sequence[_PositionDeal], *,
            from_entry: str | None, venue_now: float = 0.0,
            share_now: float | None = None, counted: Iterable[int] = (),
            own_orders: Container[int] = (),
            settle: Callable[[float], None] | None = None,
    ) -> float | None:
        """Apply the intervention policy to closes the PUSH stream missed.

        Called when the deal history retires a run-owned position, when a
        restart finds a run-owned position gone or shrunk, and when an open
        position holds less than the bot's share. Every closing deal no
        process of the run attributed or judged yet is classified. An
        external one queues a report like a manual close seen live when it
        took the venue below the bot's share (:func:`_replay_share_closes`);
        one that may have is logged as an ERROR and audited. When the share is
        not known, a HEDGED position has one owner, so every external close is
        reported; on a NETTING account nothing proves one took the bot's part,
        so it is logged as an ERROR and audited (:meth:`_note_share_unknown`).
        An unattributed one that took the bot's volume is logged as an ERROR
        and audited, never reported: nothing proves it was not the venue's.

        :param position_id: The run-owned position.
        :param deals: Every deal on the position the deal history holds for the
            window, from the earliest own entry on.
        :param from_entry: The Pine entry the position belongs to.
        :param venue_now: The volume (units) the position holds now; ``0.0``
            for a position that is gone.
        :param share_now: The bot's share (units) before these closes; the
            journal share when ``None``.
        :param counted: Deals of the bot's own fills ``share_now`` counts
            beyond those the event stream delivered.
        :param own_orders: Entry orders of the bot's whose journal rows are
            closed: a closed row no longer resolves its order.
        :param settle: The caller's writes that apply the verdict, given the
            volume taken; they share the transaction that records the deals
            judged (:meth:`_judge_recovered_close_deals`).
        :return: The volume (units) of the bot's share the deals judged now
            took: exactly when the replay covers the position's whole life,
            else as far as the evidence proves it (when the share is not
            known: all they closed). ``None`` when an order lookup failed for
            now: nothing was judged, and the caller must not retire the
            position this pass.
        """
        replay = await self._judge_recovered_close_deals(
            position_id, deals, from_entry=from_entry, venue_now=venue_now,
            share_now=share_now, counted=counted, own_orders=own_orders,
            settle=None if settle is None else (
                lambda judged: settle(volume_to_units(judged.taken))))
        return None if replay is None else volume_to_units(replay.taken)

    async def _judge_recovered_close_deals(
            self, position_id: int, deals: Sequence[_PositionDeal], *,
            from_entry: str | None, venue_now: float = 0.0,
            share_now: float | None = None, counted: Iterable[int] = (),
            own_orders: Container[int] = (),
            settle: Callable[[_ShareReplay], None] | None = None,
    ) -> _ShareReplay | None:
        """Judge closes the PUSH stream missed, keeping the verdict per deal.

        Applies the policy of :meth:`_judge_recovered_closes`, which takes the
        same parameters.

        :param settle: The caller's writes that apply the verdict (the debits,
            retirements and coverage it books). They run in the store
            transaction that records the deals as judged, so a process that
            stops before they land leaves the deals unjudged, and the next
            process judges them again from the history.
        :return: The verdict: :attr:`_ShareReplay.booked` names the closes of
            the bot's share the deals judged now stand for, and
            :attr:`_ShareReplay.taken` their sum. Empty when nothing was left
            to judge (the deals were judged earlier, or the position's
            external close was reported already). ``None`` when an order
            lookup failed for now, or when a close of the bot's share the
            verdict would deliver has no price in the history yet
            (:class:`_UnpricedCloseDeferred`): nothing was judged, and
            ``settle`` is not called.
        """
        if position_id in self._external_close_reported:
            return self._settle_close_judgment(_ShareReplay(), settle)
        self._load_close_evidence()
        deals = sorted(deals, key=lambda deal: (deal.executed_ms, deal.deal_id))
        pending = [deal for deal in deals
                   if deal.closed > 0 and deal.deal_id not in self._judged_close_deals]
        if not pending:
            return self._settle_close_judgment(_ShareReplay(), settle)
        origins: dict[int, CloseOrigin] = {}
        by_order: dict[int, CloseOrigin] = {}
        for deal in pending:
            origin = by_order.get(deal.order_id)
            if origin is None:
                origin = await self._recovered_close_origin(
                    position_id, deal.order_id, deal.executed_ms, own_orders=own_orders)
                if origin is None:
                    return None
                by_order[deal.order_id] = origin
            origins[deal.deal_id] = origin
        try:
            with self._judgment_span():
                replay = self._apply_close_judgment(
                    position_id, deals, pending, origins, from_entry=from_entry,
                    venue_now=venue_now, share_now=share_now, counted=counted,
                    own_orders=own_orders)
                unpriced = sorted(deal.deal_id for deal, _cents in replay.booked
                                  if deal.price <= 0.0
                                  and deal.deal_id not in self._seen_deal_ids)
                if unpriced:
                    raise _UnpricedCloseDeferred(unpriced)
                if settle is not None:
                    settle(replay)
        except _UnpricedCloseDeferred as deferred:
            logger.warning(
                "cTrader: closes of the bot's share on position %d lack a price in the "
                "deal history (%s); judged once they are priced", position_id, deferred.deal_ids)
            return None
        return replay

    @contextmanager
    def _judgment_span(self) -> Iterator[None]:
        """The store transaction a close judgment and the writes applying it share.

        A judgment that fails rolls back its journal writes, and the state this
        process keeps of them is put back as it was (:data:`_JUDGMENT_STATE`):
        a pass that retries in the same process judges the deals again, as a
        new process would. It is put back in place (:func:`_restore_contents`):
        a request waiting for its answer holds its close record across the
        rollback, and its answer must settle the record the run tracks. The
        close evidence earlier processes journalled is merged first
        (:meth:`_load_close_evidence`): a merge the rollback undid would stay
        marked done, and the run would lose that evidence.
        """
        self._load_close_evidence()
        roots = {name: getattr(self, name) for name in _JUDGMENT_STATE}
        saved = _snapshot_contents(roots.values())
        try:
            with self.store_ctx.transaction() if self.store_ctx is not None else nullcontext():
                yield
        except BaseException:
            _restore_contents(saved)
            for name, value in roots.items():
                setattr(self, name, value)
            raise

    def _settle_close_judgment(
            self, replay: _ShareReplay, settle: Callable[[_ShareReplay], None] | None,
    ) -> _ShareReplay:
        """Apply a verdict that judged nothing new, in one transaction like any other."""
        if settle is not None:
            with self._judgment_span():
                settle(replay)
        return replay

    def _journal_fill_delivery(self, events: Iterable[OrderEvent]) -> None:
        """Record fills whose journal write lands in the current transaction, until the engine takes them.

        Called in the transaction of the journal write that puts each fill into
        the position a restart starts from (a raised fill cursor, a debit, a
        retirement), so a record exists exactly when that write does
        (:data:`_FILL_DELIVERY_EVENT`). A fill the engine takes gets its
        receipt, and :meth:`_settle_deliveries` drops the record then; a later
        process finds the records of the fills still without one
        (:meth:`_take_inherited_deliveries`). Only a fill the engine gives a
        receipt counts: one with a fill id, a quantity and a price.

        :param events: The fill events about to be delivered.
        """
        store = self.store_ctx
        if store is None:
            return
        self._inherited_delivery_keys()
        values = {
            f'fill:{event.fill_id}': _delivery_payload(event)
            for event in events
            if (event.fill_id and event.event_type in ('filled', 'partial')
                and (event.fill_qty or 0.0) > 0.0 and (event.fill_price or 0.0) > 0.0)}
        if values:
            store.replace_keyed_events(_FILL_DELIVERY_EVENT, values)

    def _forget_fill_delivery(self, fill_ids: Iterable[str]) -> None:
        """Drop the delivery records of fills that will not be delivered after all.

        :param fill_ids: Fills whose journal write was given back, so the
            position a restart starts from does not hold them.
        """
        removed = [f'fill:{fill_id}' for fill_id in fill_ids]
        if removed and self.store_ctx is not None:
            self.store_ctx.replace_keyed_events(_FILL_DELIVERY_EVENT, {}, removed)

    def _journal_fee_delivery(self, event: OrderEvent) -> None:
        """Record a commission delivered without its fill, until the engine accounted it.

        Called in the transaction that takes the commission off the journal's
        unbooked commission, so it is delivered from the record, or charged by
        a recovery from the journal, never both (:data:`_FEE_DELIVERY_EVENT`).

        :param event: The fee event (``event_type`` ``'fee'``): its ``fee`` is
            the fill's whole commission.
        """
        if self.store_ctx is not None and event.fill_id:
            self._inherited_delivery_keys()
            self.store_ctx.replace_keyed_events(
                _FEE_DELIVERY_EVENT, {f'fee:{event.fill_id}': _delivery_payload(event)})

    def _inherited_delivery_keys(self) -> set[str]:
        """The keys of the delivery records earlier processes of the run left.

        Read once, before this process writes a record of its own
        (:meth:`_journal_fill_delivery`, :meth:`_journal_fee_delivery`) or
        first starts its event stream, whichever comes first: every record
        found then was written by an earlier process.
        """
        keys = self._inherited_deliveries
        if keys is None:
            keys = set()
            if self.store_ctx is not None:
                keys.update(self.store_ctx.keyed_events_for_run_id(_FILL_DELIVERY_EVENT))
                keys.update(self.store_ctx.keyed_events_for_run_id(_FEE_DELIVERY_EVENT))
            self._inherited_deliveries = keys
        return keys

    def _fee_accounted(self, event: OrderEvent) -> bool:
        """Whether the engine's receipt of a fee event's fill accounts its whole commission."""
        assert self.store_ctx is not None and event.fill_id is not None
        receipt = self.store_ctx.fill_receipt(event.fill_id)
        return (receipt is not None
                and abs(receipt.fee - event.fee) <= 1e-9 * max(1.0, abs(event.fee)))

    def _take_inherited_deliveries(self) -> list[OrderEvent]:
        """The fills and commissions earlier processes of the run delivered and the engine never took.

        Taken once per process, when the event stream first starts, and only
        the records an earlier process wrote (:meth:`_inherited_delivery_keys`):
        never set the mark on a fill this process journaled. A fill record
        whose fill the engine took
        (:meth:`~pynecore.core.broker.storage.RunContext.fill_delivered`) is
        dropped; any other fill is delivered again marked
        :attr:`~OrderEvent.journaled_before_restart`:
        its journal write put it into the position this process started with,
        so the engine records its receipt and books nothing, and the record is
        dropped once the receipt shows (:meth:`_settle_deliveries`). A
        commission record is dropped once the receipt accounts the whole
        commission, and delivered again otherwise: the engine books only what
        it has not accounted yet.

        :return: The events to deliver first, in the order they were recorded.
        """
        store = self.store_ctx
        if store is None or self._deliveries_inherited:
            return []
        self._deliveries_inherited = True
        inherited = self._inherited_delivery_keys()
        events: list[OrderEvent] = []
        taken: list[str] = []
        for key, payload in store.keyed_events_for_run_id(_FILL_DELIVERY_EVENT).items():
            if key not in inherited:
                continue
            event = _delivered_event(payload)
            if event is None or store.fill_delivered(cast(str, event.fill_id)):
                taken.append(key)
            else:
                events.append(replace(self._with_proven_close_coid(event),
                                      journaled_before_restart=True))
        if taken:
            store.replace_keyed_events(_FILL_DELIVERY_EVENT, {}, taken)
        accounted: list[str] = []
        for key, payload in store.keyed_events_for_run_id(_FEE_DELIVERY_EVENT).items():
            if key not in inherited:
                continue
            event = _delivered_event(payload)
            if event is None or self._fee_accounted(event):
                accounted.append(key)
            else:
                events.append(event)
        if accounted:
            store.replace_keyed_events(_FEE_DELIVERY_EVENT, {}, accounted)
        return events

    def _with_proven_close_coid(self, event: OrderEvent) -> OrderEvent:
        """An inherited close fill with the client order id its order proves.

        A fill held for a close in flight is journaled without an id; the
        close's answer journals the close's order id before the release
        rewrites the fill's delivery record with the id
        (:meth:`_release_held_claims`). A process that stopped between the two
        left the record without it, though the journal proves whose fill it
        is: the run's close whose order the venue named as the fill's order
        (order ids are unique on the account), as on the push path
        (:meth:`_own_close_fill_coid`).

        :param event: A fill event read back from its delivery record.
        :return: The event, carrying the proven id when it had none.
        """
        if event.leg_type is not LegType.CLOSE or event.order.client_order_id is not None:
            return event
        try:
            order_id = int(event.order.id)
        except ValueError:
            return event
        self._load_close_evidence()
        coid = next((record.coid for record in self._own_close_history
                     if order_id and record.order_id == order_id), None)
        if coid is None:
            return event
        return replace(event, order=replace(event.order, client_order_id=coid))

    def _settle_deliveries(self) -> list[OrderEvent]:
        """Drop the delivery records the engine settled; the commissions to deliver again.

        A fill record goes once the engine's receipt shows the fill taken. A
        commission record goes once the receipt accounts the whole commission.
        One the engine refused because it held the fill for the venue's verdict
        is delivered again once, after the fill was taken
        (:attr:`~_CTraderBase._fees_resent`); a commission no trade could
        take waits for the next process.

        :return: The fee events to deliver again.
        """
        store = self.store_ctx
        if store is None:
            return []
        taken = [key for key, payload in store.keyed_events_for_run_id(_FILL_DELIVERY_EVENT).items()
                 if (event := _delivered_event(payload)) is None
                 or store.fill_delivered(cast(str, event.fill_id))]
        if taken:
            store.replace_keyed_events(_FILL_DELIVERY_EVENT, {}, taken)
        accounted: list[str] = []
        again: list[OrderEvent] = []
        for key, payload in store.keyed_events_for_run_id(_FEE_DELIVERY_EVENT).items():
            event = _delivered_event(payload)
            if event is None or self._fee_accounted(event):
                accounted.append(key)
            elif (store.fill_delivered(cast(str, event.fill_id))
                  and event.fill_id not in self._fees_resent):
                self._fees_resent.add(cast(str, event.fill_id))
                again.append(event)
        if accounted:
            store.replace_keyed_events(_FEE_DELIVERY_EVENT, {}, accounted)
        return again

    def _apply_close_judgment(
            self, position_id: int, deals: Sequence[_PositionDeal],
            pending: Sequence[_PositionDeal], origins: Mapping[int, CloseOrigin], *,
            from_entry: str | None, venue_now: float, share_now: float | None,
            counted: Iterable[int], own_orders: Container[int],
    ) -> _ShareReplay:
        """Judge the pending closing deals whose origins are known, and record them judged.

        The synchronous part of :meth:`_judge_recovered_close_deals`, which
        takes the same parameters; ``deals`` is sorted by execution.

        :param pending: The closing deals of ``deals`` no process judged yet.
        :param origins: The origin of every pending deal.
        """
        share = share_now if share_now is not None else self._own_share(position_id)
        if share is None:
            replay = _ShareReplay(
                reported=[(deal, deal.closed) for deal in pending
                          if origins[deal.deal_id] in _FOREIGN_ORIGINS],
                taken=sum(deal.closed for deal in pending),
                booked=[(deal, deal.closed) for deal in pending])
        else:
            replay = _replay_share_closes(
                deals, pending=origins, judged=self._judged_close_origins,
                own_opening=lambda deal: (deal.order_id in own_orders
                                          or self._is_own_entry_order(deal.order_id)),
                seen=self._seen_deal_ids | set(counted), venue_now=raw_volume(venue_now),
                share_now=raw_volume(share),
                share_low_now=raw_volume(max(
                    0.0, share - self._own_close_missing_from(position_id, deals))))
        accepted_until = self._resume_accepted_until.get(position_id, 0)
        if accepted_until:
            # An operator resume accepted the closes executed before it, as
            # the venue showed them: none is another party's to report.
            replay.reported = [(deal, taken) for deal, taken in replay.reported
                               if deal.executed_ms >= accepted_until]
            replay.ambiguous = [entry for entry in replay.ambiguous
                                if entry[0].executed_ms >= accepted_until]
        external = [(deal, taken) for deal, taken in replay.reported
                    if origins[deal.deal_id] is CloseOrigin.EXTERNAL]
        unattributed = [(deal, taken) for deal, taken in replay.reported
                        if origins[deal.deal_id] is not CloseOrigin.EXTERNAL]
        if external and share is None and not self._hedging_enabled:
            first = external[0][0]
            self._note_share_unknown(
                position_id, order_id=first.order_id,
                deal_ids=[deal.deal_id for deal, _taken in external],
                closed_qty=volume_to_units(sum(deal.closed for deal, _ in external)),
                booked_qty=volume_to_units(sum(taken for _deal, taken in external)),
                from_entry=from_entry, recovered=True)
        elif external:
            first = external[0][0]
            context = {
                'position_id': position_id,
                'order_id': first.order_id,
                'deal_id': first.deal_id,
                'deal_ids': [deal.deal_id for deal, _taken in external],
                'closed_qty': volume_to_units(sum(deal.closed for deal, _ in external)),
                'booked_qty': volume_to_units(sum(taken for _deal, taken in external)),
                'from_entry': from_entry,
                'recovered': True,
            }
            fired = next((self._fired_foreign_orders[deal.order_id] for deal, _taken in external
                          if deal.order_id in self._fired_foreign_orders), None)
            reason = None
            if fired is not None:
                context.update(fired)
                reason = (
                    f"bot position {position_id} was closed while the stream was down by a "
                    f"protection the bot did not set: stop {fired['fired_stop']}, take-profit "
                    f"{fired['fired_limit']} fired (order {fired['fired_order_id']})")
            self._queue_external_close(position_id, context, reason=reason)
        if unattributed:
            self._note_unattributed_close(position_id, unattributed, from_entry=from_entry)
        if replay.ambiguous:
            self._note_share_ambiguous(
                position_id, [deal.deal_id for deal, _low, _high in replay.ambiguous],
                low=volume_to_units(sum(low for _deal, low, _high in replay.ambiguous)),
                high=volume_to_units(sum(high for _deal, _low, high in replay.ambiguous)),
                closed_qty=volume_to_units(sum(deal.closed for deal, _, _ in replay.ambiguous)))
        for deal in pending:
            if origins[deal.deal_id] is CloseOrigin.OWN:
                owner = self._own_close_of_deal(position_id, deal, CloseOrigin.OWN)
                if owner is not None and deal.deal_id not in owner.executed_deals:
                    owner.count_executed(deal.deal_id, volume_to_units(deal.closed))
                    self._journal_own_close(owner)
        self._mark_close_judged([deal.deal_id for deal in pending], origins)
        return replay

    def _note_unattributed_close(
            self, position_id: int, deals: Sequence[tuple[_PositionDeal, int]], *,
            from_entry: str | None,
    ) -> None:
        """Book recovered closes nothing proves someone else's: ERROR + audit, no report.

        Their orders are neither the bot's nor the venue's SL / TP or
        stop-out and carry no client-order-id, or the venue no longer knows
        them (:attr:`CloseOrigin.UNKNOWN`).
        """
        deal_ids = [deal.deal_id for deal, _taken in deals]
        booked = volume_to_units(sum(taken for _deal, taken in deals))
        logger.error(
            "cTrader: deals %s closed %s units of bot position %d while the stream was "
            "down; their orders are neither the bot's nor the venue's SL / TP or stop-out "
            "and carry no client-order-id, or the venue no longer knows them, so another "
            "client's close cannot be told from a server-initiated one — booked, not "
            "reported", deal_ids, booked, position_id)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _CLOSE_UNATTRIBUTED_EVENT, exchange_order_id=str(position_id),
                payload={'position_id': position_id, 'deal_ids': deal_ids,
                         'order_ids': sorted({deal.order_id for deal, _taken in deals}),
                         'closed_qty': volume_to_units(sum(deal.closed for deal, _ in deals)),
                         'booked_qty': booked, 'from_entry': from_entry})

    def _note_accepted_by_resume(
            self, position_id: int, *, client_order_id: str | None, volume: float | None,
            deal_ids: Iterable[int] = (),
    ) -> None:
        """A close of the bot's position an operator resume accepted: WARNING + audit."""
        ids = sorted(deal_ids)
        logger.warning(
            "cTrader: bot position %d was closed or reduced (%s units) before the operator "
            "resumed the run; the resume accepted the venue as it stands, not reported",
            position_id, volume if volume is not None else 'all')
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _CLOSE_ACCEPTED_BY_RESUME_EVENT, client_order_id=client_order_id,
                exchange_order_id=str(position_id),
                payload={'position_id': position_id, 'volume': volume, 'deal_ids': ids})

    async def _recovered_close_origin(
            self, position_id: int, order_id: int, executed_ms: int, *,
            own_orders: Container[int] = (),
    ) -> CloseOrigin | None:
        """Classify one closing order found in the deal history.

        A close of the run by order id, or one of the bot's entry orders, is
        *own* at once. A close of the run whose outcome is unknown claims a
        deal by position and time only, so the order is looked up first: the
        venue's SL / TP or stop-out and another client's order keep their
        verdict, and only an order the lookup cannot attribute
        (*unattributed*, *unknown*) is taken as that close's.

        :param own_orders: Entry orders of the bot's whose journal rows are
            closed (:meth:`_judge_recovered_closes`).
        :return: The origin, or ``None`` when the order lookup failed for now.
        """
        if order_id in own_orders:
            return CloseOrigin.OWN
        kind = await self._closing_order_kind(position_id, order_id, judge_protection=True)
        if kind in (CloseOrigin.UNATTRIBUTED, CloseOrigin.UNKNOWN) and any(
                record.claims_recovered(position_id, order_id, executed_ms)
                for record in self._own_close_history):
            return CloseOrigin.OWN
        return kind

    async def _recovered_protection_origin(
            self, position_id: int, order: OpenApiModelMessages.ProtoOAOrder,
    ) -> CloseOrigin:
        """Classify a recovered close by the position's own SL / TP order.

        The venue's mechanics, unless the protection that fired was not the
        bot's (:meth:`_fired_protection`): a price the bot never set, or any
        protection on a position the bot left bare, is *external*, as on the
        live path. A fired order that reports no level after a foreign venue
        view, or one that fired before an operator resume's baseline was
        taken, is the venue's, with an ERROR / WARNING and an audit event.
        """
        stop = order.stopPrice or None
        limit = order.limitPrice or None
        tick = self._cached_tick(order.tradeData.symbolId)
        if (tick is None and (stop is not None or limit is not None)
                and not self._is_bare(position_id)
                and (position_id in self._unguarded_baseline
                     or self._protection_guard().known(protection_key(position_id)))):
            tick = await self._resolve_tick(order.tradeData.symbolId)
        verdict, own = self._fired_protection(position_id, stop, limit, tick=tick)
        context = {
            'position_id': position_id,
            'order_id': order.orderId,
            'order_type': int(order.orderType),
            'fired_stop': stop,
            'fired_limit': limit,
            'recovered': True,
        }
        if verdict is FiredProtection.AFTER_RESUME:
            self._note_fired_after_resume(position_id, context)
        elif verdict is FiredProtection.UNCONFIRMED:
            self._note_fired_unconfirmed(position_id, context)
        elif verdict in (FiredProtection.FOREIGN, FiredProtection.BARE):
            fired: dict = {'fired_order_id': order.orderId, 'fired_stop': stop,
                           'fired_limit': limit}
            if verdict is FiredProtection.BARE:
                fired['bare'] = True
            else:
                fired['own_levels'] = [level.as_list() for level in own]
            self._fired_foreign_orders[order.orderId] = fired
            return CloseOrigin.EXTERNAL
        return CloseOrigin.VENUE

    async def _closing_order_kind(
            self, position_id: int, order_id: int, *, judge_protection: bool = False,
    ) -> CloseOrigin | None:
        """Classify a closing order by what is known of it, without the own-close grace.

        *own* when it is one of this run's orders (its journal row, or a
        client-order-id carrying the run's tag: :meth:`_is_run_client_order_id`)
        or a close the run sent with that order id; *venue* when ``ProtoOAOrderDetailsReq`` shows the
        position's ``STOP_LOSS_TAKE_PROFIT`` order or a stop-out; *external*
        when the order carries a client-order-id that is not this run's (an
        API client sent it, the venue never does); *unknown* when the venue
        reports it as not found; *unattributed* otherwise: a server-initiated
        close is not told from another client's by its order.

        :param judge_protection: Judge a ``STOP_LOSS_TAKE_PROFIT`` order
            against the bot's own protection
            (:meth:`_recovered_protection_origin`) instead of taking it as the
            venue's.
        :return: The origin, or ``None`` when the order lookup failed for now.
        """
        store = self.store_ctx
        if self._is_own_entry_order(order_id) or any(
                record.order_id == order_id for record in self._own_close_history):
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
                    "(%s); the position is judged once the lookup succeeds",
                    order_id, position_id, exc)
            return None
        except (BrokerError, CTraderWireError, TimeoutError) as exc:
            logger.debug("cTrader closing-order lookup for %d deferred: %s", order_id, exc)
            return None
        order = res.order
        if order.orderType == _STOP_LOSS_TAKE_PROFIT and judge_protection:
            return await self._recovered_protection_origin(position_id, order)
        if order.orderType == _STOP_LOSS_TAKE_PROFIT or order.isStopOut:
            return CloseOrigin.VENUE
        if order.clientOrderId:
            if self._is_run_client_order_id(order.clientOrderId):
                return CloseOrigin.OWN
            return CloseOrigin.EXTERNAL
        return CloseOrigin.UNATTRIBUTED

    # --- foreign edits of the bot's pending orders --------------------------

    def _remember_own_order_terms(
            self, coid: str, *, limit: float | None, stop: float | None, volume: int,
    ) -> None:
        """Record terms the bot sends for its working entry order on its row.

        The latest write goes last; a write of terms sent before replaces
        their entry, whose answer is then pending again.
        """
        if self.store_ctx is None:
            return
        row = self.store_ctx.get_order(coid)
        if row is None:
            return
        extras = dict(row.extras or {})
        entry = [limit, stop, volume]
        terms = [t for t in extras.get(_OWN_ORDER_TERMS_EXTRA) or []
                 if not (isinstance(t, list) and t[:3] == entry)]
        terms.append(entry)
        extras[_OWN_ORDER_TERMS_EXTRA] = terms
        self.store_ctx.upsert_order(coid, extras=extras)

    def _own_order_terms(self, order_id: int) -> list:
        """The terms recorded as the bot's for its working entry order."""
        if self.store_ctx is None:
            return []
        row = self.store_ctx.find_by_ref('order_id', str(order_id))
        if row is None:
            return []
        return list((row.extras or {}).get(_OWN_ORDER_TERMS_EXTRA) or [])

    def _withdraw_own_order_terms(self, order_id: int, sent: list, before: list) -> None:
        """Take back terms an own write the venue refused recorded as the bot's.

        The order never carried them by the bot's hand, so a later view showing
        them is judged like any other. An entry of the same terms an earlier
        write left stays where it was.

        :param sent: The ``[limit, stop, volume]`` the refused write sent.
        :param before: The recorded terms before the write
            (:meth:`_own_order_terms`).
        """
        if self.store_ctx is None:
            return
        row = self.store_ctx.find_by_ref('order_id', str(order_id))
        if row is None:
            return
        extras = dict(row.extras or {})
        terms = [t for t in extras.get(_OWN_ORDER_TERMS_EXTRA) or []
                 if not (isinstance(t, list) and t[:3] == sent[:3])]
        for index, entry in enumerate(before):
            if isinstance(entry, list) and entry[:3] == sent[:3]:
                terms.insert(min(index, len(terms)), entry)
                break
        extras[_OWN_ORDER_TERMS_EXTRA] = terms
        self.store_ctx.upsert_order(row.client_order_id, extras=extras)

    def _remember_own_order_answer(
            self, coid: str, sent: list, order: OpenApiModelMessages.ProtoOAOrder,
    ) -> None:
        """Record the volume the venue's answer to an own write showed for the order.

        Kept on the write's term entry: a later view of the order is judged
        against the volume as the venue represents it
        (:meth:`_foreign_order_view`), whatever the write's volume meant to it.

        :param sent: The ``[limit, stop, volume]`` the write sent.
        """
        if self.store_ctx is None:
            return
        row = self.store_ctx.get_order(coid)
        if row is None:
            return
        extras = dict(row.extras or {})
        terms = list(extras.get(_OWN_ORDER_TERMS_EXTRA) or [])
        for index, entry in enumerate(terms):
            if isinstance(entry, list) and entry[:3] == sent[:3]:
                terms[index] = [*sent[:3], order.tradeData.volume, order.executedVolume]
                extras[_OWN_ORDER_TERMS_EXTRA] = terms
                self.store_ctx.upsert_order(coid, extras=extras)
                return

    def _learn_order_volume_semantics(self, order: OpenApiModelMessages.ProtoOAOrder) -> None:
        """Learn from a partial fill of the bot's working entry what the venue's
        order volume means.

        Whether a partly filled order's ``tradeData.volume`` is its whole
        volume or its remainder is a property of the venue. A partial fill
        event reports the order, still working, as the venue left it after its
        own execution: after more executed than the answer to the bot's latest
        write showed, the whole volume stayed the answer's, while the
        remainder dropped by what executed since. Only a view that fits
        exactly one of the two teaches; the verdict holds for every order of
        the process. A later one that fits only the other undoes it (WARNING):
        both are accepted again.
        """
        if (self.store_ctx is None or not order.orderId
                or order.orderType not in (OpenApiModelMessages.ProtoOAOrderType.LIMIT,
                                           OpenApiModelMessages.ProtoOAOrderType.STOP)):
            return
        row = self.store_ctx.find_by_ref('order_id', str(order.orderId))
        terms = (row.extras or {}).get(_OWN_ORDER_TERMS_EXTRA) if row is not None else None
        if not isinstance(terms, list) or not terms:
            return
        latest = terms[-1]
        if not isinstance(latest, list) or len(latest) != 5:
            return
        view_volume, view_executed = latest[3], latest[4]
        volume, executed = order.tradeData.volume, order.executedVolume
        if executed <= view_executed:
            return
        whole = volume == view_volume
        remainder = volume + executed == view_volume + view_executed
        if whole == remainder:
            return
        learned = 'whole' if whole else 'remainder'
        if self._order_volume_semantics is None:
            self._order_volume_semantics = learned
            logger.info(
                "cTrader: a partly filled order's volume is its %s (learned from order %d)",
                learned, order.orderId)
        elif self._order_volume_semantics != learned:
            logger.warning(
                "cTrader: order %d reports a partly filled order's volume as its %s, not its "
                "%s as learned before; both are accepted from now on",
                order.orderId, learned, self._order_volume_semantics)
            self._order_volume_semantics = None

    def _remember_own_order_attrs(
            self, coid: str, order: OpenApiModelMessages.ProtoOAOrder,
    ) -> None:
        """Record the attributes the venue's answer to an own write showed for the order.

        The bot sets none of them (:data:`_DEFAULT_ORDER_ATTRS`); whatever the
        venue fills in on its own for the bot's order is the bot's too. The
        view is the latest one of the order the bot has.
        """
        attrs = _order_attrs(order)
        if order.orderId:
            self._order_attrs_seen[order.orderId] = attrs
        if self.store_ctx is None:
            return
        if attrs == _DEFAULT_ORDER_ATTRS:
            return
        row = self.store_ctx.get_order(coid)
        if row is None:
            return
        extras = dict(row.extras or {})
        known = list(extras.get(_OWN_ORDER_ATTRS_EXTRA) or [])
        if attrs in known:
            return
        known.append(attrs)
        extras[_OWN_ORDER_ATTRS_EXTRA] = known
        self.store_ctx.upsert_order(coid, extras=extras)

    def _begin_own_amend(self, order_id: int) -> None:
        """An own amend of a working entry order is about to be sent."""
        in_flight = self._own_amends_in_flight.get(order_id, 0)
        if not in_flight and order_id in self._order_attrs_seen:
            self._attrs_before_amend[order_id] = self._order_attrs_seen[order_id]
        self._own_amends_in_flight[order_id] = in_flight + 1
        self._order_terms_watch.pop(order_id, None)

    def _end_own_amend(
            self, order_id: int, answer: OpenApiMessages.ProtoOAExecutionEvent | None, *,
            sent: list | None = None,
    ) -> None:
        """An own amend of a working entry order got its answer (or lost it).

        The bot's amend sets none of the order's other attributes
        (:func:`_order_attrs`), so the answer shows them as the venue kept
        them. Attributes the bot does not know as its own that a view of the
        order showed before the amend (the latest one judged before it was
        sent, or one pushed while it was in flight with other terms than the
        answer's) were already there: someone else's, never recorded as the
        bot's, so the views showing them are still judged. Attributes the
        amend's answer is the first to show are recorded as the bot's — the
        venue may fill them in on its own — but nothing proves that it did:
        logged as an ERROR and audited. The volume the answer shows is kept on
        the amend's term entry (:meth:`_remember_own_order_answer`). A view
        pushed while the amend was in
        flight is judged by the order-event stream once no own amend of the
        order is in flight (:meth:`_judge_held_replaced_orders`).

        :param sent: The ``[limit, stop, volume]`` the amend sent, when recorded.
        """
        remaining = self._own_amends_in_flight.get(order_id, 1) - 1
        before = self._attrs_before_amend.get(order_id)
        if remaining > 0:
            self._own_amends_in_flight[order_id] = remaining
        else:
            self._own_amends_in_flight.pop(order_id, None)
            self._attrs_before_amend.pop(order_id, None)
        self._own_amend_settled_at[order_id] = epoch_time()
        self._order_terms_watch.pop(order_id, None)
        if (answer is None or not answer.HasField('order')
                or answer.order.orderId != order_id or self.store_ctx is None):
            return
        order = answer.order
        attrs = _order_attrs(order)
        self._order_attrs_seen[order_id] = attrs
        row = self.store_ctx.find_by_ref('order_id', str(order_id))
        if row is None:
            return
        if sent is not None:
            self._remember_own_order_answer(row.client_order_id, sent, order)
        if attrs in self._known_order_attrs(row):
            return
        earlier = [before] if before is not None else []
        held = self._replaced_during_amend.get(order_id)
        if held is not None and _order_terms(held) != _order_terms(order):
            earlier.append(_order_attrs(held))
        if attrs in earlier:
            return
        self._remember_own_order_attrs(row.client_order_id, order)
        logger.error(
            "cTrader: the venue's answer to the bot's amend of order %d (entry %r) shows "
            "attributes the bot never set and no earlier view of the order showed (%s); "
            "taken as the bot's, but someone else may have set them",
            order_id, row.pine_entry_id, attrs)
        self.store_ctx.log_event(
            _ORDER_ATTRS_UNEXPLAINED_EVENT, client_order_id=row.client_order_id,
            exchange_order_id=str(order_id),
            payload={'order_id': order_id, 'attrs': attrs, 'before': before})

    def _known_order_attrs(self, row: 'OrderRow') -> list[list[float | int]]:
        """The attribute lists of a working entry the bot knows as its own."""
        return [_DEFAULT_ORDER_ATTRS, *((row.extras or {}).get(_OWN_ORDER_ATTRS_EXTRA) or [])]

    def _foreign_order_view(
            self, order: OpenApiModelMessages.ProtoOAOrder,
    ) -> 'tuple[OrderRow, list] | None':
        """The bot's row and the view of its working entry order, if someone else edited it.

        The order's price and volume must be ones the bot sent for it, and its
        other attributes (:func:`_order_attrs`) the defaults the bot sends or
        what the venue's answer to an own write showed. The volume is judged
        against the venue's answer to the write: the volume it showed then,
        or that less what executed since — whichever the venue reports for a
        partly filled order, once a fill of the bot's taught it
        (:meth:`_learn_order_volume_semantics`); either until then. A write
        whose answer was lost is matched by the volume it sent, as the whole
        volume or the remainder, whatever that volume meant to the venue. An
        order with no recorded terms (recovered after a crash) is not judged.
        The view's attributes become the latest the bot saw of its order
        (:meth:`_end_own_amend`).

        :return: ``(row, [limit, stop, volume, *attrs])`` for a foreign edit;
            ``None`` when the view is the bot's or cannot be judged.
        """
        if (self.store_ctx is None or not order.orderId
                or order.orderType not in (OpenApiModelMessages.ProtoOAOrderType.LIMIT,
                                           OpenApiModelMessages.ProtoOAOrderType.STOP)
                or order.orderId in self._replaced_reported):
            return None
        row = self.store_ctx.find_by_ref('order_id', str(order.orderId))
        if row is None or row.closed_ts_ms is not None or not row.pine_entry_id:
            return None
        attrs = _order_attrs(order)
        self._order_attrs_seen[order.orderId] = attrs
        extras = row.extras or {}
        terms = extras.get(_OWN_ORDER_TERMS_EXTRA)
        if not isinstance(terms, list) or not terms:
            return None
        limit = order.limitPrice or None
        stop = order.stopPrice or None
        volume = order.tradeData.volume
        executed = order.executedVolume
        tolerance = _price_tolerance(self._cached_tick(order.tradeData.symbolId), limit, stop)
        semantics = self._order_volume_semantics

        def own_volume(entry: list) -> bool:
            if len(entry) == 5:
                whole, remainder = entry[3], entry[3] + entry[4] - executed
                if semantics == 'whole':
                    return volume == whole
                if semantics == 'remainder':
                    return volume == remainder
                return volume in (whole, remainder)
            return entry[2] in (volume, volume + executed, volume - executed)

        if attrs in self._known_order_attrs(row) and any(
                isinstance(entry, list) and len(entry) in (3, 5)
                and _same_price(entry[0], limit, tolerance)
                and _same_price(entry[1], stop, tolerance) and own_volume(entry)
                for entry in terms):
            return None
        return row, [limit, stop, volume, *attrs]

    def _judge_replaced_order(self, order: OpenApiModelMessages.ProtoOAOrder) -> None:
        """Report a pending entry order of the bot someone else amended.

        ``ORDER_REPLACED`` of a working LIMIT / STOP entry whose terms are none
        the bot ever sent for it (:meth:`_foreign_order_view`) is someone
        else's edit. No order event is yielded for the push, so the policy
        applies at once. A view pushed while an own amend of the order is in
        flight may be the amend's or someone else's: it is kept and judged once
        the amend is answered.
        """
        if order.orderId and self._own_amends_in_flight.get(order.orderId):
            held = OpenApiModelMessages.ProtoOAOrder()
            held.CopyFrom(order)
            self._replaced_during_amend[order.orderId] = held
            return
        view = self._foreign_order_view(order)
        if view is not None:
            self._report_foreign_order_edit(order, *view)

    def _venue_cancel_reason(
            self, order: OpenApiModelMessages.ProtoOAOrder, *, expired: bool = False,
            server_event: bool = False, rejected: bool = False,
    ) -> str | None:
        """The venue's own cause of an order's end without a fill, when the evidence names one.

        The bot sends every order without an expiry, so only the venue's
        mechanics expire one — unless the order carries an expiry the bot
        never set (:meth:`_foreign_expiry`): then someone else's edit of the
        order caused it, and the cancel stays unexplained. A cancel the venue
        flags as its own (``isServerEvent``) and a rejection are the venue's.
        The core names no expiry reason; an order the venue ended itself is
        :data:`~pynecore.core.broker.models.CANCEL_REASON_VENUE_REJECTED`.

        :param expired: The order expired (``ORDER_EXPIRED``, or its status
            reads ``ORDER_STATUS_EXPIRED``).
        :param server_event: The venue flagged the cancel as server-initiated.
        :param rejected: The order's status reads ``ORDER_STATUS_REJECTED``.
        :return: A venue-driven cancel reason, or ``None`` when nothing proves
            the venue ended the order.
        """
        if expired:
            return None if self._foreign_expiry(order) else CANCEL_REASON_VENUE_REJECTED
        return CANCEL_REASON_VENUE_REJECTED if server_event or rejected else None

    def _unattributed_cancel_reason(self, row: 'OrderRow', order_id: int, *, status: str) -> str:
        """Log a working order that left the venue without a fill nobody can be named for.

        Only the push of a cancel carries ``isServerEvent``; the order's
        status a later lookup reads does not, so a cancelled order (or one the
        venue no longer knows) may have been cancelled by the venue's own
        logic as well as by someone else. Nothing establishes an intervention:
        the cancel is booked, logged as an ERROR and audited, and the
        unexpected-cancel policy must not run. The core names no reason for
        such a cancel, and only a venue-driven one keeps the disappearance
        tracker from applying the policy, so the venue-ended reason is
        returned.

        :param row: The order's journal row.
        :param order_id: The order's id.
        :param status: The order's status as the lookup read it, or the
            lookup's not-found error code.
        :return: The cancel reason for the tracker's verdict.
        """
        logger.error(
            "cTrader: bot order %d (%s) left the venue without a fill while the order "
            "stream missed it, and its status (%s) cannot tell a server-initiated cancel "
            "from someone else's; retired, not reported",
            order_id, row.client_order_id, status)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _CANCEL_UNATTRIBUTED_EVENT, client_order_id=row.client_order_id,
                exchange_order_id=str(order_id),
                payload={'order_id': order_id, 'status': status,
                         'pine_entry_id': row.pine_entry_id})
        return CANCEL_REASON_VENUE_REJECTED

    def _foreign_expiry(self, order: OpenApiModelMessages.ProtoOAOrder) -> bool:
        """Whether one of the bot's orders carries an expiry the bot never set.

        The bot sets none; one the venue's answer to an own write showed is
        the bot's too (:meth:`_remember_own_order_attrs`).
        """
        expiry = order.expirationTimestamp if order.HasField('expirationTimestamp') else 0
        if not expiry or self.store_ctx is None or not order.orderId:
            return False
        row = self.store_ctx.find_by_ref('order_id', str(order.orderId))
        return row is not None and all(
            attrs[4] != expiry for attrs in self._known_order_attrs(row))

    def _judge_held_replaced_orders(self) -> None:
        """Judge the views pushed while an own amend was in flight, once it is answered."""
        for order_id in [oid for oid in self._replaced_during_amend
                         if not self._own_amends_in_flight.get(oid)]:
            self._judge_replaced_order(self._replaced_during_amend.pop(order_id))

    def _observe_working_order(
            self, order: OpenApiModelMessages.ProtoOAOrder, *, read_started_at: float,
    ) -> None:
        """Judge a reconcile snapshot's view of one of the bot's working entry orders.

        An edit made while the stream was down reaches the bot only this way.
        A view taken while an own amend of the order is in flight, or read
        before the last one was answered, is not judged; a foreign view must
        show on :data:`_ORDER_TERMS_CONFIRMATIONS` consecutive passes. In a
        process that applied an operator resume the first view of each order
        that already existed at the resume is the venue the operator
        accepted: its terms become the bot's own
        (:meth:`_accept_order_view_by_resume`). An order the bot placed after
        the resume was never part of what the operator accepted. Taking that
        baseline is marked on the order's row, so a process applying the
        same resume again judges a later edit against it.

        :param order: The snapshot's ``order[]`` entry of a live entry row.
        :param read_started_at: Wall-clock time the snapshot read started.
        """
        order_id = order.orderId
        if self._own_amends_in_flight.get(order_id):
            self._order_terms_watch.pop(order_id, None)
            return
        settled_at = self._own_amend_settled_at.get(order_id)
        if settled_at is not None and read_started_at < settled_at:
            return
        view = self._foreign_order_view(order)
        resume_ts_ms = self.operator_resume_ts_ms
        if resume_ts_ms is not None and self._resume_baseline_due(order_id, resume_ts_ms):
            if view is not None and view[0].created_ts_ms <= resume_ts_ms:
                self._accept_order_view_by_resume(order, *view)
                view = None
            self._mark_resume_baseline_taken(order_id, resume_ts_ms)
        if view is None:
            self._order_terms_watch.pop(order_id, None)
            return
        row, observed = view
        key = tuple(observed)
        seen, count = self._order_terms_watch.get(order_id, (None, 0))
        count = count + 1 if seen == key else 1
        self._order_terms_watch[order_id] = (key, count)
        if count < _ORDER_TERMS_CONFIRMATIONS:
            return
        self._order_terms_watch.pop(order_id, None)
        self._report_foreign_order_edit(order, row, observed)

    def _resume_baseline_due(self, order_id: int, resume_ts_ms: int) -> bool:
        """Whether no view of a working order was taken as the resume's baseline yet."""
        if order_id in self._orders_seen_after_resume:
            return False
        row = (self.store_ctx.find_by_ref('order_id', str(order_id))
               if self.store_ctx is not None else None)
        if row is not None and (row.extras or {}).get(
                _ORDER_RESUME_BASELINE_EXTRA) == resume_ts_ms:
            self._orders_seen_after_resume.add(order_id)
            return False
        return True

    def _mark_resume_baseline_taken(self, order_id: int, resume_ts_ms: int) -> None:
        """Record on a working order's row that the resume's baseline view of it is taken."""
        self._orders_seen_after_resume.add(order_id)
        row = (self.store_ctx.find_by_ref('order_id', str(order_id))
               if self.store_ctx is not None else None)
        if self.store_ctx is None or row is None:
            return
        self.store_ctx.upsert_order(row.client_order_id, extras={
            **(row.extras or {}), _ORDER_RESUME_BASELINE_EXTRA: resume_ts_ms})

    def _accept_order_view_by_resume(
            self, order: OpenApiModelMessages.ProtoOAOrder, row: 'OrderRow', observed: list,
    ) -> None:
        """Take a working entry's terms as the bot's own after an operator resume.

        The operator accepted the venue as it stood, an edit of the bot's
        pending order made while the bot was quarantined included: WARNING +
        audit, and later views are judged against these terms.
        """
        limit, stop, volume, *attrs = observed
        self._remember_own_order_terms(row.client_order_id, limit=limit, stop=stop, volume=volume)
        self._remember_own_order_answer(row.client_order_id, [limit, stop, volume], order)
        self._remember_own_order_attrs(row.client_order_id, order)
        logger.warning(
            "cTrader: bot order %d (entry %r) carries terms the bot did not send (limit %s, "
            "stop %s, volume %s); the operator resumed the run and accepted the venue as it "
            "stands, so they are the bot's from now on", order.orderId, row.pine_entry_id,
            limit, stop, volume_to_units(volume))
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _ORDER_ACCEPTED_BY_RESUME_EVENT, client_order_id=row.client_order_id,
                exchange_order_id=str(order.orderId),
                payload={'order_id': order.orderId, 'observed': [limit, stop, volume],
                         'observed_attrs': attrs})

    def _report_foreign_order_edit(
            self, order: OpenApiModelMessages.ProtoOAOrder, row: 'OrderRow', observed: list,
    ) -> None:
        """Apply the intervention policy to someone else's edit of a working entry, once."""
        self._replaced_reported.add(order.orderId)
        self._order_terms_watch.pop(order.orderId, None)
        extras = row.extras or {}
        limit, stop, volume, *attrs = observed
        reason = (
            f"bot order {order.orderId} (entry {row.pine_entry_id!r}) was changed on the "
            f"venue by someone else: limit {limit}, stop {stop}, volume "
            f"{volume_to_units(volume)}"
        )
        if attrs != _DEFAULT_ORDER_ATTRS:
            reason += (f", stop-loss {attrs[0] or None}, take-profit {attrs[1] or None}, "
                       f"expiry {attrs[4] or None}, time in force {attrs[5]}")
        self.report_external_intervention(reason, {
            'order_id': order.orderId,
            'pine_entry_id': row.pine_entry_id,
            'observed': [limit, stop, volume],
            'observed_attrs': attrs,
            'executed_volume': order.executedVolume,
            'own_terms': extras.get(_OWN_ORDER_TERMS_EXTRA),
            'own_attrs': [_DEFAULT_ORDER_ATTRS, *(extras.get(_OWN_ORDER_ATTRS_EXTRA) or [])],
        }, client_order_id=row.client_order_id, exchange_order_id=str(order.orderId))

    # --- report delivery ------------------------------------------------------

    def _flush_external_interventions(self) -> None:
        """Report the queued manual closes, and those an earlier process left unhandled.

        Each report is journalled as handled once the policy ran, the halt
        included; its position is then open to the next report. Views of working entry orders pushed while an own amend was
        in flight are judged here once the amend is answered.

        :raises UnexpectedCancelError: When the policy requires the halt.
        """
        self._load_close_evidence()
        self._judge_held_replaced_orders()
        while self._deferred_interventions:
            reason, context, coid, position_ref, key = self._deferred_interventions.pop(0)
            try:
                self.report_external_intervention(
                    reason, context, client_order_id=coid, exchange_order_id=position_ref)
            except UnexpectedCancelError:
                self._journal_close_report_done(key, context)
                raise
            self._journal_close_report_done(key, context)
            position_id = context.get('position_id')
            if not any(queued[1].get('position_id') == position_id
                       for queued in self._deferred_interventions):
                self._external_close_reported.discard(position_id)

    def ensure_venue_writes_allowed(self, what: str = "venue write") -> None:
        """Refuse a venue write while the quarantine or an unhandled report forbids it.

        A queued manual-close report — this process's, or one an earlier
        process queued but never handled — is handled by the order-event
        stream, the halt included; until then the intervention it reports may
        require the quarantine, so no write leaves.

        :raises VenueWritesQuarantinedError: While a queued report is not
            handled yet, or the venue-write quarantine is latched.
        """
        self._load_close_evidence()
        if self._unhandled_close_reports:
            raise VenueWritesQuarantinedError(
                f"{what} (a manual-close report is not handled yet)")
        super().ensure_venue_writes_allowed(what)

    def _journal_close_report_done(self, key: str, context: dict) -> None:
        """Persist that a manual-close report was handled; never replayed again."""
        self._unhandled_close_reports.discard(key)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                _CLOSE_REPORT_DONE_EVENT, exchange_order_id=str(context.get('position_id')),
                payload={'key': key, 'position_id': context.get('position_id')})
