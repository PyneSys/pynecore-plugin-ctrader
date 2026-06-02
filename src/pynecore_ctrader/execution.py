"""Order-execution mix-in for the cTrader Open API plugin.

Implements the write side of :class:`~pynecore.core.plugin.broker.BrokerPlugin`
on top of :class:`~pynecore_ctrader._base._CTraderBase`: every ``execute_*`` and
``modify_*`` path, plus the order-sizing rules cache and the unit conversions.

cTrader uses position-attribute protective levels, so the exit bracket is a
single ``ProtoOAAmendPositionSLTPReq`` (``tp_sl_bracket = NATIVE``) and
``modify_exit`` amends the live position in place without a protection gap. Order
prices are absolute ``double`` values rounded to the symbol's ``digits``; order
``volume`` is the INT64 centi-unit quantity (see :mod:`~pynecore_ctrader.helpers`).

NETTING accounts merge same-symbol entries into one position, so the position
the exit / close / fail-safe paths target is found by symbol from a fresh
``ProtoOAReconcileReq`` snapshot rather than threaded through per-entry state.

M2 scope: this is the forward dispatch path. Persist-first crash recovery,
reconcile-driven disappearance detection and the cancel-tentative state machine
are M3 — the ``store_ctx`` writes here are a best-effort audit + ref-mapping
trail, guarded so the plugin still runs without persistence (test paths).
"""
import logging
from time import time as epoch_time
from typing import TYPE_CHECKING, cast

from pynecore.core.broker.exceptions import (
    BracketAttachAfterFillRejectedError,
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.emulator import (
    LegClose,
    aggregate_positions,
    plan_reversal,
    select_legs_for_close,
)
from pynecore.core.broker.idempotency import (
    KIND_CANCEL,
    KIND_CLOSE,
    KIND_ENTRY,
    KIND_ENTRY_STOP,
    KIND_EXIT_SL,
    KIND_EXIT_TP,
    KIND_MODIFY_ENTRY,
    KIND_MODIFY_EXIT,
)
from pynecore.core.broker.models import (
    CancelDispositionOutcome,
    CancelIntent,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExchangeOrder,
    ExitIntent,
    OrderStatus,
    OrderType,
)
from pynecore.core.plugin import override

from ._base import _CTraderBase
from .exceptions import (
    CTraderBrokerError,
    is_not_found,
    map_error_code,
    map_protocol_error,
)
from .helpers import quantize_volume, raw_volume, round_price, volume_to_units
from .messages import OpenApiMessages_pb2 as _oa
from .messages import OpenApiModelMessages_pb2 as _model
from .models import _SymbolRules
from .wire import (
    CTraderConnectionError,
    CTraderProtocolError,
    CTraderRequestSentConnectionError,
    CTraderTimeoutError,
)

if TYPE_CHECKING:
    from pynecore.core.broker.native_failsafe_manager import NativeBracketSnapshot

logger = logging.getLogger(__name__)

_SIDE_TO_TRADE_SIDE = {
    'buy': _model.ProtoOATradeSide.BUY,
    'sell': _model.ProtoOATradeSide.SELL,
}


class _ExecutionMixin(_CTraderBase):
    """Order execution mix-in: every ``execute_*`` and ``modify_*`` path."""

    # --- order-sizing rules + position discovery --------------------------

    async def _get_symbol_rules(self, symbol: str) -> _SymbolRules:
        """Resolve and cache the order-sizing + precision rules for ``symbol``.

        Sourced from the full ``ProtoOASymbolByIdRes`` detail record. Cached
        for the session (the rules are effectively static during a trading
        day); a missing symbol surfaces as an :class:`ExchangeOrderRejectedError`.
        """
        cached = self._symbol_rules.get(symbol)
        if cached is not None:
            return cached
        wire = self._wire
        if wire is None or self._live_account_id is None:
            raise CTraderConnectionError("live connection not established")
        if symbol not in self._symbols_by_name:
            await self._fetch_light_symbols(wire, self._live_account_id)
        symbol_id = self._symbols_by_name.get(symbol)
        if symbol_id is None:
            raise ExchangeOrderRejectedError(f"cTrader: unknown symbol {symbol!r}")
        detail_res = cast(_oa.ProtoOASymbolByIdRes, await wire.send_request(
            _oa.ProtoOASymbolByIdReq(
                ctidTraderAccountId=self._live_account_id, symbolId=[symbol_id],
            )
        ))
        if not detail_res.symbol:
            raise ExchangeOrderRejectedError(
                f"cTrader has no symbol detail for {symbol!r}"
            )
        detail = detail_res.symbol[0]
        rules = _SymbolRules(
            symbol_id=symbol_id,
            digits=detail.digits,
            min_volume=detail.minVolume,
            step_volume=detail.stepVolume,
            max_volume=detail.maxVolume,
        )
        self._symbol_rules[symbol] = rules
        return rules

    async def _find_open_position_id(self, symbol: str) -> int | None:
        """Return the ``positionId`` of the open position for ``symbol``.

        NETTING merges same-symbol entries into one position, so the exit /
        close / fail-safe paths all target the single open position found by
        symbol. ``None`` when the symbol is flat.
        """
        want_id = await self._resolve_state_symbol_id(symbol)
        if want_id is None:
            # Never fall back to a symbol-agnostic match: the exit / close /
            # fail-safe paths would otherwise amend or close an unrelated
            # symbol's position on a multi-symbol account.
            return None
        res = await self._reconcile()
        for position in res.position:
            if position.positionStatus != _model.ProtoOAPositionStatus.POSITION_STATUS_OPEN:
                continue
            if position.tradeData.symbolId != want_id:
                continue
            return position.positionId
        return None

    def _find_working_order_id(self, symbol: str, pine_id: str) -> int | None:
        """Find the live working-order id for a Pine entry id (BrokerStore).

        Returns the broker ``orderId`` the entry was persisted under, or
        ``None`` when no live row matches (already filled / cancelled, or no
        persistence). Used by the cancel / amend-entry paths.
        """
        if self.store_ctx is None:
            return None
        for row in self.store_ctx.iter_live_orders(symbol=symbol):
            if row.pine_entry_id != pine_id:
                continue
            order_id = (row.extras or {}).get('order_id')
            if order_id:
                return int(order_id)
        return None

    # --- dispatch helper --------------------------------------------------

    async def _dispatch_order(
            self, req, *, coid: str, context: str,
    ) -> _oa.ProtoOAExecutionEvent:
        """Send an order request and return its acknowledging execution event.

        Translates the wire failure modes into the broker taxonomy: a protocol
        error → mapped reject; a timeout → :class:`OrderDispositionUnknownError`
        (the dispatch may have landed); a drop *after* the request was written
        (:class:`CTraderRequestSentConnectionError`) → likewise
        :class:`OrderDispositionUnknownError`, since the server may have accepted
        the order; only a pre-write drop (plain :class:`CTraderConnectionError`)
        is a clean :class:`ExchangeConnectionError`. A returned
        ``ProtoOAOrderErrorEvent`` or an ``ORDER_REJECTED`` execution event is
        raised as a mapped reject.

        :param req: The concrete order request message.
        :param coid: The client-order-id, for disposition-unknown correlation.
        :param context: Short label for the ambiguous-timeout message.
        :return: The acknowledging ``ProtoOAExecutionEvent``.
        """
        wire = self._wire
        if wire is None:
            raise CTraderConnectionError("live connection not established")
        try:
            message = await wire.send_request(req)
        except CTraderProtocolError as exc:
            raise map_protocol_error(exc) from exc
        except CTraderTimeoutError as exc:
            raise OrderDispositionUnknownError(
                f"cTrader {context} timed out; disposition unknown",
                client_order_id=coid, cause=exc,
            ) from exc
        except CTraderRequestSentConnectionError as exc:
            # The request bytes reached (or may have reached) the wire before the
            # link dropped, so the server may already hold the order. Park it as
            # disposition-unknown — a clean ``ExchangeConnectionError`` would let
            # the engine retry and duplicate an entry / close cTrader accepted.
            raise OrderDispositionUnknownError(
                f"cTrader {context} connection lost after send; disposition unknown",
                client_order_id=coid, cause=exc,
            ) from exc
        except CTraderConnectionError as exc:
            raise ExchangeConnectionError(str(exc) or "connection lost") from exc

        if isinstance(message, _oa.ProtoOAOrderErrorEvent):
            # Chain the raw code as a ``CTraderProtocolError`` cause so the
            # cancel paths can still recognise a not-found race
            # (``ORDER_NOT_FOUND`` / ``POSITION_NOT_FOUND``) when the server
            # reports it as an error EVENT rather than an error RESPONSE.
            cause = CTraderProtocolError(message.errorCode, message.description)
            raise map_error_code(message.errorCode, message.description) from cause
        if isinstance(message, _oa.ProtoOAExecutionEvent):
            if (message.executionType == _model.ProtoOAExecutionType.ORDER_REJECTED
                    or message.errorCode):
                cause = CTraderProtocolError(message.errorCode, "")
                raise map_error_code(message.errorCode, "") from cause
            self._surface_correlated_fill(message)
            return message
        raise ExchangeOrderRejectedError(
            f"cTrader {context}: unexpected response {type(message).__name__}"
        )

    def _surface_correlated_fill(self, message: _oa.ProtoOAExecutionEvent) -> None:
        """Re-inject a correlated fill onto the order-event stream.

        ``send_request`` consumes the correlated ``ProtoOAExecutionEvent`` off
        the wire (the dispatch future takes it), so a MARKET entry / close that
        fills immediately and comes back as the *response* never reaches
        ``watch_orders``. The sync engine learns fills ONLY from that stream — it
        reads just ``.id`` off the ``execute_*`` return value, never its
        ``status`` / ``filled_qty`` — so without this the position would stay
        stale after a market fill and the strategy could re-enter. Put a terminal
        fill back on the execution queue so ``watch_orders`` emits it.
        ``watch_orders`` de-duplicates by ``dealId``, so if cTrader ALSO pushes
        an uncorrelated copy of the same fill only one ``record_fill`` results —
        correct whether or not the server sends that copy.
        """
        execq = self._exec_events
        if execq is None:
            return
        if message.executionType in (
                _model.ProtoOAExecutionType.ORDER_FILLED,
                _model.ProtoOAExecutionType.ORDER_PARTIAL_FILL):
            execq.put_nowait(message)

    # --- BrokerPlugin: execute path ---------------------------------------

    @override
    async def execute_entry(
            self, envelope: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Open a position (MARKET) or place a working order (LIMIT / STOP).

        The sync engine only ever dispatches plain MARKET / LIMIT / STOP here
        (a both-set Pine entry is split into OCO legs upstream). Size below
        ``minVolume`` or above ``maxVolume`` is declined with
        :class:`OrderSkippedByPlugin` (never silently clamped) so a single
        out-of-range order cannot halt the bot. A protective bracket is NOT
        attached here — it arrives as a separate ``strategy.exit`` →
        :meth:`execute_exit`.

        On a HEDGED account a MARKET entry whose direction opposes the open
        position is a Pine reversal: Pine folds it into one combined-size order
        assuming a netting auto-flip. The opposing legs are FIFO-closed first
        (so gross exposure does not bloat with a separate opposing leg) and only
        the residual target size is opened here. A LIMIT / STOP reversal rests
        and is NOT decomposed at placement time — opening it as a fresh leg on
        fill is a follow-up.
        """
        intent = envelope.intent
        assert isinstance(intent, EntryIntent)
        rules = await self._get_symbol_rules(intent.symbol)
        qty = intent.qty
        if self.hedging_enabled and intent.order_type == OrderType.MARKET:
            qty = await self._close_opposing_legs_for_reversal(envelope, intent, rules)
            if qty <= 0.0:
                # The combined order exactly flattened the opposing legs — the
                # close FILLs reduce the position to flat and there is no
                # residual to open.
                return []
        return await self._place_entry_order(envelope, intent, rules, qty)

    async def _close_opposing_legs_for_reversal(
            self, envelope: DispatchEnvelope, intent: EntryIntent,
            rules: _SymbolRules,
    ) -> float:
        """FIFO-close opposing hedging legs for a reversal; return residual qty.

        Returns ``intent.qty`` unchanged when the order only adds to the
        existing direction (no opposing legs). Otherwise dispatches a
        ``ProtoOAClosePositionReq`` per opposing leg (oldest first) and returns
        the residual size to open in the order's direction
        (``intent.qty - |opposite|``, clamped at zero). The close FILLs
        reverse-map to ``LegType.CLOSE`` via the BrokerStore index, so the sync
        engine's ``BrokerPosition`` reduces then flips exactly as a netting
        auto-flip would.
        """
        legs = await self.fetch_raw_positions(intent.symbol)
        plan = plan_reversal(intent.side, intent.qty, legs)
        if not plan.closes:
            return intent.qty
        # Pre-flight the requested size BEFORE dispatching any close: the closes
        # are real broker side effects, so an order that would be skipped (below
        # ``minVolume`` / above ``maxVolume``) must raise the "no order sent"
        # skip while it is still true. Validating after the closes would leave
        # the broker position reduced yet report the whole reversal as skipped,
        # desyncing the engine's ``BrokerPosition``. With a residual open
        # (``open_qty > 0``) the residual leg is what gets sized; for a pure
        # reduce (``open_qty == 0``, the order only nets opposing legs) the
        # combined order size itself must clear the bounds — otherwise a
        # below-min order quantizes UP per leg and over-reduces the position.
        self._reject_out_of_range_entry(
            intent, rules, plan.open_qty if plan.open_qty > 0.0 else intent.qty,
        )
        close_coid = envelope.client_order_id(KIND_CLOSE)
        for leg_id, volume in self._plan_leg_close_volumes(
                plan.closes, rules.step_volume):
            await self._dispatch_order(
                _oa.ProtoOAClosePositionReq(
                    ctidTraderAccountId=self._live_account_id,
                    positionId=leg_id, volume=volume,
                ),
                coid=close_coid, context="reversal close",
            )
        return plan.open_qty

    def _reject_out_of_range_entry(
            self, intent: EntryIntent, rules: _SymbolRules, qty: float,
    ) -> None:
        """Skip the entry when ``qty`` falls outside cTrader's volume bounds.

        Compares the *requested* raw centi-units (pre-step rounding, see
        :func:`raw_volume`) against ``minVolume`` / ``maxVolume`` and raises
        :class:`OrderSkippedByPlugin` rather than clamping. Callable as a
        pre-flight so the reversal path can decline before its FIFO closes run.
        """
        requested = raw_volume(qty)
        if requested < rules.min_volume:
            min_units = volume_to_units(rules.min_volume)
            raise OrderSkippedByPlugin(
                f"Skipping {intent.symbol} {intent.side.upper()} entry "
                f"id={intent.pine_id!r}: size {qty} below cTrader minimum "
                f"{min_units}. No order sent.",
                intent_key=intent.intent_key, reason="below_min_volume",
                context={'symbol': intent.symbol, 'qty': qty, 'min_qty': min_units},
            )
        if rules.max_volume > 0 and requested > rules.max_volume:
            max_units = volume_to_units(rules.max_volume)
            raise OrderSkippedByPlugin(
                f"Skipping {intent.symbol} {intent.side.upper()} entry "
                f"id={intent.pine_id!r}: size {qty} above cTrader maximum "
                f"{max_units}. No order sent.",
                intent_key=intent.intent_key, reason="above_max_volume",
                context={'symbol': intent.symbol, 'qty': qty, 'max_qty': max_units},
            )

    @staticmethod
    def _plan_leg_close_volumes(
            closes: tuple[LegClose, ...], step_volume: int,
    ) -> list[tuple[int, int]]:
        """Fan a FIFO close plan out to per-leg centi-unit close volumes.

        Quantizing each FIFO slice independently changes the total close size on
        the step grid: a sub-step slice rounds to ``0`` (an invalid empty close
        that under-reduces the position) and a fractional-step slice rounds UP
        past what the leg holds. Snapping the *total* once is not enough either:
        if a slice that carries part of the owed total quantizes to ``0`` on its
        own — e.g. two 10-unit legs with ``stepVolume=1000`` closing 15 units,
        where the second slice of 5 units snaps to ``0`` — handing out
        per-slice rounded volumes leaves the snapped total under-dispatched (only
        the first leg closes 1000 instead of the owed 2000).

        Instead a single running total is snapped to ``stepVolume``: each leg
        receives the delta between the snapped cumulative close-through-this-leg
        and the snapped cumulative through the previous leg, capped at the
        snapped grand total. The sub-step remainder a slice would have dropped is
        carried into the next leg, so the dispatched volumes always sum to the
        same grand total the single-position close path would use, every volume
        sits on the step grid (cTrader rejects an off-grid
        ``ProtoOAClosePositionReq.volume``), and no leg gets more than its own
        slice rounded up to the next step. Any leg whose delta is zero is dropped
        (no zero-volume request is sent).

        :param closes: FIFO close plan (oldest first); each ``qty`` is the
            slice to take from that leg, in Pine units, never above the leg's
            open size.
        :param step_volume: The symbol's ``stepVolume`` in centi-units.
        :return: ``(leg_id, volume)`` pairs to dispatch, in FIFO order, with the
            INT64 centi-unit ``volume`` each ``ProtoOAClosePositionReq`` expects.
        """
        grand_total = quantize_volume(sum(close.qty for close in closes), step_volume)
        out: list[tuple[int, int]] = []
        cumulative_units = 0.0
        dispatched = 0
        for close in closes:
            if dispatched >= grand_total:
                break
            cumulative_units += close.qty
            snapped = min(quantize_volume(cumulative_units, step_volume), grand_total)
            volume = snapped - dispatched
            if volume <= 0:
                continue
            out.append((int(close.leg_id), volume))
            dispatched = snapped
        return out

    async def _place_entry_order(
            self, envelope: DispatchEnvelope, intent: EntryIntent,
            rules: _SymbolRules, qty: float,
    ) -> list[ExchangeOrder]:
        """Place one MARKET / LIMIT / STOP order of ``qty`` for ``intent``."""
        coid = envelope.client_order_id(
            KIND_ENTRY_STOP if intent.stop_fired_market else KIND_ENTRY,
        )
        self._reject_out_of_range_entry(intent, rules, qty)
        volume = quantize_volume(qty, rules.step_volume)

        req = _oa.ProtoOANewOrderReq(
            ctidTraderAccountId=self._live_account_id,
            symbolId=rules.symbol_id,
            tradeSide=_SIDE_TO_TRADE_SIDE[intent.side],
            volume=volume,
            clientOrderId=coid,
            label=envelope.run_tag,
        )
        if intent.comment:
            req.comment = intent.comment
        if intent.order_type == OrderType.MARKET:
            req.orderType = _model.ProtoOAOrderType.MARKET
        elif intent.order_type == OrderType.LIMIT:
            if intent.limit is None:
                raise ExchangeOrderRejectedError(
                    f"cTrader LIMIT entry needs a limit price (id={intent.pine_id!r})"
                )
            req.orderType = _model.ProtoOAOrderType.LIMIT
            req.limitPrice = round_price(intent.limit, rules.digits)
            req.timeInForce = _model.ProtoOATimeInForce.GOOD_TILL_CANCEL
        else:  # STOP
            if intent.stop is None:
                raise ExchangeOrderRejectedError(
                    f"cTrader STOP entry needs a stop price (id={intent.pine_id!r})"
                )
            req.orderType = _model.ProtoOAOrderType.STOP
            req.stopPrice = round_price(intent.stop, rules.digits)
            req.timeInForce = _model.ProtoOATimeInForce.GOOD_TILL_CANCEL

        event = await self._dispatch_order(req, coid=coid, context="entry")
        order = event.order
        qty_units = volume_to_units(volume)
        filled = volume_to_units(order.executedVolume)
        self._persist_entry(coid, intent, order, qty_units, filled)

        status = (OrderStatus.FILLED
                  if event.executionType == _model.ProtoOAExecutionType.ORDER_FILLED
                  else OrderStatus.OPEN)
        return [ExchangeOrder(
            id=str(order.orderId),
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            qty=qty_units,
            filled_qty=filled,
            remaining_qty=max(0.0, qty_units - filled),
            price=(round_price(intent.limit, rules.digits)
                   if intent.order_type is OrderType.LIMIT and intent.limit is not None
                   else None),
            stop_price=(round_price(intent.stop, rules.digits)
                        if intent.order_type is OrderType.STOP and intent.stop is not None
                        else None),
            average_fill_price=order.executionPrice or None,
            status=status,
            timestamp=epoch_time(),
            fee=0.0,
            fee_currency='',
            reduce_only=False,
            client_order_id=coid,
        )]

    def _persist_entry(
            self, coid: str, intent: EntryIntent, order, qty_units: float,
            filled: float,
    ) -> None:
        """Best-effort BrokerStore persist + ref-mapping for a new entry.

        Records the entry row keyed by ``pine_entry_id`` and the broker
        ``orderId`` / ``positionId`` aliases so the cancel / amend paths and
        ``watch_orders`` can reverse-map back to Pine identity. Guarded for
        the no-persistence test path.
        """
        if self.store_ctx is None:
            return
        order_id = str(order.orderId)
        position_id = order.positionId or 0
        self.store_ctx.upsert_order(
            coid,
            symbol=intent.symbol,
            side=intent.side,
            qty=qty_units,
            filled_qty=filled,
            state='confirmed',
            intent_key=intent.intent_key,
            pine_entry_id=intent.pine_id,
            exchange_order_id=(str(position_id) if position_id else order_id),
            extras={'order_id': order_id,
                    'position_id': position_id or None},
        )
        self.store_ctx.add_ref(coid, 'order_id', order_id)
        # FIFO-pin the shared netted position alias (NETTING merges pyramid
        # entries onto one positionId); the per-entry ``extras['position_id']``
        # above keeps every row flattenable on a full close.
        self._link_position_ref(coid, position_id)
        self.store_ctx.log_event(
            'entry_dispatched', client_order_id=coid,
            exchange_order_id=order_id, intent_key=intent.intent_key,
        )

    @override
    async def execute_exit(
            self, envelope: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Attach a TP / SL / trailing bracket to the open position (NATIVE).

        cTrader's protective levels are position attributes, so the full-row
        bracket is one ``ProtoOAAmendPositionSLTPReq`` (no separate exit
        order). The partial-qty bracket (``partial_qty_bracket_exit = SOFTWARE``)
        is engine-driven through :meth:`execute_close`; this path attaches the
        SL/TP to the whole position. The two returned legs carry synthetic
        ``{positionId}:tp`` / ``:sl`` ids so the sync engine's leg accounting
        still works.
        """
        intent = envelope.intent
        assert isinstance(intent, ExitIntent)
        rules = await self._get_symbol_rules(intent.symbol)
        if self.hedging_enabled:
            return await self._emulated_exit(envelope, intent, rules)
        position_id = await self._find_open_position_id(intent.symbol)
        if position_id is None:
            raise ExchangeOrderRejectedError(
                f"cTrader execute_exit: no open position for symbol "
                f"{intent.symbol!r} (from_entry={intent.from_entry!r})"
            )

        req = _oa.ProtoOAAmendPositionSLTPReq(
            ctidTraderAccountId=self._live_account_id,
            positionId=position_id,
        )
        self._apply_bracket_levels(req, intent, rules)

        try:
            await self._dispatch_order(req, coid=envelope.client_order_id(KIND_EXIT_SL),
                                       context="bracket attach")
        except ExchangeOrderRejectedError as exc:
            # The parent entry already filled (the position is OPEN — this amend
            # only attaches its protective levels), so a reject here leaves the
            # position open and UNPROTECTED. Surface it distinctly so the sync
            # engine flattens it with a defensive market close instead of
            # halting (a plain reject is treated as a no-fill, non-terminal
            # condition and would strand the unprotected position).
            raise self._bracket_attach_reject(intent, position_id, exc) from exc

        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'bracket_attached', intent_key=intent.intent_key,
                exchange_order_id=str(position_id),
                payload={'tp': intent.tp_price, 'sl': intent.sl_price,
                         'trail_offset': intent.trail_offset},
            )
        return self._build_bracket_legs(intent, envelope, position_id, rules)

    async def _emulated_exit(
            self, envelope: DispatchEnvelope, intent: ExitIntent,
            rules: _SymbolRules,
    ) -> list[ExchangeOrder]:
        """Replicate the one-way bracket onto every leg of a hedging position.

        cTrader SL/TP are per-position attributes, so a one-way bracket over a
        multi-leg position is delivered by amending the SAME levels onto each
        leg on the position side. When any leg's protection fires it closes that
        leg; the sync engine reduces ``BrokerPosition`` and runs its cleanup
        once the aggregate reaches zero. Legs pyramided AFTER this attach are
        protected when the next bracket amend re-runs the replication.
        """
        legs = await self.fetch_raw_positions(intent.symbol)
        pos = aggregate_positions(intent.symbol, legs)
        if pos is None or pos.side == 'flat':
            raise ExchangeOrderRejectedError(
                f"cTrader execute_exit: no open position for symbol "
                f"{intent.symbol!r} (from_entry={intent.from_entry!r})"
            )
        open_side = 'buy' if pos.side == 'long' else 'sell'
        target_legs = [leg for leg in legs if leg.side == open_side]
        coid = envelope.client_order_id(KIND_EXIT_SL)
        for leg in target_legs:
            position_id = int(leg.leg_id)
            req = _oa.ProtoOAAmendPositionSLTPReq(
                ctidTraderAccountId=self._live_account_id, positionId=position_id,
            )
            self._apply_bracket_levels(req, intent, rules)
            try:
                await self._dispatch_order(req, coid=coid, context="bracket attach")
            except ExchangeOrderRejectedError as exc:
                raise self._bracket_attach_reject(intent, position_id, exc) from exc
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'bracket_attached', intent_key=intent.intent_key,
                exchange_order_id=','.join(leg.leg_id for leg in target_legs),
                payload={'tp': intent.tp_price, 'sl': intent.sl_price,
                         'trail_offset': intent.trail_offset},
            )
        # The synthetic TP/SL leg ids the engine tracks are keyed off the oldest
        # leg's position id; on the next bracket amend the replication re-runs
        # across whatever legs are then open.
        return self._build_bracket_legs(
            intent, envelope, int(target_legs[0].leg_id), rules,
        )

    def _apply_bracket_levels(
            self, req, intent: ExitIntent, rules: _SymbolRules,
    ) -> None:
        """Set the SL / TP / trailing fields on a bracket amend request.

        Shared by :meth:`execute_exit` and :meth:`modify_exit`. An explicit
        ``sl_price`` anchors the (possibly trailing) stop; a trail-only exit
        (``trail_offset`` set, ``sl_price`` None) gets a current-price-derived
        anchor so cTrader has an absolute level to trail from.
        """
        if intent.sl_price is not None:
            req.stopLoss = round_price(intent.sl_price, rules.digits)
        elif intent.trail_offset is not None:
            anchor = self._trailing_anchor(intent, rules)
            if anchor is not None:
                req.stopLoss = anchor
        if intent.tp_price is not None:
            req.takeProfit = round_price(intent.tp_price, rules.digits)
        if intent.trail_offset is not None:
            req.trailingStopLoss = True

    def _trailing_anchor(
            self, intent: ExitIntent, rules: _SymbolRules,
    ) -> float | None:
        """Best-effort initial absolute ``stopLoss`` for a trail-only exit.

        cTrader trails from the supplied absolute ``stopLoss`` — the server
        derives the distance as ``|currentPrice - stopLoss|`` and ratchets it in
        the favourable direction. A Pine trailing exit with no explicit stop
        (``trail_offset`` set, ``sl_price`` None) carries no anchor, so seed one
        ``trail_offset`` away from the current price on the exit side: a
        ``'sell'`` exit (flattening a long) trails below, a ``'buy'`` exit
        (flattening a short) above. ``trail_offset`` is in price units, so no
        tick scaling is needed. cTrader has no activation-delay field, so the
        Pine ``trail_price`` activation level cannot be expressed (the trail is
        live immediately); the exact anchor convention is live-verified on the
        Pepperstone demo (§9.18). Returns ``None`` when no current price is known
        yet, leaving a bare ``trailingStopLoss=True`` for the server to seed.
        """
        ref = self._last_bid
        if ref is None or intent.trail_offset is None:
            return None
        anchor = (ref - intent.trail_offset if intent.side == 'sell'
                  else ref + intent.trail_offset)
        return round_price(anchor, rules.digits)

    def _build_bracket_legs(
            self, intent: ExitIntent, envelope: DispatchEnvelope,
            position_id: int, rules: _SymbolRules,
    ) -> list[ExchangeOrder]:
        """Synthesise the TP / SL :class:`ExchangeOrder` legs for a bracket."""
        now_ts = epoch_time()
        legs: list[ExchangeOrder] = []
        if intent.tp_price is not None:
            legs.append(ExchangeOrder(
                id=f"{position_id}:tp",
                symbol=intent.symbol, side=intent.side,
                order_type=OrderType.LIMIT, qty=intent.qty,
                filled_qty=0.0, remaining_qty=intent.qty,
                price=round_price(intent.tp_price, rules.digits), stop_price=None,
                average_fill_price=None, status=OrderStatus.OPEN, timestamp=now_ts,
                fee=0.0, fee_currency='', reduce_only=True,
                client_order_id=envelope.client_order_id(KIND_EXIT_TP),
            ))
        if intent.sl_price is not None or intent.trail_offset is not None:
            legs.append(ExchangeOrder(
                id=f"{position_id}:sl",
                symbol=intent.symbol, side=intent.side,
                order_type=(OrderType.TRAILING_STOP
                            if intent.trail_offset is not None else OrderType.STOP),
                qty=intent.qty, filled_qty=0.0, remaining_qty=intent.qty,
                price=None,
                stop_price=(round_price(intent.sl_price, rules.digits)
                            if intent.sl_price is not None else None),
                average_fill_price=None, status=OrderStatus.OPEN, timestamp=now_ts,
                fee=0.0, fee_currency='', reduce_only=True,
                client_order_id=envelope.client_order_id(KIND_EXIT_SL),
            ))
        return legs

    def _bracket_attach_reject(
            self, intent: ExitIntent, position_id: int,
            cause: ExchangeOrderRejectedError,
    ) -> BracketAttachAfterFillRejectedError:
        """Build the defensive-close error for a rejected bracket amend.

        The open position's side is the inverse of the exit side (a ``"sell"``
        exit flattens a long, a ``"buy"`` exit flattens a short). cTrader's
        NETTING position has no single entry client-order-id, so a stable
        surrogate (symbol + ``from_entry``) keys the defensive ``CloseIntent``
        across retries. Protective levels are position attributes, so no
        residual TP/SL order entities are left behind — the base
        :meth:`get_residual_orders_after_bracket_attach_reject` (empty list)
        is correct and no override is needed.
        """
        position_side = 'buy' if intent.side == 'sell' else 'sell'
        surrogate_coid = f"__pyne_orphan__{intent.symbol}__{intent.from_entry}"
        code = (cause.__cause__.error_code
                if isinstance(cause.__cause__, CTraderProtocolError) else None)
        return BracketAttachAfterFillRejectedError(
            f"cTrader bracket attach rejected after entry fill "
            f"(positionId={position_id}, from_entry={intent.from_entry!r}): {cause}",
            position_coid=surrogate_coid,
            position_deal_id=str(position_id),
            symbol=intent.symbol,
            position_side=position_side,
            qty=intent.qty,
            from_entry=intent.from_entry,
            exit_id=intent.pine_id,
            error_code=code,
        )

    @override
    async def execute_close(
            self, envelope: DispatchEnvelope,
    ) -> ExchangeOrder:
        """Close (or partially reduce) the open position with a market close.

        ``ProtoOAClosePositionReq(positionId, volume)`` reduces by ``volume``
        centi-units (the full position ``volume`` for a full close, a slice for
        a partial). The realized PnL settles on the ``watch_orders`` fill via
        ``deal.closePositionDetail``.
        """
        intent = envelope.intent
        assert isinstance(intent, CloseIntent)
        coid = envelope.client_order_id(KIND_CLOSE)
        rules = await self._get_symbol_rules(intent.symbol)
        if self.hedging_enabled:
            return await self._emulated_close(envelope, intent, coid, rules)
        position_id = await self._find_open_position_id(intent.symbol)
        if position_id is None:
            raise ExchangeOrderRejectedError(
                f"cTrader execute_close: no open position for symbol "
                f"{intent.symbol!r}"
            )
        volume = quantize_volume(intent.qty, rules.step_volume)
        event = await self._dispatch_order(
            _oa.ProtoOAClosePositionReq(
                ctidTraderAccountId=self._live_account_id,
                positionId=position_id, volume=volume,
            ),
            coid=coid, context="close",
        )
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'close_dispatched', client_order_id=coid,
                exchange_order_id=str(position_id), intent_key=intent.intent_key,
            )
        order = event.order
        qty_units = volume_to_units(volume)
        filled = volume_to_units(order.executedVolume)
        return ExchangeOrder(
            id=str(order.orderId) if order.orderId else str(position_id),
            symbol=intent.symbol, side=intent.side, order_type=OrderType.MARKET,
            qty=qty_units, filled_qty=filled,
            remaining_qty=max(0.0, qty_units - filled),
            price=None, stop_price=None,
            average_fill_price=order.executionPrice or None,
            status=(OrderStatus.FILLED
                    if event.executionType == _model.ProtoOAExecutionType.ORDER_FILLED
                    else OrderStatus.OPEN),
            timestamp=epoch_time(), fee=0.0, fee_currency='',
            reduce_only=True, client_order_id=coid,
        )

    async def _emulated_close(
            self, envelope: DispatchEnvelope, intent: CloseIntent,
            coid: str, rules: _SymbolRules,
    ) -> ExchangeOrder:
        """Reduce a one-way position by closing its hedging legs FIFO.

        Aggregates the open legs to find the position side, plans the close
        oldest-first across the legs on that side, and dispatches one
        ``ProtoOAClosePositionReq`` per leg. Each close FILL reverse-maps to
        ``LegType.CLOSE`` via the BrokerStore index, so the sync engine reduces
        ``BrokerPosition`` FIFO and runs its close cleanup once the aggregate
        reaches zero. A shortfall (Pine asks to close more than the legs hold)
        is logged and the available legs are still closed — the next reconcile
        re-derives the truth from the net rather than halting the bot.
        """
        legs = await self.fetch_raw_positions(intent.symbol)
        pos = aggregate_positions(intent.symbol, legs)
        if pos is None or pos.side == 'flat':
            raise ExchangeOrderRejectedError(
                f"cTrader execute_close: no open position for symbol "
                f"{intent.symbol!r}"
            )
        # Close the legs the intent targets, NOT the aggregate net side. A
        # ``CloseIntent`` carries ``side="sell"`` to reduce a long (buy legs) and
        # ``side="buy"`` to reduce a short (sell legs); on a hedged account that
        # also holds opposing legs (a manual or resting leg), the net side can be
        # the OPPOSITE of what the strategy means to close, so a net-derived side
        # would close the wrong legs and grow the intended exposure instead of
        # reducing it.
        close_side = 'buy' if intent.side == 'sell' else 'sell'
        plan, shortfall = select_legs_for_close(intent.qty, legs, close_side)
        if shortfall > 0.0:
            logger.warning(
                "cTrader emulated close for %s wants %s units but open legs "
                "hold %s less; closing what is open",
                intent.symbol, intent.qty, shortfall,
            )
        if not plan:
            raise ExchangeOrderRejectedError(
                f"cTrader execute_close: no {close_side} legs to close for "
                f"symbol {intent.symbol!r}"
            )
        last_order = None
        dispatched = self._plan_leg_close_volumes(plan, rules.step_volume)
        if not dispatched:
            # The whole close quantizes to zero on the ``stepVolume`` grid (a
            # sub-step residual / partial), so no ``ProtoOAClosePositionReq``
            # can be sent. Returning a phantom OPEN ``ExchangeOrder`` would
            # leave the sync engine waiting for a fill that never arrives;
            # decline via ``OrderSkippedByPlugin`` instead — the engine logs a
            # warning and re-evaluates next bar without halting (a non-EntryIntent
            # ``ExchangeOrderRejectedError`` would halt the bot on a recoverable
            # below-step close).
            raise OrderSkippedByPlugin(
                f"Skipping {intent.symbol} {intent.side.upper()} close "
                f"id={intent.pine_id!r}: size {intent.qty} below cTrader "
                f"minimum step. No order sent.",
                intent_key=intent.intent_key, reason="below_min_volume",
                context={'symbol': intent.symbol, 'qty': intent.qty},
            )
        for leg_id, volume in dispatched:
            event = await self._dispatch_order(
                _oa.ProtoOAClosePositionReq(
                    ctidTraderAccountId=self._live_account_id,
                    positionId=leg_id, volume=volume,
                ),
                coid=coid, context="close",
            )
            last_order = event.order
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'close_dispatched', client_order_id=coid,
                exchange_order_id=','.join(str(leg_id) for leg_id, _ in dispatched),
                intent_key=intent.intent_key,
            )
        # The engine reads only ``.id`` off this return; the per-leg FILLs carry
        # the actual reduction through ``watch_orders``. Surface the last leg's
        # order id (or the close COID) as the representative handle.
        avg_price = ((last_order.executionPrice or None)
                     if last_order is not None else None)
        return ExchangeOrder(
            id=(str(last_order.orderId)
                if last_order is not None and last_order.orderId else coid),
            symbol=intent.symbol, side=intent.side, order_type=OrderType.MARKET,
            qty=intent.qty, filled_qty=0.0, remaining_qty=intent.qty,
            price=None, stop_price=None, average_fill_price=avg_price,
            status=OrderStatus.OPEN,
            timestamp=epoch_time(), fee=0.0, fee_currency='',
            reduce_only=True, client_order_id=coid,
        )

    @override
    async def execute_cancel(self, envelope: DispatchEnvelope) -> bool:
        """Cancel the pending working order / native exit bracket for the intent.

        Two namespaces map to one ``CancelIntent`` here. An entry cancel targets
        a working order, looked up by Pine id. An exit cancel (``from_entry``
        set) targets the position's native SL/TP/trailing bracket, which
        :meth:`execute_exit` attached as a *position attribute* — there is no
        working order keyed by the exit id, so clearing it is a fresh
        ``ProtoOAAmendPositionSLTPReq`` with no protective fields set (cTrader
        treats the amend as a full overwrite, so an empty one removes the
        bracket). Without this an exit the script cancels / stops emitting would
        leave the old broker-side bracket live to close the position later
        against Pine's state.

        Idempotent: a missing live row, no open position, or a ``*_NOT_FOUND``
        from the exchange (already filled / cancelled) is a benign no-op
        returning ``True``.
        """
        intent = envelope.intent
        assert isinstance(intent, CancelIntent)
        # An exit cancel (``from_entry`` set) always targets the position's
        # native bracket, never a working order — the exit id can collide with
        # a still-live entry id (the common ``strategy.exit("Long",
        # from_entry="Long")`` reuse), so the working-order lookup must not run
        # first or it would cancel the entry order and leave the bracket live.
        if intent.from_entry is not None:
            return await self._clear_exit_bracket(intent, envelope)
        order_id = self._find_working_order_id(intent.symbol, intent.pine_id)
        if order_id is None:
            return True
        try:
            event = await self._dispatch_order(
                _oa.ProtoOACancelOrderReq(
                    ctidTraderAccountId=self._live_account_id, orderId=order_id,
                ),
                coid=envelope.client_order_id(KIND_CANCEL), context="cancel",
            )
        except ExchangeOrderRejectedError as exc:
            if isinstance(exc.__cause__, CTraderProtocolError) and is_not_found(
                    exc.__cause__.error_code):
                return True
            raise
        # ``ORDER_CANCEL_REJECTED`` (a cancel/modify race where the working
        # order was not cancelled) is reported as a non-error execution event,
        # so ``_dispatch_order`` returns it instead of raising. Reporting it as
        # a confirmed cancel would let the sync engine drop its mapping while
        # the order is still live and may later fill; ``False`` keeps it pending
        # so reconcile retries.
        if (event.executionType
                == _model.ProtoOAExecutionType.ORDER_CANCEL_REJECTED):
            return False
        return True

    async def _clear_exit_bracket(
            self, intent: CancelIntent, envelope: DispatchEnvelope,
    ) -> bool:
        """Remove the position's native SL/TP/trailing bracket (exit cancel).

        Sends a ``ProtoOAAmendPositionSLTPReq`` carrying only the ``positionId``
        — cTrader overwrites the whole protection set, so an amend with no
        ``stopLoss`` / ``takeProfit`` / ``trailingStopLoss`` clears the bracket
        without a cancel+recreate window. A flat symbol (no open position) or a
        ``*_NOT_FOUND`` race is a benign no-op.

        On a HEDGED account the clear is limited to the legs on the aggregate
        position side — the same legs :meth:`_emulated_exit` attached the bracket
        to. A symbol can also hold opposite-side legs (a manual hedge, or a leg
        opened by a resting reversal fill) that this exit never protected;
        amending them too would strip their own bracket.
        """
        if self.hedging_enabled:
            legs = await self.fetch_raw_positions(intent.symbol)
            pos = aggregate_positions(intent.symbol, legs)
            if pos is None or pos.side == 'flat':
                return True
            open_side = 'buy' if pos.side == 'long' else 'sell'
            for leg in legs:
                if leg.side != open_side:
                    continue
                try:
                    await self._dispatch_order(
                        _oa.ProtoOAAmendPositionSLTPReq(
                            ctidTraderAccountId=self._live_account_id,
                            positionId=int(leg.leg_id),
                        ),
                        coid=envelope.client_order_id(KIND_CANCEL),
                        context="clear bracket",
                    )
                except ExchangeOrderRejectedError as exc:
                    if isinstance(exc.__cause__, CTraderProtocolError) and is_not_found(
                            exc.__cause__.error_code):
                        continue
                    raise
            return True
        position_id = await self._find_open_position_id(intent.symbol)
        if position_id is None:
            return True
        try:
            await self._dispatch_order(
                _oa.ProtoOAAmendPositionSLTPReq(
                    ctidTraderAccountId=self._live_account_id,
                    positionId=position_id,
                ),
                coid=envelope.client_order_id(KIND_CANCEL),
                context="clear bracket",
            )
        except ExchangeOrderRejectedError as exc:
            if isinstance(exc.__cause__, CTraderProtocolError) and is_not_found(
                    exc.__cause__.error_code):
                return True
            raise
        return True

    @override
    async def execute_cancel_with_outcome(
            self, envelope: DispatchEnvelope,
    ) -> CancelDispositionOutcome:
        """Cancel a working order and classify the precise disposition.

        cTrader's execution events disambiguate the four terminal cancel
        dispositions that the bool-only :meth:`execute_cancel` cannot, so this
        override drives the sync engine's cancel-tentative state machine
        precisely.
        """
        intent = envelope.intent
        assert isinstance(intent, CancelIntent)
        order_id = self._find_working_order_id(intent.symbol, intent.pine_id)
        if order_id is None:
            return CancelDispositionOutcome.UNKNOWN
        try:
            event = await self._dispatch_order(
                _oa.ProtoOACancelOrderReq(
                    ctidTraderAccountId=self._live_account_id, orderId=order_id,
                ),
                coid=envelope.client_order_id(KIND_CANCEL), context="cancel",
            )
        except ExchangeOrderRejectedError as exc:
            cause = exc.__cause__
            if isinstance(cause, CTraderProtocolError) and is_not_found(cause.error_code):
                # Order is gone, but the event channel will reveal whether it
                # filled or cancelled — keep the tentative armed.
                return CancelDispositionOutcome.UNKNOWN
            raise
        exec_type = event.executionType
        if exec_type == _model.ProtoOAExecutionType.ORDER_CANCELLED:
            return CancelDispositionOutcome.CANCEL_CONFIRMED
        if exec_type == _model.ProtoOAExecutionType.ORDER_FILLED:
            return CancelDispositionOutcome.ALREADY_FILLED
        # ``ORDER_CANCEL_REJECTED`` says only that the cancel request was
        # refused — a cancel/modify race. It does NOT carry the no-fill
        # guarantee that ``TOO_LATE_TO_CANCEL`` encodes, so the working order
        # may still be live or may have filled. Reporting it as
        # ``TOO_LATE_TO_CANCEL`` (treated like a confirmed no-fill cancel) would
        # let the both-set entry-stop resolver fire its stop-market leg against
        # a LIMIT that is still live and can double-open. Map it to ``UNKNOWN``
        # so the cancel-tentative stays armed and a later fill / cancel signal
        # resolves the disposition.
        return CancelDispositionOutcome.UNKNOWN

    # --- modify (atomic amend) --------------------------------------------

    @override
    async def modify_entry(
            self, old: DispatchEnvelope, new: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Atomically amend a pending working order (price and/or size).

        ``ProtoOAAmendOrderReq`` amends ``volume`` as well as the price, so the
        default cancel+recreate is replaced by an in-place amend. Falls back to
        the base cancel+recreate when the working order id cannot be resolved
        (already filled / no persistence).
        """
        intent = new.intent
        assert isinstance(intent, EntryIntent)
        order_id = self._find_working_order_id(intent.symbol, intent.pine_id)
        if order_id is None:
            return await super().modify_entry(old, new)
        rules = await self._get_symbol_rules(intent.symbol)
        volume = quantize_volume(intent.qty, rules.step_volume)
        coid = new.client_order_id(KIND_MODIFY_ENTRY)
        req = _oa.ProtoOAAmendOrderReq(
            ctidTraderAccountId=self._live_account_id,
            orderId=order_id, volume=volume,
        )
        if intent.order_type is OrderType.LIMIT and intent.limit is not None:
            req.limitPrice = round_price(intent.limit, rules.digits)
        elif intent.order_type is OrderType.STOP and intent.stop is not None:
            req.stopPrice = round_price(intent.stop, rules.digits)
        await self._dispatch_order(req, coid=coid, context="amend entry")
        qty_units = volume_to_units(volume)
        return [ExchangeOrder(
            id=str(order_id), symbol=intent.symbol, side=intent.side,
            order_type=intent.order_type, qty=qty_units, filled_qty=0.0,
            remaining_qty=qty_units,
            price=(round_price(intent.limit, rules.digits)
                   if intent.order_type is OrderType.LIMIT and intent.limit is not None
                   else None),
            stop_price=(round_price(intent.stop, rules.digits)
                        if intent.order_type is OrderType.STOP and intent.stop is not None
                        else None),
            average_fill_price=None, status=OrderStatus.OPEN, timestamp=epoch_time(),
            fee=0.0, fee_currency='', reduce_only=False, client_order_id=coid,
        )]

    @override
    async def modify_exit(
            self, old: DispatchEnvelope, new: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Atomically amend the position's TP / SL in place (no protection gap).

        cTrader's protective levels are position attributes, so a fresh
        ``ProtoOAAmendPositionSLTPReq`` overwrites them with no cancel+recreate
        window — the clean win over a separate-order venue. Falls back to the
        base cancel+new path when the position cannot be found.
        """
        intent = new.intent
        assert isinstance(intent, ExitIntent)
        rules = await self._get_symbol_rules(intent.symbol)
        if self.hedging_enabled:
            legs = await self.fetch_raw_positions(intent.symbol)
            pos = aggregate_positions(intent.symbol, legs)
            if pos is None or pos.side == 'flat':
                return await super().modify_exit(old, new)
            open_side = 'buy' if pos.side == 'long' else 'sell'
            target_legs = [leg for leg in legs if leg.side == open_side]
            coid = new.client_order_id(KIND_MODIFY_EXIT)
            for leg in target_legs:
                req = _oa.ProtoOAAmendPositionSLTPReq(
                    ctidTraderAccountId=self._live_account_id,
                    positionId=int(leg.leg_id),
                )
                self._apply_bracket_levels(req, intent, rules)
                await self._dispatch_order(req, coid=coid, context="amend bracket")
            return self._build_bracket_legs(
                intent, new, int(target_legs[0].leg_id), rules,
            )
        position_id = await self._find_open_position_id(intent.symbol)
        if position_id is None:
            return await super().modify_exit(old, new)
        req = _oa.ProtoOAAmendPositionSLTPReq(
            ctidTraderAccountId=self._live_account_id, positionId=position_id,
        )
        self._apply_bracket_levels(req, intent, rules)
        await self._dispatch_order(req, coid=new.client_order_id(KIND_MODIFY_EXIT),
                                   context="amend bracket")
        return self._build_bracket_legs(intent, new, position_id, rules)

    # --- native fail-safe actuator + observed feed (§2.6.7) ----------------

    async def publish_native_failsafe_sl(
            self, snapshot: "NativeBracketSnapshot",
    ) -> None:
        """Push the engine's worst-case SL onto the netted position (fail-safe).

        The §2.6.7 actuator the runner wires through
        ``set_native_bracket_dispatcher``: the partial-qty bracket
        (``partial_qty_bracket_exit = SOFTWARE``) is engine-driven, so the
        broker-side protective stop behind it is placed here, distinct from
        :meth:`execute_exit`'s whole-row bracket. cTrader's protective levels
        are position attributes, so this is one ``ProtoOAAmendPositionSLTPReq``
        overwriting the whole set — the same amend path :meth:`execute_exit`
        uses. Raises (rather than returning) on an unresolved position or a
        rejected amend, so the engine records a PUT failure and degrades the
        fail-safe instead of believing the stop landed.

        On a HEDGED account the parent one-way position can span several broker
        legs (pyramided entries each open their own leg), so the worst-case SL
        is replicated onto EVERY leg on the position side — the same per-leg
        fan-out :meth:`_emulated_exit` uses. Amending only the parent entry's
        own leg would leave the other legs without the downtime stop and report
        the fail-safe healthy while part of the exposure is unprotected.

        On a successful PUT the desired levels are fed straight back to the
        engine's observed sink. M2 has no reconcile pass yet (M3), so the
        actuator self-confirms its own push — enough to flip the fail-safe
        ``DEGRADING -> HEALTHY``. The independent reconcile-based observation
        (external-edit / restart detection) is M3.

        :param snapshot: Desired bracket triple + parent COID + generation.
        """
        if self.store_ctx is None:
            raise CTraderBrokerError(
                "native fail-safe SL refused: no store_ctx to resolve the "
                f"positionId for parent {snapshot.parent_entry_dispatch_ref!r}"
            )
        rules = await self._get_symbol_rules(snapshot.symbol)
        if self.hedging_enabled:
            position_ids = await self._failsafe_leg_ids(snapshot)
        else:
            single = self._position_id_for_ref(snapshot.parent_entry_dispatch_ref)
            if not single:
                raise CTraderBrokerError(
                    "native fail-safe SL refused: unresolved positionId for parent "
                    f"{snapshot.parent_entry_dispatch_ref!r} — refusing to amend a "
                    "guessed position."
                )
            position_ids = [single]
        for position_id in position_ids:
            req = _oa.ProtoOAAmendPositionSLTPReq(
                ctidTraderAccountId=self._live_account_id, positionId=position_id,
            )
            if snapshot.stop_level is not None:
                req.stopLoss = round_price(snapshot.stop_level, rules.digits)
            if snapshot.profit_level is not None:
                req.takeProfit = round_price(snapshot.profit_level, rules.digits)
            if snapshot.trailing_stop is not None:
                req.trailingStopLoss = True
            await self._dispatch_order(
                req, coid=snapshot.parent_entry_dispatch_ref,
                context="native fail-safe SL",
            )
        self.store_ctx.log_event(
            'native_failsafe_sl_put',
            client_order_id=snapshot.parent_entry_dispatch_ref,
            exchange_order_id=','.join(str(pid) for pid in position_ids),
            payload={'stop_level': snapshot.stop_level,
                     'profit_level': snapshot.profit_level,
                     'trailing_stop': snapshot.trailing_stop,
                     'generation': snapshot.generation},
        )
        sink = self.native_failsafe_observed_sink
        if sink is not None:
            # M2 self-confirm: the PUT succeeded (no raise), so the desired
            # levels are now in place. cTrader has no relative trailing field,
            # so only the absolute stop is fed back (trailing_stop stays None).
            sink(
                snapshot.parent_entry_dispatch_ref,
                stop_level=snapshot.stop_level,
                profit_level=snapshot.profit_level,
                trailing_stop=None,
            )

    async def _failsafe_leg_ids(
            self, snapshot: "NativeBracketSnapshot",
    ) -> list[int]:
        """Resolve every HEDGED leg the fail-safe SL must protect.

        Fans across all open legs on the parent's position side (the same set
        :meth:`_emulated_exit` amends), so a pyramided multi-leg position gets
        the worst-case stop on every leg. ``parent_side`` (``'long'`` /
        ``'short'``) selects the leg side; an empty / flat result raises so the
        engine degrades the fail-safe rather than believing it landed.
        """
        legs = await self.fetch_raw_positions(snapshot.symbol)
        pos = aggregate_positions(snapshot.symbol, legs)
        if pos is None or pos.side == 'flat':
            raise CTraderBrokerError(
                "native fail-safe SL refused: no open legs for parent "
                f"{snapshot.parent_entry_dispatch_ref!r} on "
                f"{snapshot.symbol!r} — refusing to amend a guessed position."
            )
        open_side = 'buy' if snapshot.parent_side == 'long' else 'sell'
        leg_ids = [int(leg.leg_id) for leg in legs if leg.side == open_side]
        if not leg_ids:
            raise CTraderBrokerError(
                "native fail-safe SL refused: no "
                f"{snapshot.parent_side} legs for parent "
                f"{snapshot.parent_entry_dispatch_ref!r} on {snapshot.symbol!r}."
            )
        return leg_ids

    def _position_id_for_ref(self, parent_ref: str) -> int | None:
        """Resolve a parent entry's netted ``positionId`` from the BrokerStore.

        Prefers the per-entry ``extras['position_id']`` mirror, falling back to
        a numeric ``exchange_order_id`` (set to the positionId at fill time).
        """
        if self.store_ctx is None:
            return None
        row = self.store_ctx.get_order(parent_ref)
        if row is None:
            return None
        pid = (row.extras or {}).get('position_id')
        if pid:
            return int(pid)
        xid = row.exchange_order_id
        if xid and xid.isdigit():
            return int(xid)
        return None
