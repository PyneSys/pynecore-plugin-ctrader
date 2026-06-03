"""Broker state-query mix-in for the cTrader Open API plugin.

Implements the read side of :class:`~pynecore.core.plugin.broker.BrokerPlugin`
on top of :class:`~pynecore_ctrader._base._CTraderBase`:

- :meth:`get_open_orders` and :meth:`get_position` from one
  ``ProtoOAReconcileReq`` snapshot (``fetch_position = NATIVE``),
- :meth:`get_balance` from the cached ``ProtoOATrader`` record, and
- :meth:`get_capabilities` — the static capability declaration.

NETTING accounts hold at most one open position per symbol, so
:meth:`get_position` returns the single matching row (or ``None``).
"""
import logging
from typing import cast

from pynecore.core.broker.emulator import aggregate_positions
from pynecore.core.broker.models import (
    CapabilityLevel,
    ExchangeCapabilities,
    ExchangeOrder,
    ExchangePosition,
    OrderStatus,
    OrderType,
    PositionLeg,
)

from ._base import _CTraderBase
from .helpers import money_value, round_price, volume_to_units
from .messages import OpenApiMessages_pb2 as _oa
from .messages import OpenApiModelMessages_pb2 as _model
from .wire import CTraderConnectionError

logger = logging.getLogger(__name__)

#: ``ProtoOAOrderType`` -> PyneCore order type. Only the order kinds the broker
#: layer places are mapped; anything else (STOP_LOSS_TAKE_PROFIT protection
#: orders, MARKET_RANGE, STOP_LIMIT) is reported as the closest plain type.
_ORDER_TYPE_MAP = {
    _model.ProtoOAOrderType.MARKET: OrderType.MARKET,
    _model.ProtoOAOrderType.LIMIT: OrderType.LIMIT,
    _model.ProtoOAOrderType.STOP: OrderType.STOP,
}

#: ``ProtoOAOrderStatus`` -> PyneCore order status.
_ORDER_STATUS_MAP = {
    _model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED: OrderStatus.OPEN,
    _model.ProtoOAOrderStatus.ORDER_STATUS_FILLED: OrderStatus.FILLED,
    _model.ProtoOAOrderStatus.ORDER_STATUS_REJECTED: OrderStatus.REJECTED,
    _model.ProtoOAOrderStatus.ORDER_STATUS_EXPIRED: OrderStatus.EXPIRED,
    _model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED: OrderStatus.CANCELLED,
}


class _StateMixin(_CTraderBase):
    """Broker state queries + capability declaration."""

    def get_capabilities(self) -> ExchangeCapabilities:
        """Declare what the cTrader broker layer delivers end-to-end (§9.2).

        cTrader is stronger than a REST-poll venue on two axes: ``watch_orders``
        is a real PUSH ``ProtoOAExecutionEvent`` channel (NATIVE), and the
        order amend modifies ``volume`` too. The two starred levels
        (``amend_order``, ``idempotency``) are declared conservatively until
        the live verification in the M2 open-questions list confirms full
        native behaviour — under-declaring never lies to the validator.
        """
        return ExchangeCapabilities(
            stop_order=CapabilityLevel.NATIVE,
            trailing_stop=CapabilityLevel.NATIVE,
            tp_sl_bracket=CapabilityLevel.NATIVE,
            partial_qty_bracket_exit=CapabilityLevel.SOFTWARE,
            partial_qty_bracket_exit_pyramiding=CapabilityLevel.SOFTWARE,
            oca_cancel=CapabilityLevel.SOFTWARE,
            amend_order=CapabilityLevel.PARTIAL_NATIVE,
            cancel_all=CapabilityLevel.SOFTWARE,
            reduce_only=CapabilityLevel.SOFTWARE,
            watch_orders=CapabilityLevel.NATIVE,
            fetch_position=CapabilityLevel.NATIVE,
            idempotency=CapabilityLevel.PARTIAL_NATIVE,
        )

    async def _reconcile(
            self, *, return_protection_orders: bool = False,
    ) -> _oa.ProtoOAReconcileRes:
        """Fetch the account's open orders + positions snapshot.

        :param return_protection_orders: When ``True`` the snapshot's
            ``order[]`` also carries the position-attached SL/TP protection
            orders (needed by the reconcile loop's bracket-disappearance
            detection). The state-query callers leave it ``False`` — they map
            only standalone working orders and read protective levels off the
            position itself.
        :return: The ``ProtoOAReconcileRes`` for the live account.
        :raises CTraderConnectionError: If the live connection is not open.
        """
        wire = self._wire
        if wire is None or self._live_account_id is None:
            raise CTraderConnectionError("live connection not established")
        return cast(_oa.ProtoOAReconcileRes, await wire.send_request(
            _oa.ProtoOAReconcileReq(
                ctidTraderAccountId=self._live_account_id,
                returnProtectionOrders=return_protection_orders,
            )
        ))

    def _apply_adoption_baseline(self, res: _oa.ProtoOAReconcileRes) -> None:
        """Silently baseline live rows' ``filled_qty`` to the adoption snapshot.

        The engine's startup ``reconcile`` adopts the broker's NET position
        UNCONDITIONALLY and deal-independently (size + entry price), folding every
        pre-restart fill into ``BrokerPosition.size``. The PUSH and reconcile
        paths emit fills incrementally against each row's durable ``filled_qty``
        cursor; if that cursor is still pre-restart while the adopted net already
        counts the fill, the first post-restart emit would re-apply the
        pre-restart slice on top of the adopted size (``BrokerPosition.record_fill``
        has no ``dealId`` de-dup → double-count). So, once, on the first state
        query the startup adoption drives — this runs from :meth:`get_position` /
        :meth:`fetch_raw_positions`, the FIRST of which (per ``start_broker``) IS
        the adoption call — advance every live entry row's cursor up to what the
        adoption snapshot already reflects, and emit nothing. After this barrier
        the PUSH / reconcile paths emit only genuinely new fills.

        Per-order baseline (from the SAME snapshot the engine adopts, so the
        barrier can never silently absorb a later post-adoption fill):

        * order still in ``order[]`` -> its cumulative ``executedVolume``;
        * order gone from ``order[]`` but its ``position_id`` is adopted-open ->
          ``row.qty`` (it fully filled into the net — conservative, and exact for
          the common MARKET entry that vanishes from ``order[]`` the instant it
          fills);
        * gone from both -> left untouched, so the deal-history bridge can retire
          a filled-then-closed row or stamp a never-filled one.

        Monotonic (``set_filled`` writes the absolute value with no max of its
        own): only ever raises a cursor, clamped to the row's own size. A row
        whose PUSH-advanced cursor already reflects the fill (the rare
        ``drained_mutated_position`` startup, where a live fill was drained before
        adoption and the engine kept the mutated size instead of adopting) is a
        no-op here; the precise engine<->plugin coordination for that skip is the
        M3 startup-recovery (2.2) milestone's, where the diff core runs at
        ``connect`` under a single shared snapshot.
        """
        if self.store_ctx is None or self._adoption_baselined:
            return
        self._adoption_baselined = True
        executed_by_order_id = {o.orderId: o.executedVolume for o in res.order}
        open_position_ids = {
            p.positionId for p in res.position
            if p.positionStatus == _model.ProtoOAPositionStatus.POSITION_STATUS_OPEN
        }
        for row in list(self.store_ctx.iter_live_orders()):
            extras = row.extras or {}
            order_id_str = extras.get('order_id')
            baseline: float | None = None
            if order_id_str and int(order_id_str) in executed_by_order_id:
                baseline = volume_to_units(executed_by_order_id[int(order_id_str)])
            else:
                position_id = extras.get('position_id')
                if position_id and position_id in open_position_ids:
                    baseline = row.qty
            if baseline is None:
                continue
            cumulative = min(row.qty, max(row.filled_qty, baseline))
            if cumulative > row.filled_qty + 1e-9:
                self.store_ctx.set_filled(row.client_order_id, cumulative)

    async def _resolve_state_symbol_id(self, symbol: str) -> int | None:
        """Resolve ``symbol`` to its numeric ``symbolId``, fetching if needed.

        The light-symbol list is loaded lazily (the data path and the order
        path both fill it on first use), so a state query that runs before any
        of those — e.g. startup reconciliation — would otherwise see an empty
        cache. A missing id must NEVER degrade into a wildcard match: the
        callers filter positions / orders by this id, and a ``None`` treated as
        "any symbol" would adopt or amend the wrong instrument on a
        multi-symbol account. Returns ``None`` only when the symbol is genuinely
        not on the account.

        :param symbol: The symbol name to resolve.
        :return: The numeric ``symbolId``, or ``None`` if unknown to the account.
        :raises CTraderConnectionError: If the live connection is not open.
        """
        symbol_id = self._symbols_by_name.get(symbol)
        if symbol_id is not None:
            return symbol_id
        wire = self._wire
        if wire is None or self._live_account_id is None:
            raise CTraderConnectionError("live connection not established")
        await self._fetch_light_symbols(wire, self._live_account_id)
        return self._symbols_by_name.get(symbol)

    async def get_open_orders(
            self, symbol: str | None = None,
    ) -> list[ExchangeOrder]:
        """Return the account's pending working orders, optionally by symbol.

        Maps each ``ProtoOAOrder`` from the reconcile snapshot to an
        :class:`ExchangeOrder`. Protection (SL/TP) orders are skipped — they
        are position attributes the bracket path owns, not standalone working
        orders the engine tracks here.
        """
        res = await self._reconcile()
        want_id = await self._resolve_state_symbol_id(symbol) if symbol is not None else None
        if symbol is not None and want_id is None:
            # The symbol is not on the account — return no orders rather than
            # letting a ``None`` wildcard leak every other symbol's orders.
            return []
        result: list[ExchangeOrder] = []
        for order in res.order:
            order_type = _ORDER_TYPE_MAP.get(order.orderType)
            if order_type is None:
                # STOP_LOSS_TAKE_PROFIT / MARKET_RANGE / STOP_LIMIT — not a
                # working order the engine places or tracks.
                continue
            symbol_id = order.tradeData.symbolId
            if want_id is not None and symbol_id != want_id:
                continue
            qty = volume_to_units(order.tradeData.volume)
            filled = volume_to_units(order.executedVolume)
            side = ('buy' if order.tradeData.tradeSide == _model.ProtoOATradeSide.BUY
                    else 'sell')
            result.append(ExchangeOrder(
                id=str(order.orderId),
                symbol=self._symbol_name_for(symbol_id),
                side=side,
                order_type=order_type,
                qty=qty,
                filled_qty=filled,
                remaining_qty=max(0.0, qty - filled),
                price=order.limitPrice if order_type is OrderType.LIMIT else None,
                stop_price=order.stopPrice if order_type is OrderType.STOP else None,
                average_fill_price=order.executionPrice or None,
                status=_ORDER_STATUS_MAP.get(order.orderStatus, OrderStatus.OPEN),
                timestamp=order.utcLastUpdateTimestamp / 1000.0,
                fee=0.0,
                fee_currency='',
                reduce_only=order.closingOrder,
                client_order_id=order.clientOrderId or None,
            ))
        return result

    async def fetch_raw_positions(self, symbol: str) -> list[PositionLeg]:
        """Return every open broker leg for ``symbol`` (one-way emulation input).

        On a HEDGED account a symbol can carry several simultaneous open
        positions; this returns one :class:`PositionLeg` per open broker
        position, oldest first (by ``openTimestamp``) so the core FIFO close /
        reversal planner is deterministic and replay-stable. On a NETTING
        account it returns at most one leg. Performs ZERO aggregation — the core
        :mod:`~pynecore.core.broker.emulator` owns netting and leg selection.

        Unrealized P&L is left at ``0.0`` per leg (the reconcile snapshot does
        not carry it; the sync engine drives off size / side / entry price),
        matching :meth:`get_position`'s existing M2 behaviour.
        """
        want_id = await self._resolve_state_symbol_id(symbol)
        if want_id is None:
            return []
        res = await self._reconcile()
        self._apply_adoption_baseline(res)
        digits = self._symbol_rules[symbol].digits if symbol in self._symbol_rules else 5
        legs: list[PositionLeg] = []
        for position in res.position:
            if position.positionStatus != _model.ProtoOAPositionStatus.POSITION_STATUS_OPEN:
                continue
            if position.tradeData.symbolId != want_id:
                continue
            side = ('buy' if position.tradeData.tradeSide == _model.ProtoOATradeSide.BUY
                    else 'sell')
            legs.append(PositionLeg(
                leg_id=str(position.positionId),
                symbol=symbol,
                side=side,
                qty=volume_to_units(position.tradeData.volume),
                entry_price=round_price(position.price, digits),
                open_time=position.tradeData.openTimestamp / 1000.0,
                unrealized_pnl=0.0,
            ))
        legs.sort(key=lambda leg: leg.open_time)
        return legs

    async def get_position(self, symbol: str) -> ExchangePosition | None:
        """Return the net open position for ``symbol``, or ``None`` when flat.

        On a HEDGED account the symbol's legs are aggregated into one net
        one-way snapshot by the core emulator; on a NETTING account the single
        open position row is returned directly. The unrealized PnL is left at
        ``0.0`` in M2 — it is informational for the sync engine, which drives
        off size / side / entry price; a dedicated
        ``ProtoOAGetPositionUnrealizedPnLReq`` can populate it later.
        """
        if self.hedging_enabled:
            return aggregate_positions(symbol, await self.fetch_raw_positions(symbol))
        want_id = await self._resolve_state_symbol_id(symbol)
        if want_id is None:
            # Unknown symbol — report flat rather than adopting the first open
            # position of another instrument under the requested symbol's name.
            return None
        res = await self._reconcile()
        self._apply_adoption_baseline(res)
        for position in res.position:
            if position.positionStatus != _model.ProtoOAPositionStatus.POSITION_STATUS_OPEN:
                continue
            if position.tradeData.symbolId != want_id:
                continue
            digits = self._symbol_rules[symbol].digits if symbol in self._symbol_rules else 5
            side = ('long' if position.tradeData.tradeSide == _model.ProtoOATradeSide.BUY
                    else 'short')
            return ExchangePosition(
                symbol=symbol,
                side=side,
                size=volume_to_units(position.tradeData.volume),
                entry_price=round_price(position.price, digits),
                unrealized_pnl=0.0,
                liquidation_price=None,
                leverage=0.0,
                margin_mode='cross',
            )
        return None

    async def get_balance(self) -> dict[str, float]:
        """Return the account balance keyed by its deposit-asset currency.

        Reads the live ``ProtoOATrader`` record and decodes the INT64 balance
        with the account's ``moneyDigits``. The deposit-asset name is resolved
        from the asset list and cached on the instance.
        """
        wire = self._wire
        if wire is None or self._live_account_id is None:
            raise CTraderConnectionError("live connection not established")
        res = cast(_oa.ProtoOATraderRes, await wire.send_request(
            _oa.ProtoOATraderReq(ctidTraderAccountId=self._live_account_id)
        ))
        trader = res.trader
        money_digits = trader.moneyDigits
        balance = money_value(trader.balance, money_digits)
        currency = await self._deposit_asset_name(wire, trader.depositAssetId)
        return {currency: balance}

    async def _deposit_asset_name(self, wire, asset_id: int) -> str:
        """Resolve the deposit-asset name from the asset list (best-effort).

        Falls back to the stringified asset id when the list does not contain
        the asset, so :meth:`get_balance` always returns a usable key.
        """
        res = cast(_oa.ProtoOAAssetListRes, await wire.send_request(
            _oa.ProtoOAAssetListReq(ctidTraderAccountId=self._live_account_id)
        ))
        for asset in res.asset:
            if asset.assetId == asset_id:
                return asset.name or str(asset_id)
        return str(asset_id)
