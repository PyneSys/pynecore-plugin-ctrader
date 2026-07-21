"""Opt-in offline conformance scenarios for the cTrader broker plugin."""

import asyncio
from dataclasses import replace
from typing import Any

from pynecore.core.broker.models import LegType, OrderStatus
from pynecore.testing.broker_lab import Scenario, Step, pairwise_cases
from pynecore.testing.broker_lab.reference import (
    ReferenceVenueProfile,
    VenueOrder,
)
from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.messages import OpenApiMessages_pb2 as oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as model
from pynecore_ctrader.models import _SymbolRules
from pynecore_ctrader.wire import (
    CTraderConnectionError,
    CTraderRequestSentConnectionError,
)

_RULES = _SymbolRules(
    symbol_id=1,
    digits=5,
    min_volume=1000,
    step_volume=1000,
    max_volume=10_000_000,
)


def _accepted_event(request, order_id: int) -> oa.ProtoOAExecutionEvent:
    event = oa.ProtoOAExecutionEvent(
        executionType=model.ProtoOAExecutionType.ORDER_ACCEPTED
    )
    event.order.CopyFrom(
        model.ProtoOAOrder(
            orderId=order_id,
            orderType=request.orderType,
            orderStatus=model.ProtoOAOrderStatus.ORDER_STATUS_ACCEPTED,
            clientOrderId=request.clientOrderId,
            limitPrice=request.limitPrice,
            stopPrice=request.stopPrice,
            tradeData=model.ProtoOATradeData(
                symbolId=1,
                volume=request.volume,
                tradeSide=request.tradeSide,
            ),
        )
    )
    return event


class OfflineWire:
    """Account-scoped fake below cTrader's correlation-aware dispatch layer."""

    def __init__(self, profile: "CTraderProfile") -> None:
        self.profile = profile
        self.requests: list[Any] = []
        self.orders: dict[int, model.ProtoOAOrder] = {}
        self.positions: dict[int, model.ProtoOAPosition] = {}
        self.deals: list[model.ProtoOADeal] = []
        self.events: asyncio.Queue[Any] = asyncio.Queue()
        self.fail_next_new: str | None = None
        self.connected = True
        self.connect_calls = 0
        self.fail_connects = 0

    @property
    def is_connected(self) -> bool:
        return self.connected

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.fail_connects > 0:
            self.fail_connects -= 1
            self.connected = False
            raise CTraderConnectionError("injected cTrader connect failure")
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    async def send_request(self, request):
        self.requests.append(request)
        if isinstance(request, oa.ProtoOAApplicationAuthReq):
            return oa.ProtoOAApplicationAuthRes()
        if isinstance(request, oa.ProtoOAGetAccountListByAccessTokenReq):
            return oa.ProtoOAGetAccountListByAccessTokenRes(
                accessToken=request.accessToken,
                ctidTraderAccount=[
                    model.ProtoOACtidTraderAccount(
                        ctidTraderAccountId=999,
                        isLive=False,
                    )
                ],
            )
        if isinstance(request, oa.ProtoOAAccountAuthReq):
            return oa.ProtoOAAccountAuthRes(ctidTraderAccountId=999)
        if isinstance(request, oa.ProtoOATraderReq):
            return oa.ProtoOATraderRes(
                ctidTraderAccountId=999,
                trader=model.ProtoOATrader(
                    ctidTraderAccountId=999,
                    accountType=model.ProtoOAAccountType.HEDGED,
                    moneyDigits=2,
                    depositAssetId=1,
                ),
            )
        if isinstance(request, oa.ProtoOANewOrderReq):
            fault = self.fail_next_new
            self.fail_next_new = None
            if fault == "pre_write":
                raise CTraderConnectionError("injected pre-write disconnect")
            order_id = self.profile.state.next_id
            self.profile.state.next_id += 1
            event = _accepted_event(request, order_id)
            self.orders[order_id] = event.order
            if fault == "post_write":
                raise CTraderRequestSentConnectionError(
                    "injected post-write disconnect"
                )
            return event
        if isinstance(request, oa.ProtoOACancelOrderReq):
            order = self.orders.pop(request.orderId)
            order.orderStatus = model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED
            event = oa.ProtoOAExecutionEvent(
                executionType=model.ProtoOAExecutionType.ORDER_CANCELLED
            )
            event.order.CopyFrom(order)
            return event
        if isinstance(request, oa.ProtoOAReconcileReq):
            return oa.ProtoOAReconcileRes(
                order=list(self.orders.values()),
                position=list(self.positions.values()),
            )
        if isinstance(request, oa.ProtoOADealListReq):
            return oa.ProtoOADealListRes(deal=self.deals, hasMore=False)
        if isinstance(request, oa.ProtoOAClosePositionReq):
            position = self.positions[request.positionId]
            closed_volume = min(request.volume, position.tradeData.volume)
            residual = position.tradeData.volume - closed_volume
            order_id = self.profile.state.next_id
            self.profile.state.next_id += 1
            order = model.ProtoOAOrder(
                orderId=order_id,
                orderType=model.ProtoOAOrderType.MARKET,
                orderStatus=model.ProtoOAOrderStatus.ORDER_STATUS_FILLED,
                executedVolume=closed_volume,
                executionPrice=position.price,
                positionId=request.positionId,
                closingOrder=True,
                tradeData=model.ProtoOATradeData(
                    symbolId=1,
                    volume=closed_volume,
                    tradeSide=(
                        model.ProtoOATradeSide.SELL
                        if position.tradeData.tradeSide == model.ProtoOATradeSide.BUY
                        else model.ProtoOATradeSide.BUY
                    ),
                ),
            )
            event = oa.ProtoOAExecutionEvent(
                executionType=model.ProtoOAExecutionType.ORDER_FILLED
            )
            event.order.CopyFrom(order)
            event.deal.CopyFrom(
                model.ProtoOADeal(
                    dealId=order_id + 10_000,
                    orderId=order_id,
                    positionId=request.positionId,
                    filledVolume=closed_volume,
                    executionPrice=position.price,
                    closePositionDetail=model.ProtoOAClosePositionDetail(
                        closedVolume=closed_volume
                    ),
                )
            )
            if residual:
                position.tradeData.volume = residual
                event.position.CopyFrom(position)
            else:
                del self.positions[request.positionId]
            run_name = self.profile.dispatch_run
            if run_name is None:
                raise AssertionError("cTrader close request has no dispatch owner")
            self.profile.pending_events.setdefault(run_name, []).append(event)
            leg_key = self.profile.position_keys[request.positionId]
            signed = closed_volume / 100.0
            current = self.profile.state.position_legs.get(leg_key, 0.0)
            residual_units = max(0.0, abs(current) - signed)
            updated = residual_units if current >= 0 else -residual_units
            self.profile.state.position_legs[leg_key] = updated
            owner = self.profile.state.position_owners.get(run_name, 0.0)
            self.profile.state.position_owners[run_name] = (
                owner - signed if current >= 0 else owner + signed
            )
            self.profile.state.position = sum(
                self.profile.state.position_owners.values()
            )
            return event
        raise AssertionError(
            f"unexpected offline cTrader wire request: {type(request).__name__}"
        )


class OfflineCTrader(CTrader):
    """Real cTrader execution code with an in-memory correlated wire."""

    def __init__(
        self, profile: "CTraderProfile", run_name: str, store_ctx: Any
    ) -> None:
        super().__init__(
            symbol=profile.symbol,
            config=CTraderConfig(
                demo=True,
                client_id="offline",
                client_secret="offline",
                account_id="999",
            ),
        )
        self.profile = profile
        self.run_name = run_name
        self.store_ctx = store_ctx
        self._live_account_id = 999
        self._symbols_by_name = {profile.symbol: 1}
        self._symbols_by_id = {1: profile.symbol}
        self._symbol_rules = {profile.symbol: _RULES}
        self._wire = profile.wire
        self._exec_events = asyncio.Queue()
        self._hedging_enabled = True
        self.position_port = self
        self._tokens.access_token = "offline-token"

    def _make_wire(self):
        return self.profile.wire

    async def _get_symbol_rules(self, symbol: str) -> _SymbolRules:
        return _RULES

    async def execute_entry(self, envelope):
        orders = await super().execute_entry(envelope)
        for order in orders:
            self.profile.state.orders[order.id] = VenueOrder(
                order=order,
                run_name=self.run_name,
                pine_id=envelope.intent.pine_id,
                leg_type=LegType.ENTRY,
                intent_key=envelope.intent.intent_key,
            )
        self.profile.state.calls.append(
            (self.run_name, "entry", envelope.intent.intent_key)
        )
        return orders

    async def place_leg(self, envelope, qty):
        orders = await super().place_leg(envelope, qty)
        for order in orders:
            self.profile.state.orders[order.id] = VenueOrder(
                order=order,
                run_name=self.run_name,
                pine_id=envelope.intent.pine_id,
                leg_type=LegType.ENTRY,
                intent_key=envelope.intent.intent_key,
            )
        self.profile.state.calls.append(
            (self.run_name, "entry", envelope.intent.intent_key)
        )
        return orders

    async def execute_cancel(self, envelope):
        result = await super().execute_cancel(envelope)
        if result:
            for record in self.profile.state.orders.values():
                if (
                    record.run_name == self.run_name
                    and record.pine_id == envelope.intent.pine_id
                ):
                    record.order = replace(
                        record.order,
                        status=OrderStatus.CANCELLED,
                        remaining_qty=0.0,
                    )
        return result

    async def execute_close(self, envelope):
        self.profile.dispatch_run = self.run_name
        try:
            return await super().execute_close(envelope)
        finally:
            self.profile.dispatch_run = None

    async def close_leg(self, symbol, leg_id, volume, coid):
        self.profile.dispatch_run = self.run_name
        try:
            return await super().close_leg(symbol, leg_id, volume, coid)
        finally:
            self.profile.dispatch_run = None


class CTraderProfile(ReferenceVenueProfile):
    """cTrader profile using real snapshot and correlated-ACK translation."""

    plugin_name = "ctrader-offline-lab"
    symbol = "EURUSD"
    timeframe = "60"
    quantity_step = 10.0
    venue_mode = "hedged"

    def __init__(self) -> None:
        super().__init__()
        self.wire = OfflineWire(self)
        self.pending_events: dict[str, list[Any]] = {}
        self.position_keys: dict[int, tuple[str, str]] = {}
        self.dispatch_run: str | None = None

    def create_broker(self, run_name: str, store_ctx: Any) -> OfflineCTrader:
        return OfflineCTrader(self, run_name, store_ctx)

    def handle_step(self, runner: Any, step: Step) -> bool:
        if step.kind == "expect_ctrader_request":
            requests = [
                request
                for request in self.wire.requests
                if isinstance(request, oa.ProtoOANewOrderReq)
            ]
            if not requests:
                raise AssertionError("cTrader did not issue an order request")
            request = requests[-1]
            for key, value in step.values.items():
                if getattr(request, key) != value:
                    raise AssertionError(
                        f"expected cTrader request {key}={value!r}, got {getattr(request, key)!r}"
                    )
            return True
        if step.kind == "expect_ctrader_open_orders":
            broker = runner.runs[step.run].broker
            orders = asyncio.run(broker.get_open_orders(self.symbol))
            expected = int(step.values["count"])
            if len(orders) != expected:
                raise AssertionError(
                    f"expected {expected} cTrader open orders, got {len(orders)}"
                )
            return True
        if step.kind == "ctrader_fill_entry":
            runtime = runner.runs[step.run]
            records = [
                record
                for record in self.state.orders.values()
                if record.run_name == step.run
                and record.leg_type is LegType.ENTRY
                and record.order.status
                in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
            ]
            if not records:
                raise AssertionError("cTrader fill requires an open entry")
            record = records[-1]
            order_id = int(record.order.id)
            wire_order = self.wire.orders.pop(order_id)
            position_id = self.state.next_id + 1_000
            self.state.next_id += 1
            price = float(step.values.get("price", 1.10))
            filled_order = model.ProtoOAOrder()
            filled_order.CopyFrom(wire_order)
            filled_order.orderStatus = model.ProtoOAOrderStatus.ORDER_STATUS_FILLED
            filled_order.executedVolume = wire_order.tradeData.volume
            filled_order.executionPrice = price
            filled_order.positionId = position_id
            position = model.ProtoOAPosition(
                positionId=position_id,
                positionStatus=model.ProtoOAPositionStatus.POSITION_STATUS_OPEN,
                price=price,
                tradeData=model.ProtoOATradeData(
                    symbolId=1,
                    volume=wire_order.tradeData.volume,
                    tradeSide=wire_order.tradeData.tradeSide,
                ),
            )
            self.wire.positions[position_id] = position
            event = oa.ProtoOAExecutionEvent(
                executionType=model.ProtoOAExecutionType.ORDER_FILLED
            )
            event.order.CopyFrom(filled_order)
            event.position.CopyFrom(position)
            deal = model.ProtoOADeal(
                dealId=position_id + 10_000,
                orderId=order_id,
                positionId=position_id,
                filledVolume=wire_order.tradeData.volume,
                executionPrice=price,
                dealStatus=model.ProtoOADealStatus.FILLED,
                moneyDigits=2,
                commission=0,
                executionTimestamp=1_700_000_001_000,
            )
            event.deal.CopyFrom(deal)
            self.wire.deals.append(deal)
            self.pending_events.setdefault(step.run, []).append(event)
            qty = wire_order.tradeData.volume / 100.0
            signed = (
                qty
                if wire_order.tradeData.tradeSide == model.ProtoOATradeSide.BUY
                else -qty
            )
            key = (step.run, record.pine_id)
            self.position_keys[position_id] = key
            self.state.position_legs[key] = signed
            self.state.position_owners[step.run] = (
                self.state.position_owners.get(step.run, 0.0) + signed
            )
            self.state.position = sum(self.state.position_owners.values())
            record.order = replace(
                record.order,
                status=OrderStatus.FILLED,
                filled_qty=qty,
                remaining_qty=0.0,
                average_fill_price=price,
            )
            return True
        if step.kind == "ctrader_queue_pending_push":
            events = self.pending_events.get(step.run, [])
            if not events:
                raise AssertionError("cTrader PUSH queue is empty")
            runner.runs[step.run].broker._exec_events.put_nowait(events.pop(0))
            return True
        if step.kind == "ctrader_duplicate_pending_push":
            events = self.pending_events.get(step.run, [])
            if not events:
                raise AssertionError("cTrader PUSH queue is empty")
            events.insert(0, events[0])
            return True
        if step.kind == "ctrader_expect_duplicate_push_dropped":
            events = self.pending_events.get(step.run, [])
            if not events:
                raise AssertionError("cTrader PUSH queue is empty")
            event = runner.runs[step.run].broker._translate_exec_event(events.pop(0))
            if event is not None:
                raise AssertionError(
                    f"duplicate cTrader fill PUSH was emitted twice: {event}"
                )
            return True
        if step.kind == "ctrader_drop_pending_push":
            events = self.pending_events.get(step.run, [])
            if not events:
                raise AssertionError("cTrader PUSH queue is empty")
            events.pop(0)
            return True
        if step.kind == "ctrader_reconcile_once":
            runtime = runner.runs[step.run]

            async def reconcile() -> list[Any]:
                return [event async for event in runtime.broker._reconcile_snapshot()]

            events = asyncio.run(reconcile())
            if len(events) != int(step.values.get("events", 1)):
                raise AssertionError(
                    f"expected {step.values.get('events', 1)} cTrader reconcile events, got {events}"
                )
            for event in events:
                runtime.engine.on_order_event(event)
            if events:
                runtime.engine.apply_async_events()
            return True
        if step.kind == "expect_ctrader_leg":
            pine_id = str(step.values["id"])
            actual = self.state.position_legs.get((step.run, pine_id), 0.0)
            expected = float(step.values["qty"])
            if abs(actual - expected) > 1e-9:
                raise AssertionError(
                    f"expected cTrader leg {pine_id!r} qty={expected}, got {actual}"
                )
            if "wire_positions" in step.values:
                count = len(self.wire.positions)
                if count != int(step.values["wire_positions"]):
                    raise AssertionError(
                        f"expected {step.values['wire_positions']} cTrader positions, got {count}"
                    )
            return True
        if step.kind == "ctrader_fault_next_new":
            self.wire.fail_next_new = str(step.values["mode"])
            return True
        if step.kind == "expect_ctrader_wire_dispatch":
            new_requests = [
                request
                for request in self.wire.requests
                if isinstance(request, oa.ProtoOANewOrderReq)
            ]
            expected_requests = int(step.values["requests"])
            expected_orders = int(step.values["orders"])
            if (
                len(new_requests) != expected_requests
                or len(self.wire.orders) != expected_orders
            ):
                raise AssertionError(
                    "unexpected cTrader dispatch cardinality: "
                    f"requests={len(new_requests)}, physical_orders={len(self.wire.orders)}"
                )
            return True
        if step.kind == "ctrader_transient_connect_retry":
            broker = runner.runs[step.run].broker
            self.wire.connected = False
            self.wire.connect_calls = 0
            self.wire.fail_connects = 1

            async def connect_with_retry() -> None:
                try:
                    await broker.connect()
                except CTraderConnectionError:
                    await broker.connect()
                else:
                    raise AssertionError(
                        "cTrader transient connect fault did not surface"
                    )
                if not broker.is_connected or self.wire.connect_calls != 2:
                    raise AssertionError(
                        "cTrader transient connect was not retried exactly once: "
                        f"calls={self.wire.connect_calls}, connected={broker.is_connected}"
                    )
                await broker.disconnect()

            asyncio.run(connect_with_retry())
            return True
        if step.kind == "ctrader_permanent_connect_failure":
            broker = runner.runs[step.run].broker
            self.wire.connected = False
            self.wire.connect_calls = 0
            self.wire.fail_connects = 2

            async def fail_once() -> None:
                try:
                    await broker.connect()
                except CTraderConnectionError:
                    pass
                else:
                    raise AssertionError("cTrader permanent connect fault did not fail")
                if self.wire.connect_calls != 1 or broker.is_connected:
                    raise AssertionError(
                        "cTrader permanent connect did not fail fast: "
                        f"calls={self.wire.connect_calls}, connected={broker.is_connected}"
                    )

            asyncio.run(fail_once())
            return True
        return super().handle_step(runner, step)


def smoke_scenarios(seed: int = 0) -> list[Scenario]:
    return [
        Scenario(
            name="ctrader-correlated-ack-without-push",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "L", "side": "buy", "qty": 10.0}),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_ctrader_request",
                    values={
                        "volume": 1000,
                        "tradeSide": model.ProtoOATradeSide.BUY,
                        "orderType": model.ProtoOAOrderType.MARKET,
                    },
                ),
            ),
        ),
        Scenario(
            name="ctrader-real-connect-boundary-retries-transient-fault-once",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(Step("ctrader_transient_connect_retry"),),
        ),
        Scenario(
            name="ctrader-real-connect-boundary-fails-fast-on-permanent-fault",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(Step("ctrader_permanent_connect_failure"),),
        ),
        Scenario(
            name="ctrader-working-order-restart-ownership",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={"id": "S", "side": "sell", "qty": 10.0, "limit": 1.20},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step("restart"),
                Step(
                    "entry",
                    values={"id": "S", "side": "sell", "qty": 10.0, "limit": 1.20},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step("expect", values={"calls": 1}),
            ),
        ),
        Scenario(
            name="ctrader-cancel-ack-without-push-terminalizes",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={"id": "C", "side": "buy", "qty": 10.0, "limit": 1.05},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step("cancel", values={"id": "C"}),
                Step("sync", values={"last_price": 1.10}),
                Step("pump_watch"),
                Step("expect_ctrader_open_orders", values={"count": 0}),
            ),
        ),
        Scenario(
            name="ctrader-hedged-two-leg-restart-keyed-close-targets-only-one-position",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "A", "side": "buy", "qty": 10.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("ctrader_fill_entry", check_invariants=False),
                Step("ctrader_queue_pending_push", check_invariants=False),
                Step("pump_watch"),
                Step("entry", values={"id": "B", "side": "buy", "qty": 10.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("ctrader_fill_entry", check_invariants=False),
                Step("ctrader_queue_pending_push", check_invariants=False),
                Step("pump_watch"),
                Step("restart", check_invariants=False),
                Step("expect", values={"position": 20.0, "engine_position": 20.0}),
                Step("close", values={"id": "A", "qty": 10.0}),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step("ctrader_queue_pending_push", check_invariants=False),
                Step("pump_watch"),
                Step(
                    "expect_ctrader_leg",
                    values={"id": "A", "qty": 0.0, "wire_positions": 1},
                ),
                Step("expect_ctrader_leg", values={"id": "B", "qty": 10.0}),
                Step("expect", values={"position": 10.0, "engine_position": 10.0}),
            ),
        ),
        Scenario(
            name="ctrader-concurrent-opposite-runs-remain-owned-while-account-net-is-zero",
            profile_factory=CTraderProfile,
            runs=("A", "B"),
            seed=seed,
            steps=(
                Step(
                    "entry", run="A", values={"id": "Long", "side": "buy", "qty": 10.0}
                ),
                Step("sync", run="A", values={"last_price": 1.10}),
                Step("ctrader_fill_entry", run="A", check_invariants=False),
                Step("ctrader_queue_pending_push", run="A", check_invariants=False),
                Step("pump_watch", run="A"),
                Step(
                    "entry",
                    run="B",
                    values={"id": "Short", "side": "sell", "qty": 10.0},
                ),
                Step("sync", run="B", values={"last_price": 1.10}),
                Step("ctrader_fill_entry", run="B", check_invariants=False),
                Step("ctrader_queue_pending_push", run="B", check_invariants=False),
                Step("pump_watch", run="B"),
                Step("restart", run="A", check_invariants=False),
                Step("restart", run="B", check_invariants=False),
                Step(
                    "expect",
                    run="A",
                    values={
                        "position": 10.0,
                        "engine_position": 10.0,
                        "account_position": 0.0,
                    },
                ),
                Step(
                    "expect",
                    run="B",
                    values={"position": -10.0, "engine_position": -10.0},
                ),
                Step("close", run="A", values={"id": "Long", "qty": 10.0}),
                Step(
                    "sync", run="A", values={"last_price": 1.10}, check_invariants=False
                ),
                Step("ctrader_queue_pending_push", run="A", check_invariants=False),
                Step("pump_watch", run="A"),
                Step(
                    "expect",
                    run="A",
                    values={
                        "position": 0.0,
                        "engine_position": 0.0,
                        "account_position": -10.0,
                    },
                ),
                Step(
                    "expect",
                    run="B",
                    values={"position": -10.0, "engine_position": -10.0},
                ),
            ),
        ),
        Scenario(
            name="ctrader-duplicate-fill-push-and-restart-snapshot-are-exactly-once",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Replay", "side": "buy", "qty": 10.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("ctrader_fill_entry", check_invariants=False),
                Step("ctrader_duplicate_pending_push", check_invariants=False),
                Step("ctrader_queue_pending_push", check_invariants=False),
                Step("pump_watch"),
                Step("ctrader_expect_duplicate_push_dropped"),
                Step("restart", check_invariants=False),
                Step("expect", values={"position": 10.0, "engine_position": 10.0}),
            ),
        ),
        Scenario(
            name="ctrader-hedged-partial-close-preserves-target-leg-residual",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Large", "side": "buy", "qty": 20.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("ctrader_fill_entry", check_invariants=False),
                Step("ctrader_queue_pending_push", check_invariants=False),
                Step("pump_watch"),
                Step("restart", check_invariants=False),
                Step("close", values={"id": "Large", "qty": 10.0}),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step("ctrader_queue_pending_push", check_invariants=False),
                Step("pump_watch"),
                Step(
                    "expect_ctrader_leg",
                    values={"id": "Large", "qty": 10.0, "wire_positions": 1},
                ),
                Step("expect", values={"position": 10.0, "engine_position": 10.0}),
            ),
        ),
        Scenario(
            name="ctrader-hedged-reversal-closes-old-leg-before-opening-residual",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "Long", "side": "buy", "qty": 10.0}),
                Step("sync", values={"last_price": 1.10}),
                Step("ctrader_fill_entry", check_invariants=False),
                Step("ctrader_queue_pending_push", check_invariants=False),
                Step("pump_watch"),
                Step("restart", check_invariants=False),
                Step("entry", values={"id": "Short", "side": "sell", "qty": 20.0}),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step("ctrader_fill_entry", check_invariants=False),
                Step("ctrader_queue_pending_push", check_invariants=False),
                Step("pump_watch", check_invariants=False),
                Step("ctrader_queue_pending_push", check_invariants=False),
                Step("pump_watch"),
                Step(
                    "expect_ctrader_leg",
                    values={"id": "Long", "qty": 0.0, "wire_positions": 1},
                ),
                Step("expect_ctrader_leg", values={"id": "Short", "qty": -20.0}),
                Step("expect", values={"position": -20.0, "engine_position": -20.0}),
            ),
        ),
        Scenario(
            name="ctrader-pre-write-disconnect-retries-once-without-duplicate-order",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(
                Step("ctrader_fault_next_new", values={"mode": "pre_write"}),
                Step("entry", values={"id": "Retry", "side": "buy", "qty": 10.0}),
                Step(
                    "sync_expect_error",
                    values={"last_price": 1.10, "type": "ExchangeConnectionError"},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step(
                    "expect_ctrader_wire_dispatch", values={"requests": 2, "orders": 1}
                ),
            ),
        ),
        Scenario(
            name="ctrader-post-write-disconnect-parks-disposition-without-redispatch",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(
                Step("ctrader_fault_next_new", values={"mode": "post_write"}),
                Step("entry", values={"id": "Unknown", "side": "buy", "qty": 10.0}),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step("restart", check_invariants=False),
                Step("entry", values={"id": "Unknown", "side": "buy", "qty": 10.0}),
                Step("sync", values={"last_price": 1.10}, check_invariants=False),
                Step(
                    "expect_ctrader_wire_dispatch", values={"requests": 1, "orders": 1}
                ),
            ),
        ),
        Scenario(
            name="ctrader-missed-fill-push-is-recovered-once-from-reconcile-and-deal-history",
            profile_factory=CTraderProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={"id": "Gap", "side": "buy", "qty": 10.0, "limit": 1.05},
                ),
                Step("sync", values={"last_price": 1.10}),
                Step("ctrader_fill_entry", check_invariants=False),
                Step("ctrader_drop_pending_push", check_invariants=False),
                Step("ctrader_reconcile_once"),
                Step("ctrader_reconcile_once", values={"events": 0}),
                Step("expect", values={"position": 10.0, "engine_position": 10.0}),
            ),
        ),
    ]


def extended_scenarios(seed: int = 0) -> list[Scenario]:
    scenarios = smoke_scenarios(seed)
    axes = {
        "side": ("buy", "sell"),
        "order": ("market", "limit", "stop"),
        "restart": (False, True),
    }
    for index, case in enumerate(pairwise_cases(axes, seed=seed)):
        values: dict[str, Any] = {"id": "E", "side": case["side"], "qty": 10.0}
        if case["order"] == "limit":
            values["limit"] = 1.09 if case["side"] == "buy" else 1.11
        elif case["order"] == "stop":
            values["stop"] = 1.11 if case["side"] == "buy" else 1.09
        steps = [
            Step("entry", values=values),
            Step("sync", values={"last_price": 1.10}),
        ]
        if case["restart"]:
            steps.extend(
                (
                    Step("restart"),
                    Step("entry", values=values),
                    Step("sync", values={"last_price": 1.10}),
                )
            )
        scenarios.append(
            Scenario(
                name=f"ctrader-pairwise-{index:03d}",
                profile_factory=CTraderProfile,
                seed=seed,
                tags=frozenset({"extended"}),
                steps=tuple(steps),
            )
        )
    return scenarios


def build_suite(*, mode: str, seed: int) -> list[Scenario]:
    return smoke_scenarios(seed) if mode == "smoke" else extended_scenarios(seed)
