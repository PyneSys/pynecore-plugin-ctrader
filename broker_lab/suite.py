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

_RULES = _SymbolRules(
    symbol_id=1,
    digits=5,
    min_volume=1000,
    step_volume=1000,
    max_volume=10_000_000,
)


def _accepted_event(request, order_id: int) -> oa.ProtoOAExecutionEvent:
    event = oa.ProtoOAExecutionEvent(executionType=model.ProtoOAExecutionType.ORDER_ACCEPTED)
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
        self.events: asyncio.Queue[Any] = asyncio.Queue()

    @property
    def is_connected(self) -> bool:
        return True

    async def send_request(self, request):
        self.requests.append(request)
        if isinstance(request, oa.ProtoOANewOrderReq):
            order_id = self.profile.state.next_id
            self.profile.state.next_id += 1
            event = _accepted_event(request, order_id)
            self.orders[order_id] = event.order
            return event
        if isinstance(request, oa.ProtoOACancelOrderReq):
            order = self.orders.pop(request.orderId)
            order.orderStatus = model.ProtoOAOrderStatus.ORDER_STATUS_CANCELLED
            event = oa.ProtoOAExecutionEvent(executionType=model.ProtoOAExecutionType.ORDER_CANCELLED)
            event.order.CopyFrom(order)
            return event
        if isinstance(request, oa.ProtoOAReconcileReq):
            return oa.ProtoOAReconcileRes(order=list(self.orders.values()))
        raise AssertionError(f"unexpected offline cTrader wire request: {type(request).__name__}")


class OfflineCTrader(CTrader):
    """Real cTrader execution code with an in-memory correlated wire."""

    def __init__(self, profile: "CTraderProfile", run_name: str, store_ctx: Any) -> None:
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
        self.profile.state.calls.append((self.run_name, "entry", envelope.intent.intent_key))
        return orders

    async def execute_cancel(self, envelope):
        result = await super().execute_cancel(envelope)
        if result:
            for record in self.profile.state.orders.values():
                if record.run_name == self.run_name and record.pine_id == envelope.intent.pine_id:
                    record.order = replace(
                        record.order,
                        status=OrderStatus.CANCELLED,
                        remaining_qty=0.0,
                    )
        return result


class CTraderProfile(ReferenceVenueProfile):
    """cTrader profile using real snapshot and correlated-ACK translation."""

    plugin_name = "ctrader-offline-lab"
    symbol = "EURUSD"
    timeframe = "60"
    quantity_step = 10.0

    def __init__(self) -> None:
        super().__init__()
        self.wire = OfflineWire(self)

    def create_broker(self, run_name: str, store_ctx: Any) -> OfflineCTrader:
        return OfflineCTrader(self, run_name, store_ctx)

    def handle_step(self, runner: Any, step: Step) -> bool:
        if step.kind == "expect_ctrader_request":
            requests = [request for request in self.wire.requests if isinstance(request, oa.ProtoOANewOrderReq)]
            if not requests:
                raise AssertionError("cTrader did not issue an order request")
            request = requests[-1]
            for key, value in step.values.items():
                if getattr(request, key) != value:
                    raise AssertionError(f"expected cTrader request {key}={value!r}, got {getattr(request, key)!r}")
            return True
        if step.kind == "expect_ctrader_open_orders":
            broker = runner.runs[step.run].broker
            orders = asyncio.run(broker.get_open_orders(self.symbol))
            expected = int(step.values["count"])
            if len(orders) != expected:
                raise AssertionError(f"expected {expected} cTrader open orders, got {len(orders)}")
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
        steps = [Step("entry", values=values), Step("sync", values={"last_price": 1.10})]
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
