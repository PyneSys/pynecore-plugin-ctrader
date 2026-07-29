"""
@pyne

Behavior-neutral wire telemetry and subscription-outcome coverage.
"""
import asyncio
import struct
from dataclasses import FrozenInstanceError

import pytest

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.messages import OpenApiCommonMessages_pb2 as _common
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model
from pynecore_ctrader.provider import SubscribeStatus
from pynecore_ctrader.wire import CTraderProtocolError, WireClient


def _envelope(message) -> _common.ProtoMessage:
    return _common.ProtoMessage(
        payloadType=message.payloadType,
        payload=message.SerializeToString(),
    )


class _FrameReader:
    def __init__(self, envelopes: list[_common.ProtoMessage]) -> None:
        self._chunks: list[bytes] = []
        for envelope in envelopes:
            body = envelope.SerializeToString()
            self._chunks.extend((struct.pack("!I", len(body)), body))

    async def readexactly(self, size: int) -> bytes:
        if not self._chunks:
            raise asyncio.IncompleteReadError(b"", size)
        chunk = self._chunks.pop(0)
        assert len(chunk) == size
        return chunk


class _Writer:
    def __init__(self) -> None:
        self.frames: list[bytes] = []
        self.closing = False

    def is_closing(self) -> bool:
        return self.closing

    def write(self, data: bytes) -> None:
        self.frames.append(data)

    @staticmethod
    async def drain() -> None:
        return None

    def close(self) -> None:
        self.closing = True

    @staticmethod
    async def wait_closed() -> None:
        return None


class _IdleReader:
    async def readexactly(self, size: int) -> bytes:
        del size
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class _SubscribeWire:
    def __init__(self, outcomes: list[Exception | None]) -> None:
        self.outcomes = list(outcomes)
        self.requests: list = []

    async def send_request(self, request):
        self.requests.append(request)
        outcome = self.outcomes.pop(0)
        if outcome is not None:
            raise outcome
        if isinstance(request, _oa.ProtoOASubscribeSpotsReq):
            return _oa.ProtoOASubscribeSpotsRes()
        return _oa.ProtoOASubscribeLiveTrendbarRes()


def _provider(wire: _SubscribeWire) -> CTrader:
    provider = CTrader(
        symbol="broker:EURUSD",
        timeframe="1",
        config=CTraderConfig(
            demo=True,
            client_id="client",
            client_secret="secret",
            account_id="999",
        ),
    )
    provider._wire = wire  # type: ignore[assignment]
    provider._live_account_id = 999
    provider._symbols_by_name = {"EURUSD": 1}
    return provider


def __test_wire_counts_heartbeats_and_spot_shapes_without_changing_routing__() -> None:
    heartbeat = _envelope(_common.ProtoHeartbeatEvent())
    spot_without = _envelope(
        _oa.ProtoOASpotEvent(ctidTraderAccountId=999, symbolId=1, bid=114000)
    )
    spot_with = _envelope(
        _oa.ProtoOASpotEvent(
            ctidTraderAccountId=999,
            symbolId=1,
            bid=114001,
            trendbar=[
                _model.ProtoOATrendbar(
                    utcTimestampInMinutes=30_000_000,
                    volume=1,
                )
            ],
        )
    )

    async def scenario() -> tuple[WireClient, _Writer]:
        client = WireClient("example.invalid")
        frame_writer = _Writer()
        client._reader = _FrameReader([heartbeat, spot_without, spot_with])  # type: ignore[assignment]
        client._writer = frame_writer  # type: ignore[assignment]
        await client._recv_loop()
        return client, frame_writer

    wire, writer = asyncio.run(scenario())
    snapshot = wire.telemetry_snapshot()

    assert snapshot.inbound_heartbeats == 1
    assert snapshot.spot_events_without_trendbar == 1
    assert snapshot.spot_events_with_trendbar == 1
    assert [type(wire.events.get_nowait()), type(wire.events.get_nowait())] == [
        _oa.ProtoOASpotEvent,
        _oa.ProtoOASpotEvent,
    ]
    assert len(writer.frames) == 1


def __test_wire_snapshot_is_immutable_and_does_not_change_in_place__() -> None:
    wire = WireClient("example.invalid")
    snapshot = wire.telemetry_snapshot()

    wire._spot_events_with_trendbar += 1

    assert snapshot.spot_events_with_trendbar == 0
    assert wire.telemetry_snapshot().spot_events_with_trendbar == 1
    with pytest.raises(FrozenInstanceError):
        setattr(snapshot, "inbound_heartbeats", 9)


def __test_subscription_outcome_preserves_success_and_already_subscribed__() -> None:
    wire = _SubscribeWire([
        None,
        CTraderProtocolError("ALREADY_SUBSCRIBED"),
    ])
    provider = _provider(wire)

    outcome = asyncio.run(provider._subscribe_live("EURUSD", "1"))

    assert outcome.spots is SubscribeStatus.SUCCESS
    assert outcome.trendbars is SubscribeStatus.ALREADY_SUBSCRIBED
    assert "EURUSD" in provider._subscribed_symbols
    assert provider._live_subscription == ("EURUSD", "1")


def __test_subscription_outcome_is_immutable__() -> None:
    outcome = asyncio.run(
        _provider(_SubscribeWire([None, None]))._subscribe_live("EURUSD", "1")
    )

    with pytest.raises(FrozenInstanceError):
        setattr(outcome, "spots", SubscribeStatus.ALREADY_SUBSCRIBED)


def __test_successful_connect_resets_nonzero_connection_telemetry__(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wire = WireClient("example.invalid")
    wire._inbound_heartbeats = 3
    wire._spot_events_without_trendbar = 4
    wire._spot_events_with_trendbar = 5
    previous_snapshot = wire.telemetry_snapshot()
    writer = _Writer()

    async def open_connection(*args, **kwargs):
        del args, kwargs
        return _IdleReader(), writer

    monkeypatch.setattr("asyncio.open_connection", open_connection)

    async def scenario() -> None:
        await wire.connect()
        assert wire.telemetry_snapshot().inbound_heartbeats == 0
        assert wire.telemetry_snapshot().spot_events_without_trendbar == 0
        assert wire.telemetry_snapshot().spot_events_with_trendbar == 0
        await wire.disconnect()

    asyncio.run(scenario())

    assert previous_snapshot.inbound_heartbeats == 3
    assert previous_snapshot.spot_events_without_trendbar == 4
    assert previous_snapshot.spot_events_with_trendbar == 5


def __test_client_msg_id_correlated_spot_completes_future_without_queueing__() -> None:
    async def scenario() -> None:
        wire = WireClient("example.invalid")
        future = asyncio.get_running_loop().create_future()
        wire._pending["spot-1"] = future
        envelope = _envelope(
            _oa.ProtoOASpotEvent(
                ctidTraderAccountId=999,
                symbolId=1,
                bid=114001,
                trendbar=[
                    _model.ProtoOATrendbar(
                        utcTimestampInMinutes=30_000_000,
                        volume=1,
                    )
                ],
            )
        )
        envelope.clientMsgId = "spot-1"

        wire._route(envelope)

        assert future.done()
        assert isinstance(future.result(), _oa.ProtoOASpotEvent)
        assert wire.events.empty()
        assert wire.telemetry_snapshot().spot_events_with_trendbar == 1

    asyncio.run(scenario())


def __test_unknown_subscription_error_does_not_record_local_success__() -> None:
    provider = _provider(
        _SubscribeWire([CTraderProtocolError("UNKNOWN_SUBSCRIPTION_FAILURE")])
    )

    with pytest.raises(CTraderProtocolError, match="UNKNOWN_SUBSCRIPTION_FAILURE"):
        asyncio.run(provider._subscribe_live("EURUSD", "1"))

    assert "EURUSD" not in provider._subscribed_symbols
    assert provider._live_subscription is None
