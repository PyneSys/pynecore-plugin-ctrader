"""Thin asyncio TCP+TLS protobuf client for the cTrader Open API.

A single persistent connection that speaks the cTrader wire protocol:

- Framing: each message is a serialized :class:`ProtoMessage` envelope, prefixed
  with a 4-byte big-endian unsigned length (the Twisted ``Int32StringReceiver``
  format the official client uses), capped at
  :data:`~pynecore_ctrader.helpers.MAX_MESSAGE_LENGTH`.
- Envelope: an outgoing concrete message is wrapped as
  ``ProtoMessage(payloadType=msg.payloadType, payload=msg.SerializeToString(),
  clientMsgId=...)``; an incoming envelope is dispatched on its ``payloadType``.
- Correlation: :meth:`WireClient.send_request` assigns a ``clientMsgId`` and
  awaits the response carrying it; unsolicited messages (spot/execution events,
  and any response with no matching pending request) go to :attr:`WireClient.events`.
- Keepalive: an idle-aware heartbeat task sends a ``ProtoHeartbeatEvent`` after a
  period of send-inactivity, and inbound heartbeats are echoed. The same task
  watches inbound inactivity: the server heartbeats an idle connection, so a
  stretch of total inbound silence past
  :data:`~pynecore_ctrader.helpers.INBOUND_IDLE_TIMEOUT` means a dead (often
  half-open) socket and the connection is closed so the owner can reconnect.

Reconnection is intentionally *not* handled here: the
:class:`~pynecore.core.plugin.LiveProviderPlugin` base owns that, driving
:meth:`connect`/:meth:`disconnect` and observing :attr:`is_connected`.
"""
import asyncio
import logging
import ssl
import struct
from dataclasses import dataclass

from google.protobuf.message import DecodeError, Message

from pynecore.core.plugin import ProviderError

from . import helpers
from .messages import OpenApiCommonMessages_pb2 as OpenApiCommonMessages
from .messages import OpenApiMessages_pb2 as OpenApiMessages

logger = logging.getLogger(__name__)


class CTraderWireError(ProviderError):
    """Base class for wire-level errors.

    Subclasses :class:`~pynecore.core.plugin.ProviderError` so the ``pyne data``
    CLI reports connection/auth/protocol failures as a clean one-line error
    rather than a traceback; on the live path the same errors still propagate to
    the :class:`LiveProviderPlugin` reconnect logic untouched.
    """


#: cTrader ``errorCode`` strings (the enum *names* the API returns) that mean a
#: server-side connectivity / availability fault rather than a permanent,
#: user-actionable error. A request that fails with one of these may well
#: succeed once the broker's routing or maintenance window recovers, so the
#: long-running ``--broker`` / ``--live`` startup waits and retries instead of
#: halting. Kept deliberately narrow — auth, symbol and business-rule codes are
#: NOT here, so they keep failing fast. ``CANT_ROUTE_REQUEST`` is the code
#: Pepperstone returns while a broker backend is in maintenance.
_CONNECTION_CLASS_CODES = frozenset({
    'CANT_ROUTE_REQUEST',  # common: "Connection to Server is lost"
    'TIMEOUT_ERROR',  # common: server-side execution timeout
    'SERVER_IS_UNDER_MAINTENANCE',
    'CH_SERVER_NOT_REACHABLE',  # "Trading service is not available"
})


class CTraderConnectionError(CTraderWireError):
    """The connection is not established or was lost."""

    retryable: bool = True


class CTraderRequestSentConnectionError(CTraderConnectionError):
    """The connection dropped after a request was (or may have been) written.

    Distinct from a plain pre-write :class:`CTraderConnectionError`: the request
    bytes were already handed to the socket (a successful ``drain`` followed by a
    lost pending future, or a ``drain`` that failed after ``write`` queued the
    bytes), so the server may have accepted the order. The order path maps this
    to a disposition-unknown rather than a clean "nothing happened" reconnect, so
    a retry cannot blindly duplicate an entry / close the server already took.
    Subclasses :class:`CTraderConnectionError` so every data-path
    ``except CTraderConnectionError`` site still treats it as a dropped link.
    """


class CTraderTimeoutError(CTraderWireError):
    """A request did not receive its correlated response in time."""

    retryable: bool = True


class CTraderProtocolError(CTraderWireError):
    """The server answered a request with an error response.

    :ivar error_code: The ``errorCode`` string from the error response.
    :ivar description: The optional human-readable description.
    :ivar retry_after: Venue-provided seconds until a blocked payload unlocks.
    """

    def __init__(
        self,
        error_code: str,
        description: str = "",
        retry_after: float | None = None,
    ) -> None:
        self.error_code = error_code
        self.description = description
        self.retry_after = retry_after
        super().__init__(f"{error_code}: {description}" if description else error_code)

    @property
    def retryable(self) -> bool:
        """Whether this error is a transient server-side connectivity fault.

        ``True`` only for the connectivity / maintenance codes in
        :data:`_CONNECTION_CLASS_CODES`; auth, symbol and business-rule
        rejections stay permanent so they fail fast.
        """
        return self.error_code in _CONNECTION_CLASS_CODES


def _build_payload_type_map() -> dict[int, type[Message]]:
    """Build the ``payloadType -> message class`` dispatch table by reflection.

    Mirrors the official client: scan the common and OpenApi message modules for
    ``Proto*`` message classes that carry a ``payloadType`` field and key them by
    that field's default value. The envelope class itself (``ProtoMessage``,
    whose required ``payloadType`` reads as ``0``) is skipped.

    :return: Mapping from a numeric payload type to its concrete message class.
    """
    mapping: dict[int, type[Message]] = {}
    for module in (OpenApiCommonMessages, OpenApiMessages):
        for name in dir(module):
            if not name.startswith("Proto"):
                continue
            klass = getattr(module, name)
            if not (isinstance(klass, type) and issubclass(klass, Message)):
                continue
            field = klass.DESCRIPTOR.fields_by_name.get("payloadType")
            if field is None:
                continue
            payload_type = field.default_value
            if payload_type:
                mapping[payload_type] = klass
    return mapping


#: ``payloadType -> message class`` dispatch table, built once at import time.
_PAYLOAD_TYPE_TO_CLASS = _build_payload_type_map()

#: The payload type of a heartbeat event, special-cased on receive.
_HEARTBEAT_TYPE = OpenApiCommonMessages.ProtoHeartbeatEvent().payloadType


@dataclass(frozen=True, slots=True)
class WireTelemetrySnapshot:
    """Immutable connection-local counts of selected inbound wire events."""

    inbound_heartbeats: int = 0
    spot_events_without_trendbar: int = 0
    spot_events_with_trendbar: int = 0


def _raise_on_error(message: Message) -> None:
    """Raise :class:`CTraderProtocolError` if ``message`` is an error response.

    :param message: A decoded response message.
    """
    if isinstance(message, (OpenApiCommonMessages.ProtoErrorRes, OpenApiMessages.ProtoOAErrorRes)):
        retry_after = None
        if (
            isinstance(message, OpenApiMessages.ProtoOAErrorRes)
            and message.HasField("retryAfter")
        ):
            retry_after = float(message.retryAfter)
        raise CTraderProtocolError(
            message.errorCode,
            message.description,
            retry_after=retry_after,
        )


class WireClient:
    """A single persistent TLS connection to a cTrader Open API host.

    :param host: The protobuf API host (see
        :func:`~pynecore_ctrader.helpers.protobuf_host`).
    :param port: The TCP port; defaults to
        :data:`~pynecore_ctrader.helpers.PROTOBUF_PORT`.
    """

    def __init__(self, host: str, port: int = helpers.PROTOBUF_PORT) -> None:
        self._host = host
        self._port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._recv_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Future[Message]] = {}
        self._msg_id = 0
        self._last_send = 0.0
        self._last_recv = 0.0
        self._send_lock = asyncio.Lock()
        self._inbound_heartbeats = 0
        self._spot_events_without_trendbar = 0
        self._spot_events_with_trendbar = 0
        #: Queue of unsolicited inbound messages (events and orphan responses).
        self.events: asyncio.Queue[Message] = asyncio.Queue()

    @property
    def is_connected(self) -> bool:
        """Whether the socket is open and writable."""
        return self._writer is not None and not self._writer.is_closing()

    def telemetry_snapshot(self) -> WireTelemetrySnapshot:
        """Return an immutable snapshot of this connection's inbound counters."""
        return WireTelemetrySnapshot(
            inbound_heartbeats=self._inbound_heartbeats,
            spot_events_without_trendbar=self._spot_events_without_trendbar,
            spot_events_with_trendbar=self._spot_events_with_trendbar,
        )

    def _reset_telemetry(self) -> None:
        """Reset counters when a new socket becomes the active connection."""
        self._inbound_heartbeats = 0
        self._spot_events_without_trendbar = 0
        self._spot_events_with_trendbar = 0

    async def connect(self) -> None:
        """Open the TLS connection and start the receive and heartbeat tasks.

        The connect is bounded by :data:`~pynecore_ctrader.helpers.CONNECT_TIMEOUT`:
        ``asyncio.open_connection`` waits without limit, so a socket that accepts
        the TCP handshake but never finishes TLS (a broker edge in maintenance, a
        half-open network path) would otherwise stall the one-shot startup bridge
        forever. On expiry the in-flight connect is cancelled and surfaced as a
        retryable :class:`CTraderConnectionError`, so the ``--broker`` / ``--live``
        startup rides it out via its backoff-retry loop instead of hanging.

        :raises CTraderConnectionError: If the connection cannot be established
            within :data:`~pynecore_ctrader.helpers.CONNECT_TIMEOUT`.
        """
        ssl_context = ssl.create_default_context()
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port, ssl=ssl_context),
                timeout=helpers.CONNECT_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise CTraderConnectionError(
                f"connection to {self._host}:{self._port} not established "
                f"within {helpers.CONNECT_TIMEOUT}s"
            ) from None
        except OSError as exc:
            # A transient socket/TLS fault during the handshake — most notably
            # ``ConnectionResetError`` ([Errno 54]) raised straight out of
            # ``asyncio.open_connection`` when the broker edge drops the peer
            # mid-TLS. Surface it as a retryable ``CTraderConnectionError`` so
            # the ``--broker`` / ``--live`` startup rides it out on its
            # backoff-retry loop instead of exiting with a raw traceback.
            raise CTraderConnectionError(
                f"connection to {self._host}:{self._port} failed: {exc}"
            ) from exc
        self._reset_telemetry()
        now = asyncio.get_running_loop().time()
        self._last_send = now
        self._last_recv = now
        self._recv_task = asyncio.create_task(self._recv_loop(), name="ctrader-wire-recv")
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name="ctrader-wire-heartbeat"
        )

    async def disconnect(self) -> None:
        """Stop the background tasks, close the socket and fail pending requests."""
        for task in (self._heartbeat_task, self._recv_task):
            if task is not None:
                task.cancel()
        for task in (self._heartbeat_task, self._recv_task):
            if task is not None:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._heartbeat_task = None
        self._recv_task = None
        writer = self._writer
        self._close_writer()
        if writer is not None:
            # ``wait_closed`` drains the TLS close-notify exchange, which cannot
            # complete against an unresponsive / half-open peer. Bound it so a
            # dead socket never wedges teardown — critically, this teardown runs
            # in the cancellation ``finally`` of the one-shot bridge, so an
            # unbounded drain there swallows a Ctrl-C-driven cancel until an
            # external SIGKILL. On expiry the socket is already ``close()``d, so
            # abandoning the drain leaks nothing the OS won't reclaim.
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=helpers.CLOSE_TIMEOUT)
            except (OSError, ssl.SSLError, asyncio.TimeoutError):
                pass
        self._writer = None
        self._reader = None
        self._fail_pending(CTraderConnectionError("connection closed"))

    async def send(self, message: Message) -> None:
        """Send a message without awaiting a response (fire-and-forget).

        :param message: A concrete protobuf message to wrap and send.
        """
        await self._send_message(message)

    async def send_request(
            self, message: Message, *, timeout: float = helpers.REQUEST_TIMEOUT
    ) -> Message:
        """Send a request and await its correlated response.

        :param message: A concrete request message to wrap and send.
        :param timeout: Seconds to wait for the response.
        :return: The decoded response message.
        :raises CTraderTimeoutError: If no response arrives within ``timeout``.
        :raises CTraderProtocolError: If the server answers with an error response.
        :raises CTraderConnectionError: If the connection is down before the
            request was written.
        :raises CTraderRequestSentConnectionError: If the connection drops after
            the request bytes were (or may have been) written — disposition
            unknown.
        """
        loop = asyncio.get_running_loop()
        self._msg_id += 1
        client_msg_id = str(self._msg_id)
        future: asyncio.Future[Message] = loop.create_future()
        self._pending[client_msg_id] = future
        try:
            await self._send_message(message, client_msg_id)
            # The request bytes are now on the wire: a connection loss while we
            # await the correlated response leaves the disposition unknown (the
            # server may have accepted the order), so re-raise the pending
            # future's plain ``CTraderConnectionError`` as the request-sent
            # variant. A response that never arrives within ``timeout`` is the
            # ``CTraderTimeoutError`` path below, which the order layer already
            # treats as disposition-unknown.
            try:
                result = await asyncio.wait_for(future, timeout)
            except CTraderConnectionError as exc:
                raise CTraderRequestSentConnectionError(str(exc)) from exc
        except asyncio.TimeoutError:
            raise CTraderTimeoutError(
                f"no response within {timeout}s for {type(message).__name__}"
            ) from None
        finally:
            self._pending.pop(client_msg_id, None)
        try:
            _raise_on_error(result)
        except CTraderProtocolError as exc:
            if exc.error_code == "BLOCKED_PAYLOAD_TYPE":
                logger.warning(
                    "cTrader request %s was rate-limited",
                    type(message).__name__,
                )
            raise
        return result

    @staticmethod
    def _frame(message: Message, client_msg_id: str) -> bytes:
        """Wrap a concrete message into a length-prefixed ``ProtoMessage`` envelope.

        :param message: The concrete message to wrap.
        :param client_msg_id: The correlation id, or ``""`` for none.
        :return: The bytes to write to the socket.
        """
        payload_type = message.DESCRIPTOR.fields_by_name["payloadType"].default_value
        envelope = OpenApiCommonMessages.ProtoMessage(
            payloadType=payload_type, payload=message.SerializeToString()
        )
        if client_msg_id:
            envelope.clientMsgId = client_msg_id
        body = envelope.SerializeToString()
        return struct.pack("!I", len(body)) + body

    async def _send_message(self, message: Message, client_msg_id: str = "") -> None:
        """Frame and write a message under the send lock.

        :param message: The concrete message to send.
        :param client_msg_id: The correlation id, or ``""`` for none.
        :raises CTraderConnectionError: If the socket is not writable (nothing was
            sent).
        :raises CTraderRequestSentConnectionError: If the write was queued but the
            drain failed — the bytes may already be on the wire.
        """
        writer = self._writer
        if writer is None or writer.is_closing():
            raise CTraderConnectionError("not connected")
        data = self._frame(message, client_msg_id)
        async with self._send_lock:
            try:
                writer.write(data)
                await writer.drain()
            except (OSError, ssl.SSLError) as exc:
                self._close_writer()
                # ``write`` already queued the bytes into the transport buffer;
                # a failed ``drain`` cannot prove they never reached the peer, so
                # this is a disposition-unknown send, not a clean no-op.
                raise CTraderRequestSentConnectionError(
                    "connection lost while sending"
                ) from exc
            self._last_send = asyncio.get_running_loop().time()

    async def _recv_loop(self) -> None:
        """Read framed envelopes, echo heartbeats and route everything else.

        Any read/parse failure (or an oversized frame) tears the connection down:
        the socket is closed so :attr:`is_connected` flips to ``False`` and the
        owning provider can reconnect, and pending requests are always failed.
        """
        reader = self._reader
        assert reader is not None
        try:
            while True:
                header = await reader.readexactly(4)
                (length,) = struct.unpack("!I", header)
                if length > helpers.MAX_MESSAGE_LENGTH:
                    raise CTraderProtocolError(
                        "LENGTH_EXCEEDED",
                        f"inbound frame {length} > {helpers.MAX_MESSAGE_LENGTH}",
                    )
                body = await reader.readexactly(length)
                self._last_recv = asyncio.get_running_loop().time()
                envelope = OpenApiCommonMessages.ProtoMessage()
                envelope.ParseFromString(body)
                if envelope.payloadType == _HEARTBEAT_TYPE:
                    self._inbound_heartbeats += 1
                    await self._send_message(OpenApiCommonMessages.ProtoHeartbeatEvent())
                    continue
                self._route(envelope)
        except asyncio.CancelledError:
            raise
        except (asyncio.IncompleteReadError, ConnectionError, OSError, ssl.SSLError) as exc:
            logger.debug("cTrader wire receive loop stopped: %r", exc)
            self._close_writer()
        except (DecodeError, KeyError, struct.error, CTraderProtocolError) as exc:
            logger.warning("cTrader wire receive loop rejected an invalid frame: %s", exc)
            self._close_writer()
        finally:
            self._fail_pending(CTraderConnectionError("connection lost"))

    def _route(self, envelope: OpenApiCommonMessages.ProtoMessage) -> None:
        """Decode an envelope payload and deliver it to its waiter or the queue.

        :param envelope: A parsed (non-heartbeat) ``ProtoMessage`` envelope.
        """
        klass = _PAYLOAD_TYPE_TO_CLASS.get(envelope.payloadType)
        if klass is None:
            return
        message = klass()
        message.ParseFromString(envelope.payload)
        if isinstance(message, OpenApiMessages.ProtoOASpotEvent):
            if message.trendbar:
                self._spot_events_with_trendbar += 1
            else:
                self._spot_events_without_trendbar += 1
        client_msg_id = envelope.clientMsgId
        if client_msg_id:
            future = self._pending.get(client_msg_id)
            if future is not None and not future.done():
                future.set_result(message)
                return
        self.events.put_nowait(message)

    async def _heartbeat_loop(self) -> None:
        """Send a heartbeat after each period of send-inactivity (ping cadence).

        Doubles as the inbound-inactivity watchdog: the server heartbeats an
        otherwise idle connection, so total inbound silence past
        :data:`~pynecore_ctrader.helpers.INBOUND_IDLE_TIMEOUT` means the
        connection is dead even when the socket still reports writable — the
        half-open-TCP case (router restart, NAT timeout) where sends keep
        "succeeding" into the void. The socket is closed so
        :attr:`is_connected` flips to ``False`` and the owning provider's
        reconnect machinery takes over.
        """
        interval = helpers.HEARTBEAT_INTERVAL
        loop = asyncio.get_running_loop()
        try:
            while True:
                await asyncio.sleep(interval)
                inbound_idle = loop.time() - self._last_recv
                if inbound_idle >= helpers.INBOUND_IDLE_TIMEOUT:
                    logger.warning(
                        "cTrader wire: no inbound traffic for %.0fs; "
                        "closing dead connection", inbound_idle,
                    )
                    self._close_writer()
                    return
                if loop.time() - self._last_send >= interval:
                    try:
                        await self._send_message(OpenApiCommonMessages.ProtoHeartbeatEvent())
                    except CTraderConnectionError:
                        return
        except asyncio.CancelledError:
            raise

    def _close_writer(self) -> None:
        """Close the socket if open, flipping :attr:`is_connected` to ``False``."""
        if self._writer is not None and not self._writer.is_closing():
            self._writer.close()

    def _fail_pending(self, exc: Exception) -> None:
        """Resolve all pending request futures with ``exc``.

        :param exc: The exception to set on each outstanding future.
        """
        pending = self._pending
        self._pending = {}
        for future in pending.values():
            if not future.done():
                future.set_exception(exc)
