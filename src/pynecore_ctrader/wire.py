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
  period of send-inactivity, and inbound heartbeats are echoed.

Reconnection is intentionally *not* handled here: the
:class:`~pynecore.core.plugin.LiveProviderPlugin` base owns that, driving
:meth:`connect`/:meth:`disconnect` and observing :attr:`is_connected`.
"""
import asyncio
import logging
import ssl
import struct

from google.protobuf.message import Message

from . import helpers
from .messages import OpenApiCommonMessages_pb2 as _common
from .messages import OpenApiMessages_pb2 as _oa

logger = logging.getLogger(__name__)


class CTraderWireError(Exception):
    """Base class for wire-level errors."""


class CTraderConnectionError(CTraderWireError):
    """The connection is not established or was lost."""


class CTraderTimeoutError(CTraderWireError):
    """A request did not receive its correlated response in time."""


class CTraderProtocolError(CTraderWireError):
    """The server answered a request with an error response.

    :ivar error_code: The ``errorCode`` string from the error response.
    :ivar description: The optional human-readable description.
    """

    def __init__(self, error_code: str, description: str = "") -> None:
        self.error_code = error_code
        self.description = description
        super().__init__(f"{error_code}: {description}" if description else error_code)


def _build_payload_type_map() -> dict[int, type[Message]]:
    """Build the ``payloadType -> message class`` dispatch table by reflection.

    Mirrors the official client: scan the common and OpenApi message modules for
    ``Proto*`` message classes that carry a ``payloadType`` field and key them by
    that field's default value. The envelope class itself (``ProtoMessage``,
    whose required ``payloadType`` reads as ``0``) is skipped.

    :return: Mapping from a numeric payload type to its concrete message class.
    """
    mapping: dict[int, type[Message]] = {}
    for module in (_common, _oa):
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
_HEARTBEAT_TYPE = _common.ProtoHeartbeatEvent().payloadType


def _raise_on_error(message: Message) -> None:
    """Raise :class:`CTraderProtocolError` if ``message`` is an error response.

    :param message: A decoded response message.
    """
    if isinstance(message, (_common.ProtoErrorRes, _oa.ProtoOAErrorRes)):
        raise CTraderProtocolError(message.errorCode, message.description)


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
        self._send_lock = asyncio.Lock()
        #: Queue of unsolicited inbound messages (events and orphan responses).
        self.events: asyncio.Queue[Message] = asyncio.Queue()

    @property
    def is_connected(self) -> bool:
        """Whether the socket is open and writable."""
        return self._writer is not None and not self._writer.is_closing()

    async def connect(self) -> None:
        """Open the TLS connection and start the receive and heartbeat tasks."""
        ssl_context = ssl.create_default_context()
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port, ssl=ssl_context
        )
        self._last_send = asyncio.get_running_loop().time()
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
            try:
                await writer.wait_closed()
            except (OSError, ssl.SSLError):
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
        :raises CTraderConnectionError: If the connection drops while waiting.
        """
        loop = asyncio.get_running_loop()
        self._msg_id += 1
        client_msg_id = str(self._msg_id)
        future: asyncio.Future[Message] = loop.create_future()
        self._pending[client_msg_id] = future
        try:
            await self._send_message(message, client_msg_id)
            result = await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            raise CTraderTimeoutError(
                f"no response within {timeout}s for {type(message).__name__}"
            ) from None
        finally:
            self._pending.pop(client_msg_id, None)
        _raise_on_error(result)
        return result

    def _frame(self, message: Message, client_msg_id: str) -> bytes:
        """Wrap a concrete message into a length-prefixed ``ProtoMessage`` envelope.

        :param message: The concrete message to wrap.
        :param client_msg_id: The correlation id, or ``""`` for none.
        :return: The bytes to write to the socket.
        """
        payload_type = message.DESCRIPTOR.fields_by_name["payloadType"].default_value
        envelope = _common.ProtoMessage(
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
        :raises CTraderConnectionError: If the socket is not writable, or the write
            fails because the peer dropped.
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
                raise CTraderConnectionError("connection lost while sending") from exc
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
                envelope = _common.ProtoMessage()
                envelope.ParseFromString(body)
                if envelope.payloadType == _HEARTBEAT_TYPE:
                    await self._send_message(_common.ProtoHeartbeatEvent())
                    continue
                self._route(envelope)
        except asyncio.CancelledError:
            raise
        except (asyncio.IncompleteReadError, ConnectionError, OSError, ssl.SSLError) as exc:
            logger.debug("cTrader wire receive loop stopped: %r", exc)
            self._close_writer()
        except Exception:
            logger.warning("cTrader wire receive loop stopped unexpectedly", exc_info=True)
            self._close_writer()
        finally:
            self._fail_pending(CTraderConnectionError("connection lost"))

    def _route(self, envelope: _common.ProtoMessage) -> None:
        """Decode an envelope payload and deliver it to its waiter or the queue.

        :param envelope: A parsed (non-heartbeat) ``ProtoMessage`` envelope.
        """
        klass = _PAYLOAD_TYPE_TO_CLASS.get(envelope.payloadType)
        if klass is None:
            return
        message = klass()
        message.ParseFromString(envelope.payload)
        client_msg_id = envelope.clientMsgId
        if client_msg_id:
            future = self._pending.get(client_msg_id)
            if future is not None and not future.done():
                future.set_result(message)
                return
        self.events.put_nowait(message)

    async def _heartbeat_loop(self) -> None:
        """Send a heartbeat after each period of send-inactivity (ping cadence)."""
        interval = helpers.HEARTBEAT_INTERVAL
        loop = asyncio.get_running_loop()
        try:
            while True:
                await asyncio.sleep(interval)
                if loop.time() - self._last_send >= interval:
                    try:
                        await self._send_message(_common.ProtoHeartbeatEvent())
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
