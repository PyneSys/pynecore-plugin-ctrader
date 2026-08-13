"""Protocol constants and small helpers for the cTrader Open API plugin.

These are deliberately module-level constants rather than
:class:`~pynecore_ctrader.config.CTraderConfig` fields: they are protocol
invariants and internal tuning knobs with no user-facing reason to be touched,
and exposing them as config would bloat the user TOML with values the user does
not understand.

Hosts, ports and the OAuth endpoints are taken from the official Spotware
``OpenApiPy`` reference (``ctrader_open_api/endpoints.py``); the wire-level
constants mirror its ``TcpProtocol`` (``Int32StringReceiver``).
"""
from typing import cast

#: Protobuf API hosts. Demo and live are fully separate systems.
PROTOBUF_DEMO_HOST = "demo.ctraderapi.com"
PROTOBUF_LIVE_HOST = "live.ctraderapi.com"

#: TCP port for the protobuf API (always TLS).
PROTOBUF_PORT = 5035

#: OAuth2 endpoints. The same host serves both the demo and live systems.
AUTH_URI = "https://openapi.ctrader.com/apps/auth"
TOKEN_URI = "https://openapi.ctrader.com/apps/token"

#: Default OAuth scope (the reference library default; covers data and trading).
DEFAULT_SCOPE = "trading"

#: Hard cap on a single framed message, matching the official client's
#: ``TcpProtocol.MAX_LENGTH``. Inbound frames above this close the connection.
MAX_MESSAGE_LENGTH = 15_000_000

#: Send a ``ProtoHeartbeatEvent`` after this many seconds of send-inactivity. The
#: server tolerates >30 s of silence; the official client uses 20 s, we stay
#: further under the limit.
HEARTBEAT_INTERVAL = 10.0

#: Declare the connection dead after this many seconds without ANY inbound
#: traffic. The proto comments designate the server's ``ProtoHeartbeatEvent``
#: pushes as the connection-health criterion; their cadence is undocumented
#: (~25-30 s observed on an otherwise idle connection), so 90 s — roughly
#: three missed server heartbeats — flags a half-open socket (router / NAT
#: drop) that plain TCP writability checks never notice.
INBOUND_IDLE_TIMEOUT = 90.0

#: Default timeout, in seconds, for a request awaiting its correlated response.
REQUEST_TIMEOUT = 30.0

#: Bound on opening the TCP+TLS connection. ``asyncio.open_connection`` has no
#: intrinsic timeout, so a socket that accepts the TCP handshake but never
#: completes TLS (broker edge in maintenance, a half-open path) would wedge the
#: one-shot startup bridge (``_authed_session`` under ``_run``) forever — the
#: exact stall behind "Fetching symbol info..." hanging with no bound. On
#: expiry the connect is cancelled and surfaced as a retryable
#: :class:`~pynecore_ctrader.wire.CTraderConnectionError`.
CONNECT_TIMEOUT = 30.0

#: Bound on draining a socket close (``StreamWriter.wait_closed``). The TLS
#: close-notify exchange cannot complete against an unresponsive / half-open
#: peer, so an unbounded ``wait_closed`` in teardown wedges indefinitely —
#: crucially inside the cancellation ``finally`` of the one-shot bridge, which
#: swallows a Ctrl-C-driven cancel until an external SIGKILL. Teardown must
#: never block on a dead peer, so the drain is abandoned on expiry.
CLOSE_TIMEOUT = 5.0


def protobuf_host(demo: bool) -> str:
    """Return the protobuf API host for the demo or live system.

    :param demo: ``True`` for the demo host, ``False`` for live.
    :return: The hostname to connect to.
    """
    return PROTOBUF_DEMO_HOST if demo else PROTOBUF_LIVE_HOST


def parse_protocol_id(value: object, *, field: str) -> int:
    """Parse one persisted protobuf identifier without coercive truncation.

    Broker-store ``extras`` are JSON-shaped and therefore expose values as
    ``object``/``Any`` to callers. Protocol identifiers are nevertheless exact,
    positive integers: accepting ``True`` as ``1``, truncating a float, or treating
    ``None`` as zero could target the wrong live order or position. Accept only an
    actual ``int`` (excluding ``bool``) or an ASCII decimal string and reject every
    other representation.

    :param value: Persisted identifier representation.
    :param field: Field name included in validation errors.
    :return: The positive protocol identifier.
    :raises ValueError: If the value is absent, boolean, non-integral, malformed,
        or non-positive.
    """
    if type(value) is int:
        parsed = cast(int, value)
    elif isinstance(value, str) and value.isascii() and value.isdecimal():
        parsed = int(value)
    else:
        raise ValueError(f"{field} must be a positive integer")
    if parsed <= 0:
        raise ValueError(f"{field} must be positive, got {parsed}")
    return parsed


# === Unit conversions (broker order layer) ===============================
#
# cTrader expresses order ``volume`` as an INT64 in 1/100 of a unit
# (centi-units): a protocol ``volume`` of 1000 means 10.00 units. Order
# prices (limit / stop / SL / TP) are absolute ``double`` values rounded to
# the symbol's ``digits``, NOT the 1/100000 integer scaling the trendbar /
# spot decode uses. Money amounts (balance, commission, gross profit) are
# INT64 paired with a per-record ``moneyDigits`` exponent.

#: Protocol ``volume`` unit: 100 centi-units == 1.00 traded unit.
VOLUME_SCALE = 100


def raw_volume(units: float) -> int:
    """Convert a Pine unit quantity to un-stepped cTrader centi-units.

    The min/max acceptance test must run against the *requested* size, before
    snapping to ``stepVolume`` — otherwise a below-min size can round up to the
    minimum (and an above-max size round down to the maximum), so an out-of-range
    order is sent at the boundary instead of being skipped.

    :param units: The Pine-level quantity in traded units (contracts).
    :return: The requested centi-unit ``volume`` rounded to the nearest INT64.
    """
    return int(round(units * VOLUME_SCALE))


def quantize_volume(units: float, step_volume: int) -> int:
    """Snap a Pine unit quantity to a cTrader centi-unit ``volume``.

    Converts ``units`` to centi-units and rounds to the nearest multiple of
    ``step_volume``. Min/max acceptance is the caller's concern: the execute
    path compares the requested raw centi-units (see :func:`raw_volume`) against
    ``minVolume`` / ``maxVolume`` and raises
    :class:`~pynecore.core.broker.exceptions.OrderSkippedByPlugin` rather than
    silently clamping (a clamp would diverge the executed size from the
    strategy's intent and corrupt downstream sizing).

    :param units: The Pine-level quantity in traded units (contracts).
    :param step_volume: The symbol's ``stepVolume`` in centi-units.
    :return: The raw INT64 ``volume`` the order messages expect.
    """
    raw = units * VOLUME_SCALE
    if step_volume > 0:
        quantized = int(round(raw / step_volume) * step_volume)
        if quantized == 0 and raw > 0:
            # A nonzero accepted request must never emit a zero-volume
            # order. Reachable only on a venue quoting
            # ``minVolume < stepVolume / 2`` (the min/max gate runs on the
            # raw centi-units BEFORE quantization) — snap up to the
            # smallest venue-valid multiple instead.
            return step_volume
        return quantized
    return int(round(raw))


def volume_to_units(volume: int) -> float:
    """Convert a cTrader centi-unit ``volume`` back to Pine traded units.

    :param volume: The protocol ``volume`` in centi-units.
    :return: The quantity in traded units.
    """
    return volume / VOLUME_SCALE


def round_price(price: float, digits: int) -> float:
    """Round an absolute order price to the symbol's ``digits`` precision.

    :param price: The absolute price (limit / stop / SL / TP).
    :param digits: The symbol's price precision (``ProtoOASymbol.digits``).
    :return: The rounded price the order messages expect.
    """
    return round(price, digits)


def money_value(raw: int, money_digits: int) -> float:
    """Convert a cTrader INT64 money amount to its real value.

    :param raw: The raw INT64 amount (balance, commission, gross profit).
    :param money_digits: The paired ``moneyDigits`` exponent for the record.
    :return: ``raw / 10**money_digits``.
    """
    return raw / (10 ** money_digits)
