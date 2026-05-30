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

#: Default timeout, in seconds, for a request awaiting its correlated response.
REQUEST_TIMEOUT = 30.0


def protobuf_host(demo: bool) -> str:
    """Return the protobuf API host for the demo or live system.

    :param demo: ``True`` for the demo host, ``False`` for live.
    :return: The hostname to connect to.
    """
    return PROTOBUF_DEMO_HOST if demo else PROTOBUF_LIVE_HOST
