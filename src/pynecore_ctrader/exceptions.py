"""cTrader broker error taxonomy mapping.

Translates the cTrader Open API ``errorCode`` strings — carried on
:class:`~pynecore_ctrader.wire.CTraderProtocolError` (request errors),
``ProtoOAErrorRes`` and ``ProtoOAOrderErrorEvent`` — into the PyneCore broker
exception taxonomy so the sync engine and risk layer can pattern-match on type
instead of parsing message strings.

Per project policy the raw cTrader code is NEVER copied into a user-facing
message; it is logged for diagnostics only and the user sees a clean,
own-worded reason.
"""
import logging

from pynecore.core.broker.exceptions import (
    BrokerError,
    ExchangeOrderRejectedError,
    ExchangeRateLimitError,
    InsufficientMarginError,
)
from pynecore.core.plugin import ProviderError

from .wire import CTraderProtocolError

logger = logging.getLogger(__name__)


class CTraderBrokerError(ProviderError):
    """Base for cTrader broker-side failures not covered by the core taxonomy.

    Subclasses :class:`~pynecore.core.plugin.ProviderError` so a stray broker
    failure on the data CLI still surfaces as a clean one-line error rather
    than a traceback.
    """


#: ``errorCode`` strings meaning the referenced order/position is already gone.
#: A cancel/close against it is a benign no-op, never an error.
_NOT_FOUND_CODES = frozenset({
    'POSITION_NOT_FOUND',
    'POSITION_NOT_FOUND_OR_NOT_OPENED',
    'ORDER_NOT_FOUND',
    'ALREADY_CLOSED',
})

#: ``errorCode`` strings meaning a margin / funds / trading-permission
#: rejection. At entry the engine downgrades these to a skip so the bot keeps
#: running; surfacing them as the typed margin subclass aids that routing.
_MARGIN_CODES = frozenset({
    'NOT_ENOUGH_MONEY',
    'NO_ENOUGH_MONEY_TO_OPEN_POSITION',
    'TRADING_BAD_VOLUME',
    'TRADING_DISABLED',
})

#: ``errorCode`` strings meaning the client exceeded a request-rate budget.
#: Recoverable via backoff — never a halt.
_RATE_LIMIT_CODES = frozenset({
    'REQUEST_FREQUENCY_EXCEEDED',
})

#: ``errorCode`` strings meaning this connection's account session was lost
#: mid-session while the socket stayed up — another connection claimed the
#: account, a server-side session recycle, or the access token went invalid.
#: The recovery is a re-send of ``ProtoOAAccountAuthReq`` on the live wire (the
#: account is re-won), NOT an order reject — so these are detected BEFORE the
#: generic reject mapping. The socket is still writable, so ``is_connected``
#: stays True and the transport-level reconnect logic never fires on its own.
_ACCOUNT_AUTH_LOST_CODES = frozenset({
    'ACCOUNT_NOT_AUTHORIZED',
    'CH_CLIENT_NOT_AUTHENTICATED',
    'CH_CLIENT_AUTH_FAILURE',
})

#: Subset of the auth-lost condition meaning the access token itself is no
#: longer valid: recovery must refresh the token (``ProtoOARefreshTokenReq``)
#: before re-authorizing. The other auth-lost codes are re-won with the
#: still-valid token, so a needless refresh of a good token is avoided.
_TOKEN_INVALID_CODES = frozenset({
    'OA_AUTH_TOKEN_EXPIRED',
    'CH_ACCESS_TOKEN_INVALID',
})

#: Subset of the auth-lost condition meaning the APPLICATION (client) channel
#: auth is gone, not just the account session: ``CH_CLIENT_NOT_AUTHENTICATED``
#: ("a command was sent for a not-authorized client") and
#: ``CH_CLIENT_AUTH_FAILURE`` ("client not activated or wrong credentials"). An
#: account CANNOT be authorized on a de-authenticated client, so recovery must
#: re-send ``ProtoOAApplicationAuthReq`` before the account auth. If it is a
#: genuine bad-credentials case the re-app-auth keeps failing and the loss
#: surfaces as a recoverable connection error (the reconnect path caps retries),
#: never a silent account-reauth loop that can never succeed.
_CLIENT_AUTH_LOST_CODES = frozenset({
    'CH_CLIENT_NOT_AUTHENTICATED',
    'CH_CLIENT_AUTH_FAILURE',
})


def is_account_auth_lost(error_code: str, description: str = "") -> bool:
    """Whether an error means this channel's account authorization was lost.

    cTrader reports the loss two ways: a typed ``errorCode`` (e.g.
    ``ACCOUNT_NOT_AUTHORIZED``) or — observed in practice — the generic
    ``INVALID_REQUEST`` whose human ``description`` carries "not authorized".
    Both are matched; the generic case is gated on the description so an
    unrelated ``INVALID_REQUEST`` (e.g. a malformed field) is never mistaken
    for a session loss.

    :param error_code: The cTrader ``errorCode`` string.
    :param description: The optional human-readable description.
    :return: ``True`` if the account session must be re-authorized.
    """
    if error_code in _ACCOUNT_AUTH_LOST_CODES or error_code in _TOKEN_INVALID_CODES:
        return True
    return error_code == 'INVALID_REQUEST' and 'not authorized' in description.lower()


def is_token_invalid(error_code: str) -> bool:
    """Whether an auth-lost error means the access token must be refreshed.

    These warrant a ``ProtoOARefreshTokenReq`` before re-authorizing; the other
    auth-lost codes are re-won by re-sending ``ProtoOAAccountAuthReq`` with the
    current (still-valid) token.

    :param error_code: The cTrader ``errorCode`` string.
    :return: ``True`` if the access token is invalid / expired.
    """
    return error_code in _TOKEN_INVALID_CODES


def is_client_auth_lost(error_code: str) -> bool:
    """Whether an auth-lost error is an APPLICATION (client) channel loss.

    These require re-sending ``ProtoOAApplicationAuthReq`` (re-app-auth) before
    the account auth — an account cannot be authorized on a de-authenticated
    client channel.

    :param error_code: The cTrader ``errorCode`` string.
    :return: ``True`` if the application channel must be re-authenticated.
    """
    return error_code in _CLIENT_AUTH_LOST_CODES


def is_not_found(error_code: str) -> bool:
    """Whether ``error_code`` means the target order/position no longer exists.

    Cancel and close paths treat a not-found response as already-gone (benign
    no-op) rather than a failure.

    :param error_code: The cTrader ``errorCode`` string.
    :return: ``True`` if the referenced entity is known not to exist.
    """
    return error_code in _NOT_FOUND_CODES


def is_rate_limited(error_code: str) -> bool:
    """Whether ``error_code`` means the client exceeded a request-rate budget.

    Such a rejection says nothing about the request itself, so the caller may
    repeat it after a backoff.

    :param error_code: The cTrader ``errorCode`` string.
    :return: ``True`` if the request was throttled.
    """
    return error_code in _RATE_LIMIT_CODES


def map_error_code(error_code: str, description: str = "") -> BrokerError:
    """Translate a cTrader ``errorCode`` string to the broker taxonomy.

    The raw code is logged for diagnostics; the returned exception carries an
    own-worded message with no code mimicry (the broker's human ``description``
    is appended for debugging, which is text, not a code).

    :param error_code: The cTrader ``errorCode`` string.
    :param description: The optional human-readable description.
    :return: A :class:`BrokerError` subclass the execute path can raise.
    """
    logger.debug("cTrader order rejected: code=%s description=%s",
                 error_code, description)
    if error_code in _RATE_LIMIT_CODES:
        # Conservative fixed backoff — cTrader does not return a retry-after.
        return ExchangeRateLimitError(
            "cTrader request rate limit exceeded; backing off", retry_after=1.0,
        )
    if error_code in _MARGIN_CODES:
        return InsufficientMarginError(
            "cTrader declined the order: insufficient margin or trading not "
            "permitted for this size",
        )
    return ExchangeOrderRejectedError(
        "cTrader rejected the order" + (f": {description}" if description else ""),
    )


def map_protocol_error(exc: CTraderProtocolError) -> BrokerError:
    """Translate a wire-level :class:`CTraderProtocolError` to broker taxonomy.

    :param exc: The protocol error raised by the wire layer.
    :return: A :class:`BrokerError` subclass the execute path can raise.
    """
    return map_error_code(exc.error_code, exc.description)
