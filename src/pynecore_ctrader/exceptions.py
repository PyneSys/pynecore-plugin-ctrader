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


def is_not_found(error_code: str) -> bool:
    """Whether ``error_code`` means the target order/position no longer exists.

    Cancel and close paths treat a not-found response as already-gone (benign
    no-op) rather than a failure.

    :param error_code: The cTrader ``errorCode`` string.
    :return: ``True`` if the referenced entity is known not to exist.
    """
    return error_code in _NOT_FOUND_CODES


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
