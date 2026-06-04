"""The exported cTrader plugin class.

Composes the data-provider and broker mix-ins with
:class:`~pynecore.core.plugin.CLIPlugin` so the one class serves
``pyne data download ctrader``, the live order-execution layer (``pyne run
--broker``), and the ``pyne ctrader`` subcommands. Every mix-in derives from
:class:`~pynecore_ctrader._base._CTraderBase` (itself a ``BrokerPlugin``); the
mix-ins come first and ``CLIPlugin`` last, mirroring the TradingView plugin's
layout, since PyneCore checks the provider / broker / CLI capabilities with
separate ``issubclass`` tests so a single class activates all three.
"""
from typing import TYPE_CHECKING

from pynecore.core.plugin import CLIPlugin, override

from .config import CTraderConfig
from .events import _EventStreamMixin
from .execution import _ExecutionMixin
from .provider import _ProviderMixin
from .recovery import _RecoveryMixin
from .state import _StateMixin

if TYPE_CHECKING:
    import typer

__all__ = ['CTrader', 'CTraderConfig']


class CTrader(
    _EventStreamMixin,
    _RecoveryMixin,
    _ExecutionMixin,
    _StateMixin,
    _ProviderMixin,
    CLIPlugin[CTraderConfig],
):
    """cTrader Open API data provider + broker + ``pyne ctrader`` CLI.

    One open-source plugin serving every broker on the cTrader platform
    (Pepperstone, IC Markets, FxPro, ...). It connects with the user's *own*
    cTrader Open API application credentials — there is no shared PyneSys secret
    and PyneSys never relays the trading socket.

    **Supported**

    - ``pyne ctrader auth`` — OAuth2 loopback consent, storing the token
    - ``pyne data download ctrader --list-brokers`` — the user's broker titles
    - ``pyne data download ctrader:<broker> --list-symbols`` — a broker's symbols
    - Historical OHLCV via paged trendbar requests, and live OHLCV from spot
      events
    - Live order execution (``pyne run --broker``): MARKET / LIMIT / STOP
      entries, native position-attribute TP/SL/trailing brackets, atomic
      amends, and the ``ProtoOAExecutionEvent`` PUSH order stream

    **Multi-broker** — the broker segment of the provider string
    (``ctrader:pepperstoneuk:EURUSD@60``) selects the trading account by broker
    slug (``ProtoOATrader.brokerName``); ``account_id`` in the config
    disambiguates multiple accounts at one broker.
    """

    @staticmethod
    @override
    def cli() -> "typer.Typer | None":
        from .cli import ctrader_app
        return ctrader_app
