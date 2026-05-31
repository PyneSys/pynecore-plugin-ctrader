"""The exported cTrader plugin class.

Composes the data-provider mix-in with :class:`~pynecore.core.plugin.CLIPlugin`
so the one class both serves ``pyne data download ctrader`` and registers the
``pyne ctrader`` subcommands. The provider/broker base goes first and
``CLIPlugin`` last, mirroring the TradingView plugin's layout; PyneCore checks
the provider and CLI capabilities with separate ``issubclass`` tests, so a
single class activates both.
"""
from typing import TYPE_CHECKING

from pynecore.core.plugin import CLIPlugin, override

from .config import CTraderConfig
from .provider import _ProviderMixin

if TYPE_CHECKING:
    import typer

__all__ = ['CTrader', 'CTraderConfig']


class CTrader(_ProviderMixin, CLIPlugin[CTraderConfig]):
    """cTrader Open API data provider + ``pyne ctrader`` CLI.

    One open-source plugin serving every broker on the cTrader platform
    (Pepperstone, IC Markets, FxPro, ...). It connects with the user's *own*
    cTrader Open API application credentials — there is no shared PyneSys secret
    and PyneSys never relays the trading socket.

    **Supported (M1)**

    - ``pyne ctrader auth`` — OAuth2 loopback consent, storing the token
    - ``pyne data download ctrader --list-brokers`` — the user's broker titles
    - ``pyne data download ctrader:<broker> --list-symbols`` — a broker's symbols
    - Historical OHLCV via paged trendbar requests, and live OHLCV from spot
      events

    **Multi-broker** — the broker segment of the provider string
    (``ctrader:Pepperstone:EURUSD@60``) selects the trading account by broker
    title; ``account_id`` in the config disambiguates multiple accounts at one
    broker.
    """

    @staticmethod
    @override
    def cli() -> "typer.Typer | None":
        from .cli import ctrader_app
        return ctrader_app
