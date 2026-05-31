"""Configuration dataclass for the cTrader Open API plugin.

The user-facing OAuth application credentials (``client_id`` / ``client_secret``)
serve both the data-ingest and the order-execution side. The minted token pair is
*not* here: it is machine-generated auth state, persisted to the workdir cache via
:mod:`pynecore_ctrader.session` so the program never rewrites this user-edited
file. Internal tuning knobs (heartbeat cadence, reconnect timing, request
timeouts) live as module-level constants in :mod:`pynecore_ctrader.helpers`,
deliberately NOT in this dataclass: they have no user-facing reason to be touched,
and exposing them as config fields balloons the user TOML with knobs the user does
not understand.
"""
from dataclasses import dataclass

from pynecore.core.plugin import LiveProviderConfig


@dataclass
class CTraderConfig(LiveProviderConfig):
    """cTrader Open API plugin configuration.

    Covers both the data-ingest and the order-execution side; one OAuth
    application serves both. ``symbol_map`` (TradingView key -> native cTrader
    symbol name) is inherited from :class:`LiveProviderConfig`.
    """

    demo: bool = False
    """Use the demo host (demo.ctraderapi.com) instead of live (live.ctraderapi.com)."""

    client_id: str = ""
    """OAuth application client id, from your own cTrader Open API application."""

    client_secret: str = ""
    """OAuth application client secret."""

    account_id: str = ""
    """The ctidTraderAccountId to trade and stream on, selected from the access token's account list."""
