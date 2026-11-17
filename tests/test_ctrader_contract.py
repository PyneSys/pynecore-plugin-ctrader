"""
@pyne

The plugin passes the PyneCore broker plugin contract probe: the checks a live
run makes at startup, and the port probe the order sync engine makes once the
plugin has wired its ports.
"""
from pynecore.core.broker.validation import validate_plugin_contract, validate_plugin_ports

from pynecore_ctrader import CTrader, CTraderConfig


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


def __test_helper_plugin() -> CTrader:
    return CTrader(symbol=None, config=CTraderConfig(
        demo=True, client_id="cid", client_secret="sec", account_id="999"))


def __test_plugin_passes_the_contract_probe__():
    errors, _warnings = validate_plugin_contract(__test_helper_plugin())
    assert errors == []


def __test_hedged_account_port_passes_the_port_probe__():
    # A HEDGED account opts into core one-way emulation with the plugin itself
    # as the port (``connect()`` wires it).
    plugin = __test_helper_plugin()
    plugin.position_port = plugin
    assert validate_plugin_ports(plugin) == []
