"""Per-process endpoint overrides must win over the config file.

A proxy placed in front of the venue for one bot process (the live lab's
chaos cycles) cannot edit the plugin config shared by every other process on
the machine, so the environment has to take precedence over the config
override, which in turn wins over the venue endpoint.
"""

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader.helpers import PROTOBUF_DEMO_HOST, PROTOBUF_PORT, protobuf_endpoint


def __test_override_order_is_environment_then_config_then_venue__(monkeypatch):
    monkeypatch.delenv("PYNE_CTRADER_HOST", raising=False)
    monkeypatch.delenv("PYNE_CTRADER_PORT", raising=False)
    assert protobuf_endpoint(True) == (PROTOBUF_DEMO_HOST, PROTOBUF_PORT)
    assert protobuf_endpoint(True, "proxy.example", 9000) == ("proxy.example", 9000)
    assert protobuf_endpoint(True, "proxy.example") == ("proxy.example", PROTOBUF_PORT)
    monkeypatch.setenv("PYNE_CTRADER_HOST", "localhost")
    monkeypatch.setenv("PYNE_CTRADER_PORT", "47021")
    assert protobuf_endpoint(True, "proxy.example", 9000) == ("localhost", 47021)


def _plugin() -> CTrader:
    return CTrader(symbol="broker:EURUSD", timeframe="1",
                   config=CTraderConfig(demo=True, client_id="c", client_secret="s",
                                        account_id="999", host="proxy.example", port=9000))


def __test_the_wire_client_dials_the_config_override__(monkeypatch):
    monkeypatch.delenv("PYNE_CTRADER_HOST", raising=False)
    monkeypatch.delenv("PYNE_CTRADER_PORT", raising=False)
    wire = _plugin()._make_wire()
    assert (wire._host, wire._port) == ("proxy.example", 9000)


def __test_the_environment_wins_over_the_config_override__(monkeypatch):
    monkeypatch.setenv("PYNE_CTRADER_HOST", "localhost")
    monkeypatch.setenv("PYNE_CTRADER_PORT", "47021")
    wire = _plugin()._make_wire()
    assert (wire._host, wire._port) == ("localhost", 47021)
