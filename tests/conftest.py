"""Shared fixtures for the cTrader plugin tests."""
import sys

import pytest

from pynecore.core.broker.models import OrderEvent

from pynecore_ctrader import session


@pytest.fixture(autouse=True)
def _isolated_session_cache(tmp_path_factory, monkeypatch):
    """Keep every test away from the real workdir's OAuth session cache.

    The plugin loads ``<workdir>/cache/ctrader.json`` when it is built and
    takes the cross-process session lock beside it on a token refresh; a test
    must neither read a running bot's tokens nor create files in its cache.
    """
    path = tmp_path_factory.mktemp("ctrader-session") / "ctrader.json"
    monkeypatch.setattr(session, "_session_path", lambda: path)
    return path


@pytest.fixture(autouse=True)
def _fill_events_carry_the_journal_quantity(monkeypatch):
    """Every fill event the plugin builds carries ``journal_fill_qty``.

    The core's restart check compares the fills it booked with the journal
    rows' ``filled_qty`` through that figure; a fill event without it is a
    port-contract violation the engine can only report.
    """
    init = OrderEvent.__init__

    def checked(self, *args, **kwargs):
        init(self, *args, **kwargs)
        caller = sys._getframe(1).f_globals.get('__name__', '')
        if (caller.startswith('pynecore_ctrader') and (self.fill_qty or 0.0) > 0.0
                and self.journal_fill_qty is None):
            raise AssertionError(f"{caller} built a fill event without journal_fill_qty")

    monkeypatch.setattr(OrderEvent, '__init__', checked)
