"""Shared fixtures for the cTrader plugin tests."""
import pytest

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
