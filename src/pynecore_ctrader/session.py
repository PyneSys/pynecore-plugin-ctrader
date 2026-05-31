"""Persistence of the cTrader OAuth session (the long-lived auth state).

The token pair (``refresh_token`` plus the cached ``access_token``) is
machine-generated authentication state, not user configuration: the user never
hand-edits it, ``pyne ctrader auth`` mints it and the runtime rotates it. It
therefore lives in the workdir ``cache`` directory rather than the user-edited
plugin TOML, so the config file stays a clean, declarative, shareable artifact
that the program never rewrites.

Demo and live tokens are kept under separate keys in one ``ctrader.json`` file
(the access tokens are environment-specific), so authenticating one environment
never clobbers the other. The file is written ``0600`` and atomically (temp file
+ replace) because it holds bearer secrets.
"""
import json
import os
from pathlib import Path

from .auth import TokenSet


def _session_path() -> Path:
    """Return the session-cache file path under the workdir ``cache`` directory.

    :return: ``<workdir>/cache/ctrader.json``.
    """
    # Local import: keep the plugin import graph free of the CLI app module; the
    # session is only ever loaded/saved from the CLI command or the live runtime,
    # both of which have a valid ``app_state.workdir``.
    from pynecore.cli.app import app_state
    return app_state.cache_dir / "ctrader.json"


def _env_key(*, demo: bool) -> str:
    """Map the demo/live flag to its key in the session file."""
    return "demo" if demo else "live"


def load_session(*, demo: bool) -> TokenSet | None:
    """Load the stored token set for the demo or live environment.

    :param demo: Whether to load the demo-environment session.
    :return: The stored tokens, or ``None`` if no usable session is cached for
        that environment (missing file, unreadable, or no refresh token).
    """
    try:
        raw = json.loads(_session_path().read_text())
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    section = raw.get(_env_key(demo=demo))
    if not isinstance(section, dict) or not section.get("refresh_token"):
        return None
    return TokenSet(
        access_token=str(section.get("access_token", "")),
        refresh_token=str(section.get("refresh_token", "")),
        token_type=str(section.get("token_type", "bearer")),
        expires_in=int(section.get("expires_in", 0)),
    )


def save_session(tokens: TokenSet, *, demo: bool) -> None:
    """Persist the token set for the demo or live environment.

    Reads-modifies-writes the one session file so the other environment's tokens
    are preserved, then writes atomically with ``0600`` permissions.

    :param tokens: The token set to store.
    :param demo: Whether these are demo-environment tokens.
    """
    path = _session_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        raw = json.loads(path.read_text())
        if not isinstance(raw, dict):
            raw = {}
    except (OSError, json.JSONDecodeError):
        raw = {}
    raw[_env_key(demo=demo)] = {
        "refresh_token": tokens.refresh_token,
        "access_token": tokens.access_token,
        "token_type": tokens.token_type,
        "expires_in": tokens.expires_in,
    }
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(raw, indent=2) + "\n")
    os.chmod(tmp, 0o600)
    tmp.replace(path)
