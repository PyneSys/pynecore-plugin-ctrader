"""``pyne ctrader`` CLI commands.

The single command, ``pyne ctrader auth``, runs the OAuth2 authorization-code
flow against the user's own cTrader Open API application:

1. open a localhost loopback listener (the registered ``redirect_uri``),
2. send the user to the consent page (browser or printed URL),
3. catch the redirect, exchange the ``code`` for a token pair, and
4. write the refresh/access token into ``config/plugins/ctrader.toml``.

The handler is synchronous: a blocking loopback wait plus a synchronous HTTP
token exchange, with no ``sleep`` polling — :meth:`HTTPServer.handle_request`
blocks until one request arrives and honours the server timeout.
"""
import asyncio
import http.server
import logging
import secrets
import time
import urllib.parse
import webbrowser
from typing import cast

import typer

from pynecore.cli.app import app_state
from pynecore.core.config import ensure_config, generate_toml

from . import auth, helpers
from .config import CTraderConfig

logger = logging.getLogger(__name__)

ctrader_app = typer.Typer(help="cTrader Open API authentication")


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    """Capture the OAuth redirect's ``code``/``state``/``error`` query params."""

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        code = params.get("code", [""])[0]
        error = params.get("error", [""])[0]
        if not code and not error:
            # Stray request (e.g. /favicon.ico): ignore without stopping the wait.
            self.send_response(404)
            self.end_headers()
            return
        self.server.oauth_code = code  # type: ignore[attr-defined]
        self.server.oauth_state = params.get("state", [""])[0]  # type: ignore[attr-defined]
        self.server.oauth_error = error  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        body = ("<html><body><h3>cTrader authentication complete.</h3>"
                "<p>You can close this tab and return to the terminal.</p></body></html>")
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, *args) -> None:  # noqa: D401 - silence default logging
        """Suppress the default per-request stderr logging."""


@ctrader_app.command("auth")
def ctrader_auth(
    demo: bool = typer.Option(True, "--demo/--live",
                              help="Authenticate against the demo or live system."),
    port: int = typer.Option(8765, "-p", "--port",
                             help="Loopback port; must match the app's registered redirect URI."),
    timeout: int = typer.Option(300, "--timeout",
                                help="Seconds to wait for the browser redirect."),
    no_browser: bool = typer.Option(False, "--no-browser",
                                    help="Print the consent URL instead of opening a browser."),
) -> None:
    """Obtain and store a cTrader OAuth token via the loopback consent flow."""
    config_path = app_state.config_dir / "plugins" / "ctrader.toml"
    config = cast(CTraderConfig, ensure_config(CTraderConfig, config_path))

    client_id = (config.client_id or "").strip()
    client_secret = (config.client_secret or "").strip()
    if not client_id or not client_secret:
        typer.secho(
            f"Error: set client_id and client_secret in {config_path} first "
            "(from your own cTrader Open API application).",
            err=True, fg=typer.colors.RED,
        )
        raise typer.Exit(1)

    # One redirect_uri value, used byte-identically in the consent URL and the
    # token exchange (a mismatch yields invalid_grant).
    redirect_uri = f"http://localhost:{port}"
    state = secrets.token_urlsafe(24)
    consent_url = helpers.AUTH_URI + "?" + urllib.parse.urlencode({
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": helpers.DEFAULT_SCOPE,
        "response_type": "code",
        "state": state,
    })

    try:
        server = http.server.HTTPServer(("127.0.0.1", port), _CallbackHandler)
    except OSError as exc:
        typer.secho(
            f"Error: cannot listen on port {port} ({exc}). "
            f"Choose a free port with --port (and register it as the app's redirect URI).",
            err=True, fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    server.timeout = timeout
    server.oauth_code = ""  # type: ignore[attr-defined]
    server.oauth_state = ""  # type: ignore[attr-defined]
    server.oauth_error = ""  # type: ignore[attr-defined]

    if no_browser or not webbrowser.open(consent_url):
        typer.echo("Open this URL in your browser to authorize:")
        typer.echo(f"  {consent_url}")
    else:
        typer.echo("Opened the consent page in your browser; waiting for the redirect...")

    # Block until the redirect arrives or the deadline passes; no sleep-polling.
    # ``handle_request`` waits (event-driven select) up to ``server.timeout`` for
    # one request and returns early on a stray one (e.g. /favicon.ico), so the
    # timeout is enforced by a monotonic deadline, not by the request count.
    deadline = time.monotonic() + timeout
    while not server.oauth_code and not server.oauth_error:  # type: ignore[attr-defined]
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            typer.secho("Error: timed out waiting for the authorization redirect.",
                        err=True, fg=typer.colors.RED)
            server.server_close()
            raise typer.Exit(1)
        server.timeout = remaining
        server.handle_request()
    server.server_close()

    if server.oauth_error:  # type: ignore[attr-defined]
        typer.secho(f"Error: authorization failed: {server.oauth_error}",  # type: ignore[attr-defined]
                    err=True, fg=typer.colors.RED)
        raise typer.Exit(1)
    if server.oauth_state != state:  # type: ignore[attr-defined]
        typer.secho("Error: state mismatch; aborting (possible CSRF).",
                    err=True, fg=typer.colors.RED)
        raise typer.Exit(1)

    try:
        tokens = asyncio.run(auth.exchange_code(
            client_id=client_id, client_secret=client_secret,
            code=server.oauth_code, redirect_uri=redirect_uri,  # type: ignore[attr-defined]
        ))
    except auth.CTraderAuthError as exc:
        typer.secho(f"Error: token exchange failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(1)

    _persist_tokens(config, config_path, tokens, demo=demo)
    typer.secho("cTrader authentication stored. You can now use the ctrader provider.",
                fg=typer.colors.GREEN)


def _persist_tokens(config: CTraderConfig, config_path, tokens: auth.TokenSet,
                    *, demo: bool) -> None:
    """Write the obtained tokens back into the plugin TOML, preserving the rest."""
    values = {k: v for k, v in vars(config).items() if v not in ("", None, {})}
    values["refresh_token"] = tokens.refresh_token
    values["access_token"] = tokens.access_token
    values["demo"] = demo
    config_path.write_text(generate_toml(CTraderConfig, values))
