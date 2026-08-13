"""``pyne ctrader`` CLI commands.

The single command, ``pyne ctrader auth``, runs the OAuth2 authorization-code
flow against the user's own cTrader Open API application:

1. open a localhost loopback listener (the registered ``redirect_uri``),
2. send the user to the consent page (browser or printed URL),
3. catch the redirect, exchange the ``code`` for a token pair, and
4. store the refresh/access token in the workdir cache via
   :mod:`pynecore_ctrader.session` (never in the user config).

The handler is synchronous: a blocking loopback wait plus a synchronous HTTP
token exchange, with no ``sleep`` polling — :meth:`HTTPServer.handle_request`
blocks until one request arrives and honours the server timeout.
"""
import asyncio
import http.server
import logging
import time
import urllib.parse
import webbrowser
from typing import Any, cast

import typer

from pynecore.cli.app import app_state
from pynecore.core.config import ensure_config

from . import auth, helpers, session
from .config import CTraderConfig

logger = logging.getLogger(__name__)

ctrader_app = typer.Typer(help="cTrader Open API authentication")


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    """Capture the OAuth redirect's ``code``/``error`` query params.

    cTrader does not echo the OAuth ``state`` parameter back to the redirect, so
    there is nothing to validate it against; the loopback flow's protection rests
    on the listener being bound to ``127.0.0.1`` only, short-lived and single-use,
    with the ``code`` exchanged immediately over TLS using the ``client_secret``.
    """

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        code = params.get("code", [""])[0]
        error = params.get("error", [""])[0]
        if not code and not error:
            # Stray request (e.g. /favicon.ico): ignore without stopping the wait.
            self.send_response(404)
            self.end_headers()
            return
        server = cast(_OAuthCallbackServer, self.server)
        server.oauth_code = code
        server.oauth_error = error
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        body = ("<html><body><h3>cTrader authentication complete.</h3>"
                "<p>You can close this tab and return to the terminal.</p></body></html>")
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, *args) -> None:  # noqa: D401 - silence default logging
        """Suppress the default per-request stderr logging."""


class _OAuthCallbackServer(http.server.HTTPServer):
    """Single-use loopback server carrying the OAuth redirect result."""

    oauth_code: str
    oauth_error: str

    def __init__(self, address: tuple[str, int]) -> None:
        super().__init__(address, cast(Any, _CallbackHandler))
        self.oauth_code = ""
        self.oauth_error = ""


@ctrader_app.command("auth")
def ctrader_auth(
    demo: bool | None = typer.Option(None, "--demo/--live",
                                     help="Which environment to store the session under; "
                                          "defaults to the config's demo setting."),
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

    # The OAuth consent/token exchange is environment-agnostic; ``demo`` only
    # selects which session key the token is stored under. Default it from the
    # config so the stored env matches the one the runtime will connect to.
    demo_env = bool(config.demo if demo is None else demo)

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
    consent_url = helpers.AUTH_URI + "?" + urllib.parse.urlencode({
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": helpers.DEFAULT_SCOPE,
        "response_type": "code",
    })

    try:
        server = _OAuthCallbackServer(("127.0.0.1", port))
    except OSError as exc:
        typer.secho(
            f"Error: cannot listen on port {port} ({exc}). "
            f"Choose a free port with --port (and register it as the app's redirect URI).",
            err=True, fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    server.timeout = timeout

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
    while not server.oauth_code and not server.oauth_error:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            typer.secho("Error: timed out waiting for the authorization redirect.",
                        err=True, fg=typer.colors.RED)
            server.server_close()
            raise typer.Exit(1)
        server.timeout = remaining
        server.handle_request()
    server.server_close()

    if server.oauth_error:
        typer.secho(f"Error: authorization failed: {server.oauth_error}",
                    err=True, fg=typer.colors.RED)
        raise typer.Exit(1)

    try:
        tokens = asyncio.run(auth.exchange_code(
            client_id=client_id, client_secret=client_secret,
            code=server.oauth_code, redirect_uri=redirect_uri,
        ))
    except auth.CTraderAuthError as exc:
        typer.secho(f"Error: token exchange failed: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(1)

    session.save_session(tokens, demo=demo_env)
    env_name = "demo" if demo_env else "live"
    typer.secho(f"cTrader authentication stored ({env_name}). "
                "You can now use the ctrader provider.", fg=typer.colors.GREEN)
