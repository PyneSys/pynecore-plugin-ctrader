"""OAuth and socket authentication for the cTrader Open API.

Two distinct phases, deliberately kept apart:

- **Initial consent (HTTP).** Exchanging the browser ``authorization_code`` for a
  token pair has no socket equivalent, so :func:`exchange_code` posts to the
  OAuth token endpoint over HTTPS (a GET with query-string params, the encoding
  the official ``OpenApiPy`` reference uses). Driven by ``pyne ctrader auth``.
- **Runtime socket auth.** Once a :class:`~pynecore_ctrader.wire.WireClient` is
  connected, :func:`authenticate` runs the protobuf handshake
  (``ProtoOAApplicationAuthReq`` -> optional account resolution ->
  ``ProtoOAAccountAuthReq``). Access-token refresh at runtime stays on the same
  socket via :func:`refresh_via_socket` (``ProtoOARefreshTokenReq``) — no second
  HTTP endpoint and no resending of the client secret, since the socket is
  already application-authenticated.

The functions are pure protocol logic: they never touch the config TOML. The
caller (the CLI persists, the provider keeps it in memory) owns the returned
:class:`TokenSet`.
"""
import logging
from dataclasses import dataclass
from typing import cast

import httpx

from . import helpers
from .messages import OpenApiMessages_pb2 as _oa
from .messages import OpenApiModelMessages_pb2 as _model
from .wire import CTraderProtocolError, CTraderWireError, WireClient

logger = logging.getLogger(__name__)


@dataclass
class TokenSet:
    """An OAuth token pair as returned by the token endpoint or a socket refresh.

    :ivar access_token: The Bearer access token used for socket account auth.
    :ivar refresh_token: The long-lived token used to mint new access tokens.
    :ivar token_type: The token type the server reports (normally ``"bearer"``).
    :ivar expires_in: Access-token lifetime in seconds, or ``0`` if unknown.
    """

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = 0


@dataclass
class AuthResult:
    """The outcome of a successful socket authentication handshake.

    :ivar account_id: The ``ctidTraderAccountId`` the socket is now authed on.
    :ivar tokens: The token pair in force, refreshed if the original access
        token had expired (so the caller can persist a rotated refresh token).
    """

    account_id: int
    tokens: TokenSet


class CTraderAuthError(CTraderWireError):
    """Authentication failed at the OAuth endpoint or during account resolution.

    :ivar error_code: A short machine-readable code (from the token endpoint, or
        a local sentinel for account-resolution problems).
    :ivar description: The optional human-readable detail.
    """

    def __init__(self, error_code: str, description: str = "") -> None:
        self.error_code = error_code
        self.description = description
        super().__init__(f"{error_code}: {description}" if description else error_code)


async def exchange_code(
    *, client_id: str, client_secret: str, code: str, redirect_uri: str
) -> TokenSet:
    """Exchange a browser ``authorization_code`` for a token pair over HTTPS.

    :param client_id: The OAuth application's client id.
    :param client_secret: The OAuth application's client secret.
    :param code: The single-use ``code`` returned to the redirect URI.
    :param redirect_uri: The exact redirect URI the consent request used.
    :return: The minted :class:`TokenSet`.
    :raises CTraderAuthError: If the endpoint reports an error.
    """
    params = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    async with httpx.AsyncClient(timeout=helpers.REQUEST_TIMEOUT) as client:
        response = await client.get(helpers.TOKEN_URI, params=params)
    return _token_set_from_http(response)


def _token_set_from_http(response: httpx.Response) -> TokenSet:
    """Parse a token-endpoint HTTP response into a :class:`TokenSet`.

    :param response: The token-endpoint response.
    :return: The parsed token pair.
    :raises CTraderAuthError: On an HTTP error status or an ``errorCode`` body.
    """
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise CTraderAuthError("HTTP_ERROR", f"{exc.response.status_code} from token endpoint") from exc
    data = response.json()
    error_code = data.get("errorCode")
    if error_code:
        raise CTraderAuthError(str(error_code), str(data.get("description", "")))
    return TokenSet(
        access_token=data["accessToken"],
        refresh_token=data.get("refreshToken", ""),
        token_type=data.get("tokenType", "bearer"),
        expires_in=int(data.get("expiresIn", 0)),
    )


async def refresh_via_socket(wire: WireClient, refresh_token: str) -> TokenSet:
    """Mint a fresh access token on an already-connected socket.

    Uses ``ProtoOARefreshTokenReq``, which needs only the refresh token because
    the socket is already application-authenticated.

    :param wire: A connected, application-authenticated client.
    :param refresh_token: The current refresh token.
    :return: The refreshed token pair (the refresh token may be rotated; the old
        one is kept if the server omits it).
    """
    response = await wire.send_request(_oa.ProtoOARefreshTokenReq(refreshToken=refresh_token))
    result = cast(_oa.ProtoOARefreshTokenRes, response)
    return TokenSet(
        access_token=result.accessToken,
        refresh_token=result.refreshToken or refresh_token,
        token_type=result.tokenType or "bearer",
        expires_in=result.expiresIn,
    )


async def get_accounts(
    wire: WireClient, access_token: str
) -> list[_model.ProtoOACtidTraderAccount]:
    """List the trading accounts the access token grants.

    :param wire: A connected, application-authenticated client.
    :param access_token: The access token to enumerate accounts for.
    :return: The accounts, each carrying its ``ctidTraderAccountId`` and
        ``isLive`` flag.
    """
    response = await wire.send_request(
        _oa.ProtoOAGetAccountListByAccessTokenReq(accessToken=access_token)
    )
    result = cast(_oa.ProtoOAGetAccountListByAccessTokenRes, response)
    return list(result.ctidTraderAccount)


async def authenticate(
    wire: WireClient,
    *,
    client_id: str,
    client_secret: str,
    tokens: TokenSet,
    account_id: int | None,
) -> AuthResult:
    """Run the full socket authentication handshake.

    Sends ``ProtoOAApplicationAuthReq`` (client id/secret), then authenticates
    the account with ``ProtoOAAccountAuthReq``. When ``account_id`` is ``None``
    the account is resolved from the token's account list (which must contain
    exactly one).

    If the access token has expired, the account-scoped step fails; this is
    handled by minting a fresh token via :func:`refresh_via_socket` and retrying
    once. Because the expired-token error arrives as a server-defined ``errorCode``
    *string* (not a typed enum), the retry triggers on any protocol error in the
    token-scoped phase rather than on a hard-coded code; a genuinely non-token
    error simply fails again on the retry and propagates.

    :param wire: A freshly connected client.
    :param client_id: The OAuth application's client id.
    :param client_secret: The OAuth application's client secret.
    :param tokens: The current token pair.
    :param account_id: The ``ctidTraderAccountId`` to authenticate, or ``None``
        to resolve it from the token's (sole) account.
    :return: The resolved account id and the token pair in force.
    :raises CTraderProtocolError: If application auth fails, or the token-scoped
        phase still fails after a refresh.
    :raises CTraderAuthError: If account resolution is impossible (no accounts,
        or more than one with no ``account_id`` set).
    """
    await wire.send_request(
        _oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret)
    )

    async def _token_phase(token_set: TokenSet) -> int:
        resolved = account_id
        if resolved is None:
            resolved = _select_sole_account(await get_accounts(wire, token_set.access_token))
        await wire.send_request(
            _oa.ProtoOAAccountAuthReq(
                ctidTraderAccountId=resolved, accessToken=token_set.access_token
            )
        )
        return resolved

    try:
        resolved_id = await _token_phase(tokens)
    except CTraderProtocolError:
        if not tokens.refresh_token:
            raise
        logger.debug("cTrader account auth failed; refreshing access token on socket")
        tokens = await refresh_via_socket(wire, tokens.refresh_token)
        resolved_id = await _token_phase(tokens)
    return AuthResult(account_id=resolved_id, tokens=tokens)


def _select_sole_account(accounts: list[_model.ProtoOACtidTraderAccount]) -> int:
    """Pick the only trading account, or fail if the choice is not unambiguous.

    :param accounts: The accounts the access token grants.
    :return: The single account's ``ctidTraderAccountId``.
    :raises CTraderAuthError: If there are no accounts, or more than one (the
        user must then set ``account_id`` in the config).
    """
    if not accounts:
        raise CTraderAuthError("NO_TRADING_ACCOUNTS", "the access token grants no trading accounts")
    if len(accounts) > 1:
        ids = ", ".join(str(account.ctidTraderAccountId) for account in accounts)
        raise CTraderAuthError(
            "ACCOUNT_AMBIGUOUS", f"set account_id; the token grants multiple accounts: {ids}"
        )
    return accounts[0].ctidTraderAccountId
