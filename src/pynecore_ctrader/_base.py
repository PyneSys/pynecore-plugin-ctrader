"""Shared connection, authentication and account resolution for the cTrader plugin.

``_CTraderBase`` owns the asyncio :class:`~pynecore_ctrader.wire.WireClient`
lifecycle plus the OAuth socket handshake, and exposes two ways to use them:

- the **persistent** live-streaming lifecycle (:meth:`connect` / :meth:`disconnect`
  / :attr:`is_connected`) that :class:`~pynecore.core.plugin.LiveProviderPlugin`
  drives on its background event loop, and
- a **one-shot synchronous bridge** (:meth:`_run`) used by the historical and
  listing CLI methods (``--list-symbols``, ``download``, ``--symbol-info``): each
  call spins up a private connection, authenticates, runs one coroutine and tears
  the connection down again.

cTrader is a *multi-broker* provider: one OAuth application reaches every broker
the user holds a cTrader account with (Pepperstone, IC Markets, ...). The broker
is selected by the leading segment of the provider string
(``ctrader:pepperstoneuk:EURUSD``) — matched against the short
:attr:`ProtoOATrader.brokerName` slug — with the config ``account_id`` as the
tie-breaker when one broker holds several accounts.

In M2 this base swaps :class:`LiveProviderPlugin` for ``BrokerPlugin``.
"""
import asyncio
import logging
import threading
from collections import deque
from collections.abc import Awaitable, Callable
from typing import cast

from google.protobuf.message import Message

from pynecore.core.plugin import LiveProviderPlugin
from pynecore.types.ohlcv import OHLCV

from . import auth, helpers, session
from .config import CTraderConfig
from .messages import OpenApiMessages_pb2 as _oa
from .messages import OpenApiModelMessages_pb2 as _model
from .wire import CTraderProtocolError, WireClient

logger = logging.getLogger(__name__)


class _CTraderBase(LiveProviderPlugin[CTraderConfig]):
    """Connection, authentication and account/broker resolution for cTrader."""

    plugin_name = "cTrader"
    Config = CTraderConfig
    multi_broker = True

    def __init__(self, *, symbol: str | None = None, timeframe: str | None = None,
                 ohlcv_dir=None, config: CTraderConfig | None = None) -> None:
        """
        :param symbol: Either a broker selector (``"Pepperstone"``) or a
            ``"<broker>:<instrument>"`` pair (``"Pepperstone:EURUSD"``); the
            broker is split off into :attr:`_broker_title` and only the
            instrument is kept as ``self.symbol``. ``None`` leaves both unset
            (the symbol-browser path).
        :param timeframe: Timeframe in TradingView format.
        :param ohlcv_dir: Directory to write the OHLCV file into.
        :param config: Pre-loaded :class:`CTraderConfig`.
        """
        # Pass the original (broker-folded) symbol to the base so the OHLCV path
        # and the ``symbol and timeframe`` assertion see a truthy value, then
        # split the broker off — mirroring the CCXT provider's exchange split.
        super().__init__(symbol=symbol, timeframe=timeframe, ohlcv_dir=ohlcv_dir, config=config)

        broker_title: str | None = None
        instrument: str | None = symbol
        if symbol:
            head, sep, tail = symbol.partition(':')
            broker_title = head or None
            instrument = tail if sep else None
        self._broker_title = broker_title
        self.symbol = instrument

        self._demo = bool(getattr(config, 'demo', False))
        account_id = str(getattr(config, 'account_id', '') or '').strip()
        self._account_id: int | None = int(account_id) if account_id else None
        # The token pair is machine-generated auth state, loaded from the workdir
        # cache rather than the user config; empty until ``pyne ctrader auth`` ran.
        self._tokens = session.load_session(demo=self._demo) or auth.TokenSet(
            access_token="", refresh_token=""
        )

        #: Persistent live connection (``None`` until :meth:`connect`).
        self._wire: WireClient | None = None
        #: ``ctidTraderAccountId`` resolved for the persistent live connection.
        self._live_account_id: int | None = None
        #: Symbol-name -> ``symbolId`` cache, filled on first listing/lookup.
        self._symbols_by_name: dict[str, int] = {}

        # Live-streaming state (filled by ``watch_ohlcv``).
        self._subscribed_symbols: set[str] = set()
        self._watch_symbol_id: int | None = None
        self._pending_bars: deque[OHLCV] = deque()
        self._current_bar: OHLCV | None = None
        self._current_bar_ts: int | None = None
        # Latest spot ``bid`` of the bar in progress. The live trendbar's close
        # lags the spot stream (it is not refreshed on every tick), so the spot
        # bid is the authoritative close. Reset on each rollover.
        self._last_bid: float | None = None
        # Ask-side ``(open, high, low, close)`` accumulated from spot ``ask``
        # quotes for the bar in progress (the bid OHLC comes from the trendbar;
        # the ask is only on the spot stream). Reset on each rollover; ``None``
        # until the first ask tick of the bar.
        self._ask_bar: tuple[float, float, float, float] | None = None

    # --- credentials --------------------------------------------------------

    def _credentials(self) -> tuple[str, str]:
        """Return the OAuth application id/secret, or fail with a clear hint.

        :return: ``(client_id, client_secret)``.
        :raises CTraderAuthError: If either is missing from the config.
        """
        client_id = str(getattr(self.config, 'client_id', '') or '')
        client_secret = str(getattr(self.config, 'client_secret', '') or '')
        if not client_id or not client_secret:
            raise auth.CTraderAuthError(
                "MISSING_CREDENTIALS",
                "set client_id/client_secret in config/plugins/ctrader.toml",
            )
        if not self._tokens.access_token and not self._tokens.refresh_token:
            raise auth.CTraderAuthError(
                "MISSING_TOKEN",
                "no access/refresh token; run `pyne ctrader auth` first",
            )
        return client_id, client_secret

    # --- wire + authentication primitives -----------------------------------

    def _make_wire(self) -> WireClient:
        """Build a wire client for the configured demo/live host."""
        return WireClient(helpers.protobuf_host(self._demo))

    async def _token_call(
        self, wire: WireClient, call: Callable[[str], Awaitable[Message]]
    ) -> Message:
        """Run an access-token-scoped request, refreshing once on failure.

        The expired-token error arrives as a server-defined ``errorCode`` string
        (not a typed enum), so the retry triggers on any protocol error rather
        than a hard-coded code: a genuinely non-token error simply fails again on
        the retry and propagates.

        :param wire: A connected, application-authenticated client.
        :param call: Builds the coroutine for one attempt, given the access token.
        :return: The successful response.
        """
        try:
            return await call(self._tokens.access_token)
        except CTraderProtocolError:
            if not self._tokens.refresh_token:
                raise
            logger.debug("cTrader token-scoped request failed; refreshing on socket")
            self._tokens = await auth.refresh_via_socket(wire, self._tokens.refresh_token)
            # cTrader may rotate the refresh token on refresh; persist the new pair
            # so it survives a restart (the old refresh token may now be invalid).
            session.save_session(self._tokens, demo=self._demo)
            return await call(self._tokens.access_token)

    async def _app_auth(self, wire: WireClient) -> None:
        """Send the application-auth request on a freshly connected socket."""
        client_id, client_secret = self._credentials()
        await wire.send_request(
            _oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret)
        )

    async def _get_accounts(self, wire: WireClient) -> list[_model.ProtoOACtidTraderAccount]:
        """List the trading accounts the access token grants (refresh-aware)."""
        async def call(token: str) -> Message:
            response = await wire.send_request(
                _oa.ProtoOAGetAccountListByAccessTokenReq(accessToken=token)
            )
            return response
        response = cast(_oa.ProtoOAGetAccountListByAccessTokenRes, await self._token_call(wire, call))
        return list(response.ctidTraderAccount)

    async def _account_auth(self, wire: WireClient, account_id: int) -> None:
        """Authorize one trading account on the channel (refresh-aware).

        cTrader rejects a second auth of the same account on one channel
        (``ALREADY_LOGGED_IN``), so each account must be authorized exactly once.

        :param wire: A connected, application-authenticated client.
        :param account_id: The ``ctidTraderAccountId`` to authorize.
        """
        async def call(token: str) -> Message:
            return await wire.send_request(
                _oa.ProtoOAAccountAuthReq(ctidTraderAccountId=account_id, accessToken=token)
            )
        await self._token_call(wire, call)

    async def _broker_name(self, wire: WireClient, account_id: int) -> str:
        """Fetch the broker whitelabel slug for an authorized account.

        ``ProtoOATrader.brokerName`` is the short, space-free broker identifier
        (e.g. ``pepperstoneuk``) — unlike ``brokerTitleShort`` which is a readable
        UI title (e.g. ``Pepperstone - Europe``). The account must already be
        authorized via :meth:`_account_auth`.

        :param wire: A connected client with ``account_id`` authorized.
        :param account_id: The authorized account's ``ctidTraderAccountId``.
        :return: The broker slug (possibly empty if the broker set none).
        """
        response = cast(_oa.ProtoOATraderRes, await wire.send_request(
            _oa.ProtoOATraderReq(ctidTraderAccountId=account_id)
        ))
        return response.trader.brokerName

    async def _resolve_account(
        self, wire: WireClient, accounts: list[_model.ProtoOACtidTraderAccount]
    ) -> int:
        """Authorize and return the ``ctidTraderAccountId`` to trade on.

        Resolution order: an explicit config ``account_id`` wins; otherwise the
        broker slug from the provider string selects it; otherwise the sole
        account of the right kind is used. The pool is first filtered to the
        host's kind (demo accounts on the demo host, live on live).

        Broker matching is on the short ``brokerName`` slug (e.g. ``pepperstoneuk``),
        which lives on ``ProtoOATrader`` and so requires authorizing each candidate
        account before it can be read. The chosen account is left authorized.

        :param wire: A connected, application-authenticated client.
        :param accounts: The accounts the access token grants.
        :return: The selected (and authorized) account id.
        :raises CTraderAuthError: If the choice is empty or ambiguous.
        """
        want_live = not self._demo
        pool = [a for a in accounts if a.isLive == want_live]
        kind = "live" if want_live else "demo"
        if not pool:
            raise auth.CTraderAuthError(
                "NO_TRADING_ACCOUNTS", f"the access token grants no {kind} accounts"
            )
        if self._account_id is not None:
            if any(a.ctidTraderAccountId == self._account_id for a in pool):
                await self._account_auth(wire, self._account_id)
                return self._account_id
            raise auth.CTraderAuthError(
                "ACCOUNT_NOT_FOUND",
                f"account_id {self._account_id} is not among the {kind} accounts",
            )
        if self._broker_title:
            target = self._broker_title.lower()
            matches: list[int] = []
            names: dict[int, str] = {}
            for a in pool:
                await self._account_auth(wire, a.ctidTraderAccountId)
                name = await self._broker_name(wire, a.ctidTraderAccountId)
                names[a.ctidTraderAccountId] = name
                if name.lower() == target:
                    matches.append(a.ctidTraderAccountId)
            if not matches:
                available = ", ".join(sorted({n for n in names.values() if n}))
                raise auth.CTraderAuthError(
                    "BROKER_NOT_FOUND",
                    f"broker '{self._broker_title}' not found; available: {available}",
                )
            if len(matches) > 1:
                ids = ", ".join(str(i) for i in matches)
                raise auth.CTraderAuthError(
                    "ACCOUNT_AMBIGUOUS",
                    f"broker '{self._broker_title}' has multiple accounts; set account_id: {ids}",
                )
            return matches[0]
        if len(pool) > 1:
            ids = ", ".join(str(a.ctidTraderAccountId) for a in pool)
            raise auth.CTraderAuthError(
                "ACCOUNT_AMBIGUOUS",
                f"multiple {kind} accounts; set account_id or name a broker: {ids}",
            )
        await self._account_auth(wire, pool[0].ctidTraderAccountId)
        return pool[0].ctidTraderAccountId

    async def _full_handshake(self, wire: WireClient) -> int:
        """Run app-auth, then resolve and authorize the account; return its id."""
        await self._app_auth(wire)
        accounts = await self._get_accounts(wire)
        return await self._resolve_account(wire, accounts)

    # --- one-shot synchronous bridge ----------------------------------------

    def _run(self, coro: Awaitable) -> object:
        """Run ``coro`` to completion on a private event loop.

        Used by the synchronous CLI data methods. When called from outside any
        running loop (the offline ``pyne data`` path) it uses :func:`asyncio.run`
        directly; when a loop is already running (e.g. live warmup inside the
        async runner) it runs the coroutine on a fresh loop in a worker thread so
        it never clashes with the caller's loop.

        :param coro: The coroutine to run.
        :return: The coroutine's result.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        box: dict[str, object] = {}
        error: dict[str, BaseException] = {}

        def worker() -> None:
            try:
                box['value'] = asyncio.run(coro)
            except BaseException as exc:  # noqa: BLE001 - re-raised on the caller thread
                error['error'] = exc

        thread = threading.Thread(target=worker, name="ctrader-oneshot")
        thread.start()
        thread.join()
        if error:
            raise error['error']
        return box['value']

    async def _authed_session(self, work: Callable[[WireClient, int], Awaitable]) -> object:
        """Connect, run the full handshake, run ``work`` and disconnect.

        :param work: Coroutine factory receiving the connected client and the
            resolved account id.
        :return: Whatever ``work`` returns.
        """
        wire = self._make_wire()
        await wire.connect()
        try:
            account_id = await self._full_handshake(wire)
            return await work(wire, account_id)
        finally:
            await wire.disconnect()

    async def _app_session(self, work: Callable[[WireClient], Awaitable]) -> object:
        """Connect, app-authenticate only, run ``work`` and disconnect.

        Used for account enumeration (``--list-brokers``), which must run before
        any single account is chosen.

        :param work: Coroutine factory receiving the connected client.
        :return: Whatever ``work`` returns.
        """
        wire = self._make_wire()
        await wire.connect()
        try:
            await self._app_auth(wire)
            return await work(wire)
        finally:
            await wire.disconnect()

    # --- persistent live lifecycle ------------------------------------------

    async def connect(self) -> None:
        """Open the persistent live connection and authenticate the account."""
        self._wire = self._make_wire()
        await self._wire.connect()
        self._live_account_id = await self._full_handshake(self._wire)

    async def disconnect(self) -> None:
        """Close the persistent live connection."""
        wire = self._wire
        self._wire = None
        self._live_account_id = None
        if wire is not None:
            await wire.disconnect()

    @property
    def is_connected(self) -> bool:
        """Whether the persistent live connection is open."""
        return self._wire is not None and self._wire.is_connected
