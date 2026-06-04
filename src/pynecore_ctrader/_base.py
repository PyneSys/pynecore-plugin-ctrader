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

In M2 this base extends ``BrokerPlugin`` so the one class serves both the
data-provider surface and the high-level order-execution layer, and acts as
the shared base every cTrader mix-in derives from.
"""
import asyncio
import logging
import threading
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, cast

from google.protobuf.message import Message

from pynecore.core.plugin.broker import BrokerPlugin
from pynecore.types.ohlcv import OHLCV

from . import auth, helpers, session
from .config import CTraderConfig
from .messages import OpenApiMessages_pb2 as _oa
from .messages import OpenApiModelMessages_pb2 as _model
from .wire import CTraderProtocolError, WireClient

if TYPE_CHECKING:
    from pynecore.core.broker.models import (
        DispatchEnvelope,
        ExchangeOrder,
        OrderEvent,
        PositionLeg,
    )

    from .models import _SymbolRules

logger = logging.getLogger(__name__)


class _CTraderBase(BrokerPlugin[CTraderConfig]):
    """Connection, authentication and account/broker resolution for cTrader.

    Also the shared base every cTrader mix-in derives from: it declares the
    cross-mix-in instance state and the plugin-private method surface (type
    -only stubs with a ``...`` body) so static analysers resolve the
    ``self.<x>`` references one mix-in makes against another's implementation.
    The real method always wins at runtime via the MRO.
    """

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
        # Optional numeric ``ctidTraderAccountId`` selector from the user config
        # (tie-breaker when one broker holds several accounts). This is NOT the
        # ``BrokerPlugin.account_id`` identity — that inherited ``str | None``
        # slot is populated with a plugin-qualified id once the live account is
        # resolved (see :meth:`connect`), so the BrokerStore run-tag / persisted
        # state never collide across two cTrader accounts running the same
        # script + symbol + timeframe.
        account_id = str(getattr(config, 'account_id', '') or '').strip()
        self._account_selector: int | None = int(account_id) if account_id else None
        # The token pair is machine-generated auth state, loaded from the workdir
        # cache rather than the user config; empty until ``pyne ctrader auth`` ran.
        self._tokens = session.load_session(demo=self._demo) or auth.TokenSet(
            access_token="", refresh_token=""
        )

        #: Persistent live connection (``None`` until :meth:`connect`).
        self._wire: WireClient | None = None
        #: ``ctidTraderAccountId`` resolved for the persistent live connection.
        self._live_account_id: int | None = None
        #: ``True`` once :meth:`_probe_account` reads a HEDGED account type.
        #: Drives the one-way emulation path (multi-leg aggregation / FIFO
        #: close / reversal decomposition); ``False`` on a NETTING account
        #: keeps the single-position fast path unchanged.
        self._hedging_enabled: bool = False
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

        # --- Broker layer (M2) ---
        #: Symbol-name -> order-sizing / precision rules cache (lazy, populated
        #: on first order for the symbol). See :class:`._SymbolRules`.
        self._symbol_rules: dict[str, '_SymbolRules'] = {}
        #: Reverse of ``_symbols_by_name`` (symbolId -> name), filled alongside
        #: it so the broker state queries can label orders/positions by name.
        self._symbols_by_id: dict[int, str] = {}
        #: ``moneyDigits`` exponent for the authorized account; set by the
        #: startup probe and used to decode balance / commission / PnL.
        self._money_digits: int | None = None
        #: ``depositAssetId`` (balance currency); set by the startup probe.
        self._deposit_asset_id: int | None = None
        #: Demultiplexed event queues, created on :meth:`connect`. The shared
        #: ``wire.events`` queue is drained by a single router task that fans
        #: spot events to ``_spot_events`` (consumed by ``watch_ohlcv``) and
        #: execution events to ``_exec_events`` (consumed by ``watch_orders``).
        self._spot_events: asyncio.Queue | None = None
        self._exec_events: asyncio.Queue | None = None
        self._event_router_task: asyncio.Task | None = None
        #: Deal ids already surfaced to ``watch_orders``. A correlated fill the
        #: dispatch path re-injects (``send_request`` consumes the wire copy) and
        #: any uncorrelated PUSH copy of the same fill share a ``dealId``, so this
        #: set keeps the fill from being recorded twice. M2: unbounded (deal ids
        #: are unique and a session's fill volume is modest).
        self._seen_deal_ids: set[int] = set()
        #: Position ids whose pyramid-sharing was already audit-logged, so the
        #: ``ctrader_position_id_shared`` warning fires once per position per run
        #: rather than per linking entry. See :meth:`_link_position_ref`.
        self._shared_position_logged: set[int] = set()
        #: One-shot guard for the startup adoption baseline. The engine's startup
        #: ``reconcile`` adopts the broker net position deal-independently; the
        #: FIRST ``get_position`` / ``fetch_raw_positions`` call (which IS that
        #: adoption call) silently advances each live row's ``filled_qty`` cursor
        #: to what the adopted snapshot already reflects, so a post-restart fill
        #: is never re-emitted on top of the adopted size. See
        #: ``_StateMixin._apply_adoption_baseline``.
        self._adoption_baselined: bool = False

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
        if self._account_selector is not None:
            if any(a.ctidTraderAccountId == self._account_selector for a in pool):
                await self._account_auth(wire, self._account_selector)
                return self._account_selector
            raise auth.CTraderAuthError(
                "ACCOUNT_NOT_FOUND",
                f"account_id {self._account_selector} is not among the {kind} accounts",
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
        """Run app-auth, then resolve and authorize the account; return its id.

        Also latches the plugin-qualified ``BrokerPlugin.account_id`` identity
        from the resolved account. This runs on BOTH the one-shot data path
        (``_authed_session``, driven by the historical warmup) and the
        persistent live :meth:`connect`, so the unified broker storage already
        sees the real account id when ``BrokerStore.open_run`` builds the
        ``RunIdentity`` — before the live connection opens. Without it the
        warmup-only path would leave the inherited ``"default"`` fallback and
        two accounts on the same script/symbol/timeframe would share run state.
        """
        await self._app_auth(wire)
        accounts = await self._get_accounts(wire)
        account_id = await self._resolve_account(wire, accounts)
        env = 'demo' if self._demo else 'live'
        self._account_id = f"ctrader-{env}-{account_id}"
        return account_id

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
        """Open the persistent live connection, authenticate and probe.

        Beyond the M1 handshake this also runs the broker startup probe
        (NETTING enforcement + money/asset cache) and starts the event router
        that demultiplexes the shared ``wire.events`` queue into the spot and
        execution streams.
        """
        self._wire = self._make_wire()
        await self._wire.connect()
        # ``_full_handshake`` also latches the plugin-qualified
        # ``BrokerPlugin.account_id`` (``ctrader-<env>-<account>``) so the
        # BrokerStore run-tag / persisted state are scoped to THIS account (two
        # cTrader accounts on the same script + symbol + timeframe must not
        # share state). Mirrors the documented ``capitalcom-<env>-<account>``
        # pattern on ``BrokerPlugin._account_id``.
        self._live_account_id = await self._full_handshake(self._wire)
        await self._probe_account(self._wire, self._live_account_id)
        if self._hedging_enabled:
            # HEDGED account: opt into core one-way emulation. The Order Sync
            # Engine then drives reducing / closing / reversing / bracket intents
            # through the core OneWayEmulator via this plugin's PositionPort
            # primitives, presenting Pine one-way semantics over the multi-leg
            # account. A NETTING account leaves ``position_port`` None and keeps
            # the cheaper single-position ``execute_*`` path.
            self.position_port = self
        # Persist-first crash recovery: resolve any dispatch row a crash left
        # pending (submitted / disposition_unknown / server_ref_seen) against the
        # broker's authoritative view, and retire startup orphans — BEFORE the
        # engine's startup reconcile adopts the net position. Runs here (not the
        # runner) so a HEDGED or NETTING account recovers identically; a no-op
        # without persistence or a live connection.
        #
        # ONLY before the one-time startup adoption baseline. Recovery confirms a
        # recovered fill WITHOUT emitting an OrderEvent and seeds
        # ``_seen_deal_ids`` — that is only safe because the engine's startup
        # ``reconcile`` adoption (which fires once, after this first ``connect``)
        # folds the broker net into ``BrokerPosition.size``. ``connect`` is also
        # re-entered on every live reconnect (``live_runner`` calls it again),
        # where the engine does NOT re-run adoption; running recovery there would
        # seed a fill into ``_seen_deal_ids`` (suppressing the PUSH replay) yet
        # never apply it to the in-memory position. The periodic
        # ``_reconcile_snapshot`` gap-filler — which DOES emit OrderEvents — owns
        # the post-reconnect resolution instead.
        if not self._adoption_baselined:
            await self._recover_in_flight_submissions()
        # Reuse the demux queues across reconnects rather than replacing them.
        # ``watch_orders`` is consumed by ONE long-lived ``async for`` that
        # captures ``self._exec_events`` once and is never re-invoked on
        # reconnect (unlike ``watch_ohlcv``, which the live runner re-calls per
        # bar under an ``asyncio.wait_for`` and so re-reads the attribute). A
        # fresh queue here would strand that generator on the dead object and
        # silently drop every post-reconnect fill / cancel. Keeping the same
        # object lets the new router task feed the in-flight consumer.
        if self._spot_events is None:
            self._spot_events = asyncio.Queue()
        if self._exec_events is None:
            self._exec_events = asyncio.Queue()
        self._event_router_task = asyncio.create_task(
            self._event_router_loop(self._wire), name="ctrader-event-router"
        )

    async def disconnect(self) -> None:
        """Close the persistent live connection and stop the event router."""
        wire = self._wire
        self._wire = None
        self._live_account_id = None
        task = self._event_router_task
        self._event_router_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if wire is not None:
            await wire.disconnect()

    @property
    def is_connected(self) -> bool:
        """Whether the persistent live connection is open."""
        return self._wire is not None and self._wire.is_connected

    @property
    def hedging_enabled(self) -> bool:
        """Whether the live account is in cTrader HEDGED (multi-leg) mode.

        Resolved once by :meth:`_probe_account`. When ``True`` the broker-side
        execution and state paths run the one-way emulation
        (:mod:`~pynecore.core.broker.emulator`): a symbol may carry several
        legs, so reads aggregate and reduce / close / reversal operations are
        planned across legs oldest-first. ``False`` (NETTING) keeps the
        single-position path the plugin shipped with.
        """
        return self._hedging_enabled

    async def _probe_account(self, wire: WireClient, account_id: int) -> None:
        """Read the trader record once: detect HEDGED mode and cache money/asset.

        A HEDGED account is not refused — the broker-side execution and state
        paths emulate Pine's one-way semantics over its multiple legs (see
        :attr:`hedging_enabled` and :mod:`~pynecore.core.broker.emulator`). The
        account type is recorded so those paths know whether to run the
        multi-leg emulation (HEDGED) or the single-position fast path
        (NETTING). The same record carries ``moneyDigits`` (money-amount
        exponent) and ``depositAssetId`` (balance currency), cached
        unconditionally for the broker state queries.

        :param wire: A connected client with ``account_id`` authorized.
        :param account_id: The authorized ``ctidTraderAccountId``.
        """
        response = cast(_oa.ProtoOATraderRes, await wire.send_request(
            _oa.ProtoOATraderReq(ctidTraderAccountId=account_id)
        ))
        trader = response.trader
        self._money_digits = trader.moneyDigits
        self._deposit_asset_id = trader.depositAssetId
        self._hedging_enabled = (
            trader.accountType == _model.ProtoOAAccountType.HEDGED
        )

    async def _event_router_loop(self, wire: WireClient) -> None:
        """Demultiplex the shared unsolicited-event queue (sole consumer).

        The wire delivers every uncorrelated inbound message on one queue.
        The data feed (``watch_ohlcv``) and the order stream (``watch_orders``)
        run concurrently on the same connection, so one ``.get()`` per
        coroutine would steal the other's messages. This task is the only
        consumer of ``wire.events`` and fans each message out by type.

        Unsolicited messages neither surface consumes (e.g. symbol-change
        events) are dropped; execution events are never dropped — the order
        queue is unbounded and ``watch_orders`` empties it eagerly.
        """
        spot = self._spot_events
        execq = self._exec_events
        try:
            while True:
                message = await wire.events.get()
                if isinstance(message, _oa.ProtoOASpotEvent):
                    if spot is not None:
                        spot.put_nowait(message)
                elif isinstance(message, (_oa.ProtoOAExecutionEvent,
                                          _oa.ProtoOAOrderErrorEvent)):
                    # Broker-slug account resolution authorizes every candidate
                    # account on this channel to read ``brokerName`` (see
                    # ``_resolve_account``), and cTrader then pushes execution
                    # events for ALL authorized accounts. Discard any event not
                    # carrying the selected ``ctidTraderAccountId`` so activity
                    # from a non-selected account cannot enter this run's order
                    # stream and corrupt its position tracking.
                    if (execq is not None
                            and (self._live_account_id is None
                                 or message.ctidTraderAccountId
                                 == self._live_account_id)):
                        execq.put_nowait(message)
        except asyncio.CancelledError:
            raise

    # --- shared broker helpers ----------------------------------------------

    def _symbol_name_for(self, symbol_id: int) -> str:
        """Best-effort reverse lookup of a symbol name from its numeric id.

        Falls back to the stringified id when the light-symbol list has not
        been fetched yet, so the broker state queries still return a usable
        (if numeric) label.

        :param symbol_id: The numeric ``symbolId``.
        :return: The symbol name, or ``str(symbol_id)`` when unknown.
        """
        return self._symbols_by_id.get(symbol_id, str(symbol_id))

    def _link_position_ref(self, coid: str, position_id: int) -> None:
        """Pin the ``position_id`` reverse-map alias to the FIFO-oldest entry.

        Under NETTING every pyramid entry on a symbol shares one ``positionId``,
        but the generic ``position_id`` alias holds exactly ONE client-order-id.
        Overwriting it per entry (last-write-wins) would make a closing fill —
        which carries its own ``orderId``, not an entry's — reverse-map to the
        NEWEST entry. Keep the alias on the OLDEST entry (the FIFO head): a
        NETTING partial close reduces oldest-first, so that is the safer default
        until the exact rule is live-verified via ``ProtoOADealOffsetListReq``.
        Every entry still mirrors its own ``positionId`` into ``orders.extras``
        (see the callers) so a full-position close flattens ALL pyramid rows
        (see :meth:`_mark_position_closed`); only the public alias is FIFO-pinned.
        A second-or-later entry sharing the position is audit-logged once per
        position so the residual mis-attribution risk stays observable.

        :param coid: The entry's client-order-id.
        :param position_id: The shared netted ``positionId`` (``0`` is a no-op).
        """
        if self.store_ctx is None or not position_id:
            return
        existing = self.store_ctx.find_by_ref('position_id', str(position_id))
        if existing is None:
            self.store_ctx.add_ref(coid, 'position_id', str(position_id))
            return
        if existing.client_order_id == coid:
            return
        if position_id not in self._shared_position_logged:
            self._shared_position_logged.add(position_id)
            self.store_ctx.log_event(
                'ctrader_position_id_shared',
                client_order_id=coid,
                exchange_order_id=str(position_id),
                payload={'fifo_head_coid': existing.client_order_id},
            )

    # === Cross-mix-in private surface (type-only) ===========================
    # Implementations live in the provider / execution / state / events
    # mix-ins; declared here with a ``...`` body so each mix-in can call
    # ``self.<name>`` against another's implementation without analyser
    # warnings. The real method always wins at runtime via the MRO.

    async def _fetch_light_symbols(self, wire, account_id: int) -> list: ...

    async def _get_symbol_rules(self, symbol: str) -> '_SymbolRules': ...

    async def _reconcile(
            self, *, return_protection_orders: bool = False,
    ) -> '_oa.ProtoOAReconcileRes': ...

    def _reconcile_snapshot(self) -> 'AsyncIterator[OrderEvent]': ...

    async def _recover_in_flight_submissions(self) -> None: ...

    async def _resolve_state_symbol_id(self, symbol: str) -> int | None: ...

    async def fetch_raw_positions(self, symbol: str) -> 'list[PositionLeg]': ...

    # PositionPort transport surface — real bodies on the execution mix-in;
    # declared here so the assembled plugin structurally satisfies the protocol
    # and ``connect()`` can assign ``self.position_port = self`` on a HEDGED
    # account.
    async def get_volume_quantizer(
            self, symbol: str,
    ) -> 'Callable[[float], int]': ...

    async def close_leg(
            self, symbol: str, leg_id: str, volume: int, coid: str,
    ) -> None: ...

    async def reject_out_of_range(
            self, envelope: 'DispatchEnvelope', qty: float,
    ) -> None: ...

    async def place_leg(
            self, envelope: 'DispatchEnvelope', qty: float,
    ) -> 'list[ExchangeOrder]': ...

    async def amend_bracket(
            self, symbol: str, leg_id: str, *,
            side: str, tp_price: float | None, sl_price: float | None,
            trail_offset: float | None, coid: str,
    ) -> None: ...
