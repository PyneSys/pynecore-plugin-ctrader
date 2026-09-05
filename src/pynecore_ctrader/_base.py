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
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import TYPE_CHECKING, Any, TypeVar, cast

from google.protobuf.message import Message

from pynecore.core.broker.exceptions import (
    ExchangeConnectionError,
    ExchangeRateLimitError,
)
from pynecore.core.plugin.broker import BrokerPlugin
from pynecore.types.ohlcv import OHLCV

from . import auth, helpers, session
from .config import CTraderConfig
from .exceptions import (
    is_account_auth_lost,
    is_client_auth_lost,
    is_rate_limited,
    is_token_invalid,
    map_protocol_error,
)
from .messages import OpenApiMessages_pb2 as OpenApiMessages
from .messages import OpenApiModelMessages_pb2 as OpenApiModelMessages
from .wire import (
    CTraderConnectionError,
    CTraderProtocolError,
    CTraderTimeoutError,
    WireClient,
)

if TYPE_CHECKING:
    from pynecore.core.broker.disappearance import DisappearanceTracker
    from pynecore.core.broker.models import (
        DispatchEnvelope,
        ExchangeOrder,
        OrderEvent,
        PositionLeg,
    )

    from .models import _SymbolRules

logger = logging.getLogger(__name__)

_ResultT = TypeVar('_ResultT')


#: Hard ceiling on a single mid-session account re-authorization (token refresh
#: + app/account auth round-trips). A recovery that overruns is cancelled —
#: releasing :attr:`_reauth_lock` (so a stalled re-auth cannot wedge the lock
#: and cascade-stall every subsequent operational call) and surfacing a
#: recoverable ``ExchangeConnectionError`` the engine parks. Bounds the lock
#: hold and the re-auth phase only, NOT the whole dispatch round-trip (the
#: original failing send and the post-recovery retry have their own wire
#: timeouts) — it is the lock-cascade guard, not a dispatch-deadline guarantee.
_REAUTH_TIMEOUT = 20.0

#: Idempotent reads rejected by a cTrader payload throttle are retried on the
#: same authenticated wire instead of opening another session and increasing
#: the startup request burst.
_RATE_LIMIT_REQUEST_ATTEMPTS = 5
_RATE_LIMIT_RETRY_BUDGET_SECONDS = 120.0
_RATE_LIMIT_FALLBACK_SECONDS = 1.0
#: The venue's ``retryAfter`` hint describes the per-second request pacing, not
#: the rolling history quota: when that quota is empty, obeying the small hint
#: verbatim burns every attempt within seconds and the raise kills the run
#: (observed live: a restart burst drained the quota and warmup died with a raw
#: BLOCKED_PAYLOAD_TYPE). Retries therefore also grow geometrically from the
#: fallback, capped per wait; the venue's own hint still wins when larger.
_RATE_LIMIT_BACKOFF_CAP_SECONDS = 120.0
#: Blocking startup reads (symbol metadata and the warmup history download) may
#: legitimately have to sit out a drained rolling quota; a bot that dies there
#: cannot recover on its own, while one that waits minutes can. Live-path reads
#: keep the tight default profile so a throttled periodic pass fails fast and
#: is skipped instead of stalling its loop.
_RATE_LIMIT_HISTORY_ATTEMPTS = 12
_RATE_LIMIT_HISTORY_BUDGET_SECONDS = 900.0


class _CTraderBase(BrokerPlugin[CTraderConfig], ABC):
    """Connection, authentication and account/broker resolution for cTrader.

    This is the concrete shared state/lifecycle base of the final plugin. The
    feature layers remain explicit abstract mix-ins until their methods are
    composed by :class:`pynecore_ctrader.plugin.CTrader`.
    """

    plugin_name = "cTrader"
    Config = CTraderConfig
    multi_broker = True
    initial_connect_timeout = 180.0

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
        #: The ``(symbol, timeframe)`` the live feed should be subscribed to.
        #: Recorded by the first successful subscribe and NEVER cleared on
        #: reconnect — it is the desired-subscription record, not connection
        #: state — so ``on_reconnect`` can replay the subscription on the
        #: fresh wire outside the framework's ``watch_ohlcv`` timeout budget.
        self._live_subscription: tuple[str, str] | None = None
        self._watch_symbol_id: int | None = None
        self._pending_bars: deque[OHLCV] = deque()
        self._current_bar: OHLCV | None = None
        self._current_bar_ts: int | None = None
        # Last closed live bar handed to PyneCore. Unlike connection-scoped
        # accumulators this survives reconnect so the provider can query the
        # venue for closed trendbars missed while the socket was down.
        self._last_live_closed_bar: OHLCV | None = None
        # Opening of the first bar a reconnect backfill must recover while no
        # closed bar has been delivered yet — everything before it came from
        # the warmup history / the startup-gap query. Survives reconnect for
        # the same reason ``_last_live_closed_bar`` does; without it an outage
        # that starts before the stream's first close has nothing to resume
        # from and its bars are lost.
        self._live_gap_cursor_ts: int | None = None
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
        #: Core disappearance state machine (stamp / clear / grace /
        #: dual-signal). Built lazily by
        #: ``_ReconcileMixin._disappearance_tracker`` — construction reads
        #: ``store_ctx``, ``on_unexpected_cancel`` and ``quarantine_sink``,
        #: all injected after ``__init__``.
        self._disappearance: 'DisappearanceTracker | None' = None
        #: Consecutive failed reconcile passes. Rate-limits the transient-
        #: failure warning (the pass runs every ~5 s, so a network outage
        #: would otherwise warn 12×/minute) and lets the recovery line report
        #: how many passes the outage cost. See
        #: ``_EventStreamMixin._run_reconcile_pass``.
        self._reconcile_fail_streak: int = 0
        #: Position ids whose pyramid-sharing was already audit-logged, so the
        #: ``ctrader_position_id_shared`` warning fires once per position per run
        #: rather than per linking entry. See :meth:`_link_position_ref`.
        self._shared_position_logged: set[int] = set()
        #: Pine entry id (``CloseIntent.pine_id``; ``None`` for close_all) per
        #: ``positionId`` this session dispatched a close against — recorded by
        #: ``execute_close`` / ``close_leg``. A close fill's PUSH copy carries
        #: only the venue's own close ``orderId`` (never in the ref index) plus
        #: the ``positionId``; when no entry row links that position (a
        #: startup-adopted position closed by this run), ``_resolve_identity``
        #: falls back to this map so our OWN close fill is emitted as a CLOSE
        #: leg instead of being dropped as external activity.
        self._close_dispatch_pine_by_position: dict[int, str | None] = {}
        #: One-shot guard for the startup adoption baseline. The engine's startup
        #: ``reconcile`` adopts the broker net position deal-independently; the
        #: FIRST ``get_position`` / ``fetch_raw_positions`` call (which IS that
        #: adoption call) silently advances each live row's ``filled_qty`` cursor
        #: to what the adopted snapshot already reflects, so a post-restart fill
        #: is never re-emitted on top of the adopted size. See
        #: ``_StateMixin._apply_adoption_baseline``.
        self._adoption_baselined: bool = False
        #: Serializes mid-session account re-authorization. cTrader can drop
        #: this channel's account session while the socket stays up (another
        #: connection claimed the account, a server-side recycle). Every
        #: operational coroutine runs on ONE broker event loop, so concurrent
        #: dispatch / state-query / reconcile-pass calls could each observe the
        #: loss and each re-send ``ProtoOAAccountAuthReq``; this lock plus the
        #: generation counter collapse that into a single re-auth. Built here
        #: (sync, off-loop); ``asyncio.Lock`` binds to the running loop on first
        #: await, always the broker loop.
        self._reauth_lock = asyncio.Lock()
        #: Bumped on every successful re-auth. A caller captures it before
        #: awaiting the lock; if it changed once the lock is held, another
        #: caller already re-won the session and this one skips its own re-auth.
        self._reauth_generation: int = 0
        #: Background task for an event-triggered (proactive) re-auth, so the
        #: event router never blocks on the handshake; one in flight at a time.
        self._reauth_task: asyncio.Task | None = None
        #: Local order-write deadline after a definitive cTrader rate-limit
        #: rejection. It survives transport reconnects, so reconnecting cannot
        #: bypass the venue-requested quiet interval.
        self._order_rate_limit_until: float = 0.0

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

    @staticmethod
    async def _wait_rate_limit_retry(seconds: float) -> None:
        """Wait for one venue-requested retry interval without polling or sleep."""
        loop = asyncio.get_running_loop()
        elapsed = loop.create_future()
        handle = loop.call_later(max(0.0, seconds), elapsed.set_result, None)
        try:
            await elapsed
        finally:
            handle.cancel()

    async def _retry_rate_limited(
        self,
        call: Callable[[], Coroutine[Any, Any, _ResultT]],
        *,
        context: str,
        attempts: int = _RATE_LIMIT_REQUEST_ATTEMPTS,
        budget_seconds: float = _RATE_LIMIT_RETRY_BUDGET_SECONDS,
    ) -> _ResultT:
        """Retry one idempotent request after a definitive payload throttle.

        Retries stay on the same wire and preserve the authenticated session.
        Each wait is the larger of the venue-provided ``retryAfter`` and a
        geometric backoff grown from the fallback (capped per wait), so a
        drained rolling quota is sat out instead of being hammered with the
        venue's per-request pacing hint. Both the attempt count and total timer
        budget are bounded.

        :param call: Coroutine factory that re-sends the same safe request.
        :param context: Diagnostic name for the request group.
        :param attempts: Bound on the number of attempts.
        :param budget_seconds: Bound on the total time spent waiting.
        :return: The successful response.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + budget_seconds
        for attempt in range(attempts):
            try:
                return await call()
            except CTraderProtocolError as exc:
                if not is_rate_limited(exc.error_code):
                    raise
                backoff = min(
                    _RATE_LIMIT_FALLBACK_SECONDS * 2.0 ** attempt,
                    _RATE_LIMIT_BACKOFF_CAP_SECONDS,
                )
                venue_hint = (
                    0.0 if exc.retry_after is None
                    else max(0.0, exc.retry_after)
                )
                delay = max(backoff, venue_hint)
                remaining = deadline - loop.time()
                if attempt + 1 >= attempts or delay > remaining:
                    raise
                logger.warning(
                    "cTrader %s was rate-limited; retrying on the same session "
                    "in %.0fs",
                    context, delay,
                )
                await self._wait_rate_limit_retry(delay)
        raise AssertionError("rate-limit retry loop did not terminate")

    async def _token_call(
            self, wire: WireClient,
            call: Callable[[str], Coroutine[Any, Any, Message]],
    ) -> Message:
        """Run an access-token-scoped request, refreshing once on failure.

        The expired-token error arrives as a server-defined ``errorCode`` string
        (not a typed enum), so non-rate-limit protocol errors retain the existing
        refresh-once behavior. Payload throttles instead wait and retry the same
        request on the same wire without rotating a valid token.

        :param wire: A connected, application-authenticated client.
        :param call: Builds the coroutine for one attempt, given the access token.
        :return: The successful response.
        """
        try:
            return await self._retry_rate_limited(
                lambda: call(self._tokens.access_token),
                context="token-scoped request",
            )
        except CTraderProtocolError as exc:
            if is_rate_limited(exc.error_code) or not self._tokens.refresh_token:
                raise
            logger.debug("cTrader token-scoped request failed; refreshing on socket")
            self._tokens = await auth.refresh_via_socket(wire, self._tokens.refresh_token)
            # cTrader may rotate the refresh token on refresh; persist the new pair
            # so it survives a restart (the old refresh token may now be invalid).
            session.save_session(self._tokens, demo=self._demo)
            return await self._retry_rate_limited(
                lambda: call(self._tokens.access_token),
                context="token-scoped request after refresh",
            )

    async def _app_auth(self, wire: WireClient) -> None:
        """Send the application-auth request on a freshly connected socket."""
        client_id, client_secret = self._credentials()
        request = OpenApiMessages.ProtoOAApplicationAuthReq(
            clientId=client_id,
            clientSecret=client_secret,
        )
        await self._retry_rate_limited(
            lambda: wire.send_request(request),
            context="application authentication",
        )

    async def _get_accounts(self, wire: WireClient) -> list[OpenApiModelMessages.ProtoOACtidTraderAccount]:
        """List the trading accounts the access token grants (refresh-aware)."""

        async def call(token: str) -> Message:
            return await wire.send_request(
                OpenApiMessages.ProtoOAGetAccountListByAccessTokenReq(accessToken=token)
            )

        account_list = cast(
            OpenApiMessages.ProtoOAGetAccountListByAccessTokenRes,
            await self._token_call(wire, call),
        )
        return list(account_list.ctidTraderAccount)

    async def _account_auth(self, wire: WireClient, account_id: int) -> None:
        """Authorize one trading account on the channel (refresh-aware).

        cTrader rejects a second auth of the same account on one channel
        (``ALREADY_LOGGED_IN``), so each account must be authorized exactly once.

        :param wire: A connected, application-authenticated client.
        :param account_id: The ``ctidTraderAccountId`` to authorize.
        """

        async def call(token: str) -> Message:
            return await wire.send_request(
                OpenApiMessages.ProtoOAAccountAuthReq(ctidTraderAccountId=account_id, accessToken=token)
            )

        await self._token_call(wire, call)

    # --- mid-session account re-authorization recovery ----------------------

    async def _send_account_auth(self, wire: WireClient) -> None:
        """Re-send ``ProtoOAAccountAuthReq`` for the resolved live account.

        ``ALREADY_LOGGED_IN`` means the channel already holds this account's
        session (it was re-won, or never lost from the wire's view) — a success,
        not a failure. Any other protocol error propagates.

        :param wire: The live, application-authenticated connection.
        """
        request = OpenApiMessages.ProtoOAAccountAuthReq(
            ctidTraderAccountId=self._live_account_id,
            accessToken=self._tokens.access_token,
        )
        try:
            await self._retry_rate_limited(
                lambda: wire.send_request(request),
                context="account re-authorization",
            )
        except CTraderProtocolError as exc:
            if exc.error_code == 'ALREADY_LOGGED_IN':
                return
            raise

    async def _refresh_and_persist(self, wire: WireClient) -> None:
        """Refresh the access token on the socket and persist the rotated pair.

        Shielded from the recovery budget's cancellation: the outer
        :data:`_REAUTH_TIMEOUT` (20s) is shorter than the wire request timeout
        (:data:`~pynecore_ctrader.helpers.REQUEST_TIMEOUT`, 30s), so an unshielded
        refresh could be cancelled mid-flight after its
        ``ProtoOARefreshTokenReq`` was sent. If the response then arrives carrying
        a *rotated* refresh token, dropping it would leave the on-disk refresh
        token stale and break every later auth until a manual re-consent. The
        shield lets the in-flight refresh finish and assign+save the rotated pair
        even when the budget cancels the surrounding re-auth; the refresh stays
        bounded by the wire's own request timeout, so the shield cannot run
        unbounded and re-wedge the lock.

        :param wire: The live, application-authenticated connection.
        """

        async def refresh() -> None:
            self._tokens = await auth.refresh_via_socket(wire, self._tokens.refresh_token)
            session.save_session(self._tokens, demo=self._demo)

        task = asyncio.ensure_future(refresh())
        # When the await below completes normally its result/exception is already
        # retrieved by that frame. When the budget cancels it, the task keeps
        # running detached to persist a rotated token; this callback retrieves a
        # late detached failure so it does not surface as an unhandled-task
        # warning (the next operational request re-drives recovery anyway).
        task.add_done_callback(lambda t: t.cancelled() or t.exception())
        await asyncio.shield(task)

    async def _reauth_account(
            self, *, force_refresh: bool = False, reauth_app: bool = False,
            seen_generation: int | None = None,
    ) -> None:
        """Re-win the account session on the LIVE wire (mid-session recovery).

        Targets the already-resolved :attr:`_live_account_id` with a direct
        ``ProtoOAAccountAuthReq`` — NOT :meth:`_full_handshake`, which would
        re-enumerate accounts and could mutate the plugin identity. The access
        token is kept (it is usually still valid); a token refresh is a fallback
        used only when ``force_refresh`` is set (a token-invalidated push) or the
        re-auth itself reports a token error. When the loss is an APPLICATION
        (client) channel loss, the application is re-authenticated first
        (``reauth_app``, or on demand if the account auth reports a client
        error) — an account cannot be authorized on a de-authenticated client.

        Serialized by :attr:`_reauth_lock`; the generation check collapses a
        burst of concurrent callers into one re-auth: a caller that observed the
        loss passes the generation it saw, and skips its own re-auth if another
        caller already bumped it.

        :param force_refresh: Refresh the access token before re-authorizing.
        :param reauth_app: Re-send ``ProtoOAApplicationAuthReq`` first (client
            channel loss).
        :param seen_generation: The :attr:`_reauth_generation` the caller saw
            before awaiting; ``None`` to always attempt (proactive path).
        :raises CTraderConnectionError: If the live connection is gone.
        :raises CTraderProtocolError: If the re-auth (and its fallbacks) fails.
        """
        async with self._reauth_lock:
            if seen_generation is not None and seen_generation != self._reauth_generation:
                # Another caller already re-won the session since the loss was seen.
                return
            wire = self._wire
            if wire is None or self._live_account_id is None:
                raise CTraderConnectionError("live connection not established")
            if reauth_app:
                await self._app_auth(wire)
            if force_refresh and self._tokens.refresh_token:
                try:
                    await self._refresh_and_persist(wire)
                except CTraderProtocolError as exc:
                    # ``ProtoOARefreshTokenReq`` needs an application-authenticated
                    # socket. A token-invalid loss can coincide with the client
                    # channel also being de-authenticated (or it goes between the
                    # push and this re-auth), so the refresh itself is rejected as
                    # a client-auth loss. Re-authenticate the application once,
                    # then refresh — otherwise this would surface as a needless
                    # ``ExchangeConnectionError`` (heavy full reconnect) for a loss
                    # the in-place re-auth can restore.
                    if not reauth_app and is_client_auth_lost(exc.error_code):
                        reauth_app = True
                        await self._app_auth(wire)
                        await self._refresh_and_persist(wire)
                    else:
                        raise
            try:
                await self._send_account_auth(wire)
            except CTraderProtocolError as exc:
                # The account auth itself reports the token is invalid: refresh
                # once on the socket and re-auth with the fresh token.
                if (not force_refresh and self._tokens.refresh_token
                        and is_token_invalid(exc.error_code)):
                    await self._refresh_and_persist(wire)
                    await self._send_account_auth(wire)
                # The account auth is rejected because the application channel is
                # de-authenticated: re-authenticate the application, then re-auth.
                elif not reauth_app and is_client_auth_lost(exc.error_code):
                    await self._app_auth(wire)
                    await self._send_account_auth(wire)
                else:
                    raise
            self._reauth_generation += 1
            logger.info("cTrader account re-authorized on the live connection")

    async def _recover_account_session(
            self, exc: CTraderProtocolError, seen_generation: int,
    ) -> None:
        """Re-win the account session after an auth-loss error, or surface it.

        On failure — or if the recovery overruns :data:`_REAUTH_TIMEOUT` — the
        loss is raised as the recoverable :class:`ExchangeConnectionError` so the
        engine's reconnect/reconcile path takes over rather than a raw protocol
        error crashing the run. The bounded wait cancels a stalled recovery so it
        releases :attr:`_reauth_lock` (preventing a lock-cascade stall) instead
        of holding it indefinitely. The caller retries its (idempotent) request
        once this returns normally; for an order write the original send was a
        definitive rejection (nothing executed), so surfacing the loss here
        without the retry is safe — the next bar re-dispatches.

        :param exc: The auth-loss protocol error that triggered recovery.
        :param seen_generation: The :attr:`_reauth_generation` the caller read
            BEFORE issuing the request that lost the session — passed through to
            :meth:`_reauth_account` for single-flight coalescing. Captured at
            issue time (not here) so a request whose auth-loss arrives late, after
            a concurrent caller already re-won the session and bumped the
            generation, still observes the stale value and coalesces instead of
            launching a redundant second recovery.
        :raises ExchangeConnectionError: If the session could not be re-won.
        """
        try:
            await asyncio.wait_for(
                self._reauth_account(
                    force_refresh=is_token_invalid(exc.error_code),
                    reauth_app=is_client_auth_lost(exc.error_code),
                    seen_generation=seen_generation,
                ),
                timeout=_REAUTH_TIMEOUT,
            )
        except (CTraderProtocolError, CTraderConnectionError, CTraderTimeoutError,
                asyncio.TimeoutError) as reauth_exc:
            await self._drop_live_wire_after_failed_reauth()
            raise ExchangeConnectionError(
                "cTrader account authorization was lost and could not be restored"
            ) from reauth_exc

    async def _drop_live_wire_after_failed_reauth(self) -> None:
        """Take the live socket down so the transport reconnect starts now.

        A failed in-place re-auth leaves a socket that is writable but useless:
        cTrader answers every account request with the auth loss (measured live:
        a pushed account-disconnect followed by ``UNKNOWN_ERROR: No pooled
        connection to main server``). :attr:`is_connected` sees only the
        transport, so the runner would notice through the feed-staleness
        watchdog minutes later — with position reads unusable past the engine's
        grace the whole time. Closing the socket flips :attr:`is_connected`, and
        the runner's reconnect re-authenticates from scratch right away. A socket
        that is already down needs nothing.
        """
        wire = self._wire
        if wire is None or not wire.is_connected:
            return
        await wire.disconnect()

    @staticmethod
    def _account_auth_loss(message: Message) -> CTraderProtocolError | None:
        """Return the auth-loss error a *successful* response actually carries.

        An account de-auth on an order WRITE is not always raised as a
        ``ProtoOAErrorRes`` (which :meth:`WireClient.send_request` maps to
        :class:`CTraderProtocolError`): cTrader answers the failed
        ``ProtoOANewOrderReq`` with a *correlated* reject — either a
        ``ProtoOAOrderErrorEvent`` or a ``ProtoOAExecutionEvent`` with
        ``executionType == ORDER_REJECTED`` / a set ``errorCode`` — whose code is
        the loss (``ACCOUNT_NOT_AUTHORIZED`` / ``CH_CLIENT_NOT_AUTHENTICATED``).
        Either comes back as the request's normal return value, so without this
        check it would skip recovery and be mapped to a permanent reject —
        dropping a live order the session could re-win. Normalize it into the
        same :class:`CTraderProtocolError` the recovery path keys on (the reject
        is definitive, so a re-send after re-auth cannot duplicate).

        :param message: A response returned (not raised) by ``send_request``.
        :return: The synthesized protocol error, or ``None`` if not an auth loss.
        """
        if isinstance(message, OpenApiMessages.ProtoOAOrderErrorEvent):
            error_code, description = message.errorCode, message.description
        elif isinstance(message, OpenApiMessages.ProtoOAExecutionEvent) and message.errorCode:
            error_code, description = message.errorCode, ""
        else:
            return None
        if not is_account_auth_lost(error_code, description):
            return None
        return CTraderProtocolError(error_code, description)

    async def _account_request(self, req: Message) -> Message:
        """Send an account-scoped request, parking a transient connection loss.

        Wraps :meth:`_account_request_raw` (mid-session de-auth re-auth + single
        retry), retries definitive payload throttles on the same authenticated
        wire, and translates a wire-level connection loss / timeout into the
        recoverable :class:`ExchangeConnectionError` the engine parks. A net drop
        during the per-bar broker sync — the reconcile / balance reads and the
        symbol-rule / open-position prefetch — then retries on the next bar
        instead of crashing the run with a raw ``CTraderConnectionError``
        (including its :class:`CTraderRequestSentConnectionError` subclass).

        The order-SEND path calls :meth:`_account_request_raw` directly: it must
        see the raw ``CTraderTimeoutError`` / ``CTraderRequestSentConnectionError``
        to classify an ambiguous post-write drop as disposition-unknown — a
        blanket ``ExchangeConnectionError`` here would let the engine retry and
        duplicate an order cTrader may already hold. Reads are idempotent, so the
        conversion carries no such ambiguity.

        :param req: The request message; re-sent unchanged on retry.
        :return: The successful response.
        :raises ExchangeConnectionError: On an unrecovered connection loss.
        :raises ExchangeRateLimitError: When cTrader asks the caller to back off.
        """
        try:
            return await self._retry_rate_limited(
                lambda: self._account_request_raw(req),
                context=type(req).__name__,
            )
        except CTraderProtocolError as exc:
            mapped = map_protocol_error(exc)
            if isinstance(mapped, ExchangeRateLimitError):
                raise mapped from exc
            raise
        except (CTraderConnectionError, CTraderTimeoutError) as exc:
            raise ExchangeConnectionError(str(exc) or "connection lost") from exc

    async def _account_request_raw(self, req: Message) -> Message:
        """Send an account-scoped request, recovering from a mid-session de-auth.

        On an "account not authorized" error the socket is still up (only this
        channel's account session was dropped), so the session is re-won on the
        same wire and the request is retried once. A failed recovery surfaces as
        :class:`ExchangeConnectionError`, never a raw protocol error.

        A wire-level connection loss / timeout is propagated raw (NOT mapped to
        ``ExchangeConnectionError``) so the order-send path can classify a
        post-write drop as disposition-unknown; read / prefetch callers go
        through the converting :meth:`_account_request` instead.

        The loss arrives two ways and both are recovered: as a raised
        :class:`CTraderProtocolError` (an error RESPONSE), or — for an order
        write — as a correlated ``ProtoOAOrderErrorEvent`` *returned* by
        ``send_request`` (see :meth:`_account_auth_loss`).

        The request is re-sent verbatim, so use this ONLY for requests that are
        safe to repeat: idempotent reads, and order WRITES whose auth-loss was a
        definitive server *rejection* (an error response proves nothing
        executed, so re-sending the same ``client_order_id`` cannot duplicate).

        :param req: The request message; re-sent unchanged on retry.
        :return: The successful response.
        :raises ExchangeConnectionError: If the session could not be re-won.
        """
        wire = self._wire
        if wire is None:
            raise CTraderConnectionError("live connection not established")
        # Capture the re-auth generation BEFORE issuing the request: if this send
        # loses the session and its rejection arrives late — after a concurrent
        # caller already re-won the session and bumped the generation — recovery
        # must still see this pre-send value so it coalesces into the completed
        # re-auth rather than launching a redundant second one (which, on a
        # client-channel loss, would re-app-auth an already-authenticated channel).
        seen_generation = self._reauth_generation
        try:
            response = await wire.send_request(req)
            loss = self._account_auth_loss(response)
            if loss is None:
                return response
            exc = loss
        except CTraderProtocolError as protocol_exc:
            if not is_account_auth_lost(protocol_exc.error_code, protocol_exc.description):
                raise
            exc = protocol_exc
        await self._recover_account_session(exc, seen_generation)
        wire = self._wire
        if wire is None:
            raise ExchangeConnectionError(
                "cTrader live connection not established") from exc
        try:
            response = await wire.send_request(req)
        except CTraderProtocolError as retry_exc:
            # The session was re-won, but the re-sent request hit a fresh
            # auth-loss — the very condition being handled (another
            # connection / server recycle) can recur immediately. Surface it
            # as the recoverable connection error the engine parks, never a
            # raw protocol error that crashes the run. Non-auth protocol
            # errors (a genuine reject) still propagate to the caller's map.
            if is_account_auth_lost(retry_exc.error_code, retry_exc.description):
                raise ExchangeConnectionError(
                    "cTrader account authorization could not be restored"
                ) from retry_exc
            raise
        # A retry that comes back as another correlated auth-loss order event
        # (not raised) is the same recurring loss — surface it as recoverable.
        if self._account_auth_loss(response) is not None:
            raise ExchangeConnectionError(
                "cTrader account authorization could not be restored") from exc
        return response

    def _schedule_reauth(self, *, force_refresh: bool, reauth_app: bool = False) -> None:
        """Proactively re-win the account session off a server de-auth push.

        Runs as a background task so the event router never blocks on the
        handshake, and coalesces a burst of pushes into one in-flight re-auth.
        A failed re-auth is logged and drops the live socket, so the transport
        reconnect re-authenticates from scratch
        (:meth:`_drop_live_wire_after_failed_reauth`).

        :param force_refresh: Refresh the token first (token-invalidated push).
        :param reauth_app: Re-authenticate the application channel first
            (client-disconnect push — all account sessions are terminated).
        """
        if self._reauth_task is not None and not self._reauth_task.done():
            return
        self._reauth_task = asyncio.create_task(
            self._proactive_reauth(force_refresh=force_refresh, reauth_app=reauth_app),
            name="ctrader-reauth",
        )

    async def _proactive_reauth(self, *, force_refresh: bool, reauth_app: bool = False) -> None:
        """Background re-auth body: never raises to the router.

        Bounded by :data:`_REAUTH_TIMEOUT` for the same reason the reactive path
        is: this task holds :attr:`_reauth_lock` while it runs, and a slow cTrader
        (app auth / token refresh / account auth round-trips, each up to the wire
        request timeout) would otherwise wedge the lock far beyond the recovery
        budget — stalling every foreground :meth:`_account_request` that hits an
        auth loss and waits on the same lock, so they keep parking even though the
        socket is up. An overrun is cancelled (releasing the lock) and logged, and
        the live socket is dropped like any other failed re-auth.
        """
        try:
            await asyncio.wait_for(
                self._reauth_account(force_refresh=force_refresh, reauth_app=reauth_app),
                timeout=_REAUTH_TIMEOUT,
            )
        except (CTraderProtocolError, CTraderConnectionError, CTraderTimeoutError,
                asyncio.TimeoutError, OSError) as exc:
            logger.warning(
                "cTrader proactive re-authorization failed (%s); dropping the live "
                "connection so the transport reconnect re-authenticates from scratch",
                exc,
            )
            await self._drop_live_wire_after_failed_reauth()

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
        request = OpenApiMessages.ProtoOATraderReq(
            ctidTraderAccountId=account_id,
        )
        response = cast(
            OpenApiMessages.ProtoOATraderRes,
            await self._retry_rate_limited(
                lambda: wire.send_request(request),
                context="broker-name read",
            ),
        )
        return response.trader.brokerName

    async def _resolve_account(
            self, wire: WireClient, accounts: list[OpenApiModelMessages.ProtoOACtidTraderAccount]
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

    @staticmethod
    def _run(coro: Coroutine[Any, Any, _ResultT]) -> _ResultT:
        """Run one coroutine object to completion on exactly one private loop.

        Used by the synchronous CLI data methods. When called from outside any
        running loop (the offline ``pyne data`` path) it uses :func:`asyncio.run`
        directly; when a loop is already running (e.g. live warmup inside the
        async runner) ownership of the same not-yet-started coroutine object moves
        to one worker thread, where one fresh loop awaits it exactly once.

        :param coro: The coroutine object to run once.
        :return: The coroutine result.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        with ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="ctrader-oneshot",
        ) as executor:
            return executor.submit(asyncio.run, coro).result()

    async def _authed_session(
            self,
            work: Callable[[WireClient, int], Coroutine[Any, Any, _ResultT]],
    ) -> _ResultT:
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

    async def _app_session(
            self,
            work: Callable[[WireClient], Coroutine[Any, Any, _ResultT]],
    ) -> _ResultT:
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
        # Subscriptions are CONNECTION-scoped server state: a fresh TCP
        # session starts with no spot / live-trendbar subscriptions, whatever
        # the previous connection held. Clearing the guard set makes the
        # subscription replay (``on_reconnect`` / the lazy subscribe in
        # ``watch_ohlcv``) re-send SubscribeSpots + SubscribeLiveTrendbar on
        # the new wire — without it a reconnect leaves the data feed
        # permanently silent while ``is_connected`` reports healthy. Quotes
        # queued by the previous connection are dropped along with the
        # per-bar bid/ask accumulators. The bar-in-progress state
        # (``_current_bar`` / ``_current_bar_ts`` / ``_pending_bars``) is
        # cleared too: if the outage crossed a timeframe boundary, the first
        # trendbar on the fresh subscription would otherwise see its newer
        # timestamp finalize the stale pre-disconnect partial as a CLOSED bar
        # (with no fresh spot close), emitting incomplete OHLCV for the
        # boundary instead of leaving it to be backfilled / synthesized. Runs
        # BEFORE the first ``await`` so a connect() that fails mid-handshake
        # can never leave the previous connection's subscription state looking
        # current.
        self._subscribed_symbols.clear()
        if self._spot_events is not None:
            while not self._spot_events.empty():
                self._spot_events.get_nowait()
        self._last_bid = None
        self._ask_bar = None
        self._current_bar = None
        self._current_bar_ts = None
        self._pending_bars.clear()
        wire = self._make_wire()
        self._wire = wire
        await wire.connect()
        # ``_full_handshake`` also latches the plugin-qualified
        # ``BrokerPlugin.account_id`` (``ctrader-<env>-<account>``) so the
        # BrokerStore run-tag / persisted state are scoped to THIS account (two
        # cTrader accounts on the same script + symbol + timeframe must not
        # share state). Mirrors the documented ``capitalcom-<env>-<account>``
        # pattern on ``BrokerPlugin._account_id``.
        account_id = await self._full_handshake(wire)
        self._live_account_id = account_id
        await self._probe_account(account_id)
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
            self._event_router_loop(wire), name="ctrader-event-router"
        )

    async def disconnect(self) -> None:
        """Close the persistent live connection and stop the event router."""
        wire = self._wire
        self._wire = None
        self._live_account_id = None
        # Stop the event router FIRST: its de-auth branches schedule a re-auth
        # task with no await between the check and ``create_task``, so once the
        # router is gone no new re-auth task can appear. THEN sweep the re-auth
        # slot — covering both an in-flight task and any the router spawned
        # during its own cancellation. Reversing the order would let a queued
        # de-auth push spawn a fresh, unsupervised re-auth task during the
        # router's teardown await.
        task = self._event_router_task
        self._event_router_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        reauth_task = self._reauth_task
        self._reauth_task = None
        if reauth_task is not None and not reauth_task.done():
            reauth_task.cancel()
            try:
                await reauth_task
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

    async def _probe_account(self, account_id: int) -> None:
        """Read the trader record once: detect HEDGED mode and cache money/asset.

        A HEDGED account is not refused — the broker-side execution and state
        paths emulate Pine's one-way semantics over its multiple legs (see
        :attr:`hedging_enabled` and :mod:`~pynecore.core.broker.emulator`). The
        account type is recorded so those paths know whether to run the
        multi-leg emulation (HEDGED) or the single-position fast path
        (NETTING). The same record carries ``moneyDigits`` (money-amount
        exponent) and ``depositAssetId`` (balance currency), cached
        unconditionally for the broker state queries.

        :param account_id: The authorized ``ctidTraderAccountId``.
        """
        response = cast(
            OpenApiMessages.ProtoOATraderRes,
            await self._account_request(
                OpenApiMessages.ProtoOATraderReq(
                    ctidTraderAccountId=account_id,
                )
            ),
        )
        trader = response.trader
        self._money_digits = trader.moneyDigits
        self._deposit_asset_id = trader.depositAssetId
        self._hedging_enabled = (
                trader.accountType == OpenApiModelMessages.ProtoOAAccountType.HEDGED
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
        queue is unbounded and ``watch_orders`` empties it eagerly. The server's
        de-authorization pushes (account-disconnect / token-invalidated /
        client-disconnect) proactively schedule a re-auth so the session is
        re-won before the next request would fail — handled non-blockingly so
        the queue keeps draining.
        """
        spot = self._spot_events
        execq = self._exec_events
        try:
            while True:
                message = await wire.events.get()
                if isinstance(message, OpenApiMessages.ProtoOASpotEvent):
                    if spot is not None:
                        spot.put_nowait(message)
                elif isinstance(message, (OpenApiMessages.ProtoOAExecutionEvent,
                                          OpenApiMessages.ProtoOAOrderErrorEvent)):
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
                elif isinstance(message, OpenApiMessages.ProtoOAAccountDisconnectEvent):
                    # The account's session was dropped on this channel while the
                    # socket stays up; re-send ProtoOAAccountAuthReq to re-win it.
                    if message.ctidTraderAccountId == self._live_account_id:
                        logger.warning(
                            "cTrader pushed account-disconnect; "
                            "re-authorizing the live session")
                        self._schedule_reauth(force_refresh=False)
                elif isinstance(message, OpenApiMessages.ProtoOAAccountsTokenInvalidatedEvent):
                    # The access token is no longer valid for these accounts;
                    # refresh the token before re-authorizing. ctidTraderAccountIds
                    # is a repeated field — match by membership.
                    if self._live_account_id in message.ctidTraderAccountIds:
                        logger.warning(
                            "cTrader pushed token-invalidated (%s); refreshing "
                            "the token and re-authorizing", message.reason or "")
                        self._schedule_reauth(force_refresh=True)
                elif isinstance(message, OpenApiMessages.ProtoOAClientDisconnectEvent):
                    # Channel-wide disconnect (no account id): the server
                    # cancelled the APPLICATION connection, so ALL account
                    # sessions are terminated. Re-authenticate the application
                    # first, then the account — a bare account re-auth would be
                    # rejected on a de-authenticated client. If recovery keeps
                    # failing (e.g. an admin block) the loss surfaces on the next
                    # operational request and the transport reconnect takes over.
                    logger.warning(
                        "cTrader pushed client-disconnect (%s); re-authenticating "
                        "the application and re-authorizing the session",
                        message.reason or "")
                    self._schedule_reauth(force_refresh=False, reauth_app=True)
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

    def _owned_position_ids(self) -> set[int] | None:
        """Venue ``positionId``s this run owns, from its durable order journal.

        Run-ownership isolation for the venue snapshot reads. A cTrader account
        reports EVERY open position for a symbol regardless of which run opened
        it, so on a shared account+symbol scope two independent runs see each
        other's legs. Filtering those reads to the set THIS run's own journal
        recorded keeps each run acting only on its own exposure: close / exit
        targeting (:meth:`_find_open_position_id`), the one-way emulator leg
        source (``fetch_raw_positions``), the netted ``get_position`` adoption
        input and ``get_open_orders`` all consult this set.

        Ownership is every live entry row's ``positionId`` (mirrored into
        ``extras['position_id']`` when the entry fills, with ``exchange_order_id``
        as the compatibility fallback) plus every position this run dispatched a
        close against (``_close_dispatch_pine_by_position`` — so the closing
        position stays visible until its close settles). Persisted across
        restarts, so a genuine restart still adopts its own prior position while
        a concurrent fresh run (empty journal) owns nothing and stays flat.

        Returns ``None`` — the "do not filter" sentinel — when no journal is
        available (store off / unit fixtures), preserving the raw account-wide
        reads those paths rely on.
        """
        if self.store_ctx is None:
            return None
        owned: set[int] = set()
        for row in self.store_ctx.iter_live_orders():
            pid = (row.extras or {}).get('position_id')
            if pid is not None:
                owned.add(helpers.parse_protocol_id(pid, field='position_id'))
                continue
            xoid = row.exchange_order_id
            if xoid and xoid.isdigit():
                owned.add(int(xoid))
        owned.update(
            pid for pid in self._close_dispatch_pine_by_position if pid
        )
        return owned

    def _order_is_owned(self, order) -> bool:
        """Report whether a venue working order was placed by THIS run.

        Run-ownership for ``get_open_orders``: a working order is ours when its
        venue ``orderId`` is a journaled ref, or its ``clientOrderId`` echoes one
        of our own rows (a resting entry not yet linked by ``orderId``). Foreign
        runs' working orders on a shared account+symbol scope are excluded so the
        engine never verifies / tracks another run's order. Store-less callers
        never reach this (the caller gates on ``store_ctx``).
        """
        if self.store_ctx is None:
            return True
        if order.orderId and self.store_ctx.find_by_ref(
                'order_id', str(order.orderId)) is not None:
            return True
        return bool(order.clientOrderId
                    and self.store_ctx.get_order(order.clientOrderId) is not None)

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

    def _retire_cancelled_working_order(self, order_id: int) -> None:
        """Close the BrokerStore working-order row of a confirmed cancel.

        A synchronous ``ORDER_CANCELLED`` acknowledgement is consumed by the
        dispatch path (``send_request`` correlates it away), so no PUSH
        ``cancelled`` event ever reaches :meth:`_translate_exec_event` to retire
        the row. Without this the working row stays ``confirmed`` /
        ``closed_ts_ms = NULL`` indefinitely, so a graceful shutdown before the
        reconcile disappearance-grace window leaves a venue-cancelled order live
        in the store — a durable terminal-state inconsistency. Match the row by
        its ``order_id`` alias and close it.

        A partially filled entry whose unfilled residual is cancelled keeps a
        live position under its row (the fill side is still open exposure that
        later close / reconcile events must reverse-map), so a row carrying fills
        is left live for the position-close / reconcile path to retire. A missing
        row (persistence off, or already retired by a concurrent reconcile pass)
        is a benign no-op; :meth:`close_order` is itself idempotent, so a later
        duplicate signal cannot double-count.

        Also writes a durable ``'cancelled'`` audit event alongside the close
        (mirroring the Capital.com confirmed-cancel convention): the generic
        ``order_closed`` row cannot be told apart from a fill-driven close, so
        without it the durable log carries no cancellation terminal at all for
        a synchronously acknowledged cancel — a run audit sees the cancel
        reach the venue with no local terminal progression.

        :param order_id: The broker ``orderId`` of the cancelled working order.
        """
        if self.store_ctx is None or not order_id:
            return
        row = self.store_ctx.find_by_ref('order_id', str(order_id))
        if row is None or row.filled_qty > 1e-9:
            return
        self.store_ctx.close_order(row.client_order_id)
        self.store_ctx.log_event(
            'cancelled',
            client_order_id=row.client_order_id,
            exchange_order_id=str(order_id),
            intent_key=row.intent_key,
        )

    # --- assembled feature surface -----------------------------------------

    @abstractmethod
    async def _fetch_light_symbols(
            self, wire: WireClient, account_id: int, *, recover: bool = False,
    ) -> list[OpenApiModelMessages.ProtoOALightSymbol]: ...

    @abstractmethod
    async def _get_symbol_rules(self, symbol: str) -> '_SymbolRules': ...

    @abstractmethod
    async def _reconcile(
            self, *, return_protection_orders: bool = False,
    ) -> OpenApiMessages.ProtoOAReconcileRes: ...

    @abstractmethod
    def _reconcile_snapshot(self) -> 'AsyncIterator[OrderEvent]': ...

    @abstractmethod
    def _emit_unexpected_cancellations(self) -> 'AsyncIterator[OrderEvent]': ...

    @abstractmethod
    async def _dispatch_order(
            self, req: Message, *, coid: str, context: str,
            predecessor_cancel_ids: tuple[str, ...] | None = None,
    ) -> OpenApiMessages.ProtoOAExecutionEvent: ...

    @abstractmethod
    async def _recover_in_flight_submissions(self) -> None: ...

    @abstractmethod
    async def _resolve_state_symbol_id(self, symbol: str) -> int | None: ...

    @abstractmethod
    async def fetch_raw_positions(self, symbol: str) -> 'list[PositionLeg]': ...

    @abstractmethod
    async def get_volume_quantizer(
            self, symbol: str,
    ) -> 'Callable[[float], int]': ...

    @abstractmethod
    async def close_leg(
            self, symbol: str, leg_id: str, volume: int, coid: str,
    ) -> None: ...

    @abstractmethod
    async def reject_out_of_range(
            self, envelope: 'DispatchEnvelope', qty: float,
    ) -> None: ...

    @abstractmethod
    async def place_leg(
            self, envelope: 'DispatchEnvelope', qty: float,
    ) -> 'list[ExchangeOrder]': ...

    @abstractmethod
    async def amend_bracket(
            self, symbol: str, leg_id: str, *,
            side: str, tp_price: float | None, sl_price: float | None,
            trail_offset: float | None, coid: str,
    ) -> None: ...
