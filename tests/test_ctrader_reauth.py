"""
@pyne
"""
import asyncio

import pytest

from pynecore.core.broker.exceptions import (
    ExchangeConnectionError,
    ExchangeRateLimitError,
    OrderDispositionUnknownError,
)

from pynecore_ctrader import CTrader, CTraderConfig
from pynecore_ctrader import auth
from pynecore_ctrader.exceptions import (
    is_account_auth_lost,
    is_client_auth_lost,
    is_token_invalid,
)
from pynecore_ctrader.messages import OpenApiMessages_pb2 as _oa
from pynecore_ctrader.messages import OpenApiModelMessages_pb2 as _model
from pynecore_ctrader.wire import (
    CTraderConnectionError,
    CTraderProtocolError,
    CTraderRequestSentConnectionError,
    CTraderTimeoutError,
    _raise_on_error,
)


# === Fakes ================================================================


class _StopLoop(Exception):
    """Test sentinel to break the event-router loop deterministically."""


class _OneShotEvents:
    """A ``wire.events`` stand-in: yields a fixed list of pushes, then stops the
    router loop by raising :class:`_StopLoop` (which the loop does not catch)."""

    def __init__(self, items):
        self._items = list(items)

    async def get(self):
        if self._items:
            return self._items.pop(0)
        raise _StopLoop


class _ReauthWire:
    """Stubbed wire for the re-auth recovery paths.

    ``ProtoOAAccountAuthReq`` / ``ProtoOARefreshTokenReq`` are answered by their
    configurable outcome (a response by default, or an Exception to raise) and
    counted. Operational requests consume a per-class scripted outcome list (an
    Exception is raised, a message returned); once exhausted a default success
    response is returned, so "fail once, then succeed on retry" is one script
    entry. Every request is recorded in :attr:`requests`.
    """

    def __init__(self):
        self.requests: list = []
        self.is_connected = True
        self.events = None
        self._scripted: dict = {}
        #: Single "always" outcome for ProtoOAAccountAuthReq (Exception -> raise).
        self.account_auth_outcome = None
        #: Sequence of per-call account-auth outcomes consumed before the
        #: "always" outcome (Exception -> raise, else success).
        self.account_auth_outcomes: list = []
        #: When True, account-auth hangs until cancelled (recovery-timeout test).
        self.account_auth_hang = False
        self.refresh_outcome = None
        #: When set, ProtoOARefreshTokenReq blocks on this event before
        #: responding (lets a test cancel the surrounding re-auth mid-refresh).
        self.refresh_gate: asyncio.Event | None = None
        self.account_auth_calls = 0
        self.app_auth_calls = 0
        self.app_auth_outcomes: list = []
        self.refresh_calls = 0

    def script(self, req_cls, *outcomes):
        """Queue per-request-class outcomes consumed in order on send."""
        self._scripted[req_cls] = list(outcomes)
        return self

    async def send_request(self, req):
        self.requests.append(req)
        if isinstance(req, _oa.ProtoOAApplicationAuthReq):
            self.app_auth_calls += 1
            if self.app_auth_outcomes:
                outcome = self.app_auth_outcomes.pop(0)
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome
            return _oa.ProtoOAApplicationAuthRes()
        if isinstance(req, _oa.ProtoOAAccountAuthReq):
            self.account_auth_calls += 1
            if self.account_auth_hang:
                await asyncio.Event().wait()  # cancelled by the recovery timeout
            if self.account_auth_outcomes:
                outcome = self.account_auth_outcomes.pop(0)
                if isinstance(outcome, Exception):
                    raise outcome
                return _oa.ProtoOAAccountAuthRes(ctidTraderAccountId=req.ctidTraderAccountId)
            if isinstance(self.account_auth_outcome, Exception):
                raise self.account_auth_outcome
            return _oa.ProtoOAAccountAuthRes(ctidTraderAccountId=req.ctidTraderAccountId)
        if isinstance(req, _oa.ProtoOARefreshTokenReq):
            self.refresh_calls += 1
            if self.refresh_gate is not None:
                await self.refresh_gate.wait()
            if isinstance(self.refresh_outcome, Exception):
                raise self.refresh_outcome
            return _oa.ProtoOARefreshTokenRes(
                accessToken="fresh-access", refreshToken="fresh-refresh",
                tokenType="bearer", expiresIn=2_628_000,
            )
        for cls, outcomes in self._scripted.items():
            if isinstance(req, cls):
                if outcomes:
                    outcome = outcomes.pop(0)
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome
                break
        if isinstance(req, _oa.ProtoOAReconcileReq):
            return _oa.ProtoOAReconcileRes()
        return _oa.ProtoOAReconcileRes()


def _make_config(**overrides) -> CTraderConfig:
    defaults = dict(demo=True, client_id="cid", client_secret="sec", account_id="999")
    defaults.update(overrides)
    return CTraderConfig(**defaults)


class _ReauthBroker(CTrader):
    """cTrader broker with the wire stubbed and a known token pair."""

    def __init__(self, wire: _ReauthWire):
        super().__init__(symbol=None, config=_make_config())
        self._live_account_id = 999
        self._symbols_by_name = {'EURUSD': 1}
        self._symbols_by_id = {1: 'EURUSD'}
        self._wire = wire
        # Deterministic token pair (not the on-disk session cache).
        self._tokens = auth.TokenSet(
            access_token="tok", refresh_token="ref", expires_in=2_628_000,
        )


class _RateLimitBroker(_ReauthBroker):
    """Capture rate-limit timers without waiting in focused retry tests."""

    retry_delays: list[float] = []

    def __init__(self, wire: _ReauthWire):
        super().__init__(wire)
        type(self).retry_delays = []

    @staticmethod
    async def _wait_rate_limit_retry(seconds: float) -> None:
        _RateLimitBroker.retry_delays.append(seconds)


_AUTH_LOST = CTraderProtocolError('ACCOUNT_NOT_AUTHORIZED', 'Trading account is not authorized')
_AUTH_LOST_GENERIC = CTraderProtocolError('INVALID_REQUEST', 'Trading account is not authorized')


# === Detection helpers ====================================================


def __test_is_account_auth_lost_matches_typed_and_generic__():
    # Typed code and the generic INVALID_REQUEST + "not authorized" description
    # (the shape that crashed the live run) both count as a session loss.
    assert is_account_auth_lost('ACCOUNT_NOT_AUTHORIZED', '')
    assert is_account_auth_lost('INVALID_REQUEST', 'Trading account is not authorized')
    assert is_account_auth_lost('OA_AUTH_TOKEN_EXPIRED', '')
    # An unrelated INVALID_REQUEST must NOT be mistaken for a session loss.
    assert not is_account_auth_lost('INVALID_REQUEST', 'symbol not found')
    assert not is_account_auth_lost('NOT_ENOUGH_MONEY', '')


def __test_is_token_invalid_only_for_token_codes__():
    assert is_token_invalid('OA_AUTH_TOKEN_EXPIRED')
    assert is_token_invalid('CH_ACCESS_TOKEN_INVALID')
    assert not is_token_invalid('ACCOUNT_NOT_AUTHORIZED')


def __test_wire_preserves_rate_limit_retry_after__():
    message = _oa.ProtoOAErrorRes(
        errorCode="BLOCKED_PAYLOAD_TYPE",
        description="injected payload throttle",
        retryAfter=11,
    )

    with pytest.raises(CTraderProtocolError) as caught:
        _raise_on_error(message)

    assert caught.value.retry_after == 11.0


# === Read path: re-auth + retry ===========================================


def __test_reconcile_retries_rate_limit_on_same_session__():
    wire = _ReauthWire().script(
        _oa.ProtoOAReconcileReq,
        CTraderProtocolError(
            "BLOCKED_PAYLOAD_TYPE",
            "injected bounded throttle",
            retry_after=7.0,
        ),
        _oa.ProtoOAReconcileRes(),
    )
    broker = _RateLimitBroker(wire)

    result = asyncio.run(broker._reconcile())

    assert isinstance(result, _oa.ProtoOAReconcileRes)
    assert [type(request).__name__ for request in wire.requests] == [
        "ProtoOAReconcileReq",
        "ProtoOAReconcileReq",
    ]
    assert broker.retry_delays == [7.0]
    assert wire.account_auth_calls == 0
    assert wire.refresh_calls == 0


def __test_reconcile_persistent_rate_limit_is_bounded__():
    blocked = CTraderProtocolError(
        "BLOCKED_PAYLOAD_TYPE",
        "injected persistent throttle",
    )
    wire = _ReauthWire().script(
        _oa.ProtoOAReconcileReq,
        blocked,
        blocked,
        blocked,
        blocked,
        blocked,
    )
    broker = _RateLimitBroker(wire)

    with pytest.raises(ExchangeRateLimitError) as caught:
        asyncio.run(broker._reconcile())

    assert caught.value.retry_after == 1.0
    assert len(wire.requests) == 5
    # Without a venue retryAfter the waits grow geometrically from the
    # fallback: flat 1 s pacing cannot outlive a drained rolling quota.
    assert broker.retry_delays == [1.0, 2.0, 4.0, 8.0]
    assert wire.account_auth_calls == 0
    assert wire.refresh_calls == 0


def __test_application_auth_retries_rate_limit_on_same_wire__():
    wire = _ReauthWire()
    wire.app_auth_outcomes = [
        CTraderProtocolError(
            "BLOCKED_PAYLOAD_TYPE",
            "injected application-auth throttle",
            retry_after=4.0,
        ),
        _oa.ProtoOAApplicationAuthRes(),
    ]
    broker = _RateLimitBroker(wire)

    asyncio.run(broker._app_auth(wire))

    assert wire.app_auth_calls == 2
    assert broker.retry_delays == [4.0]
    assert wire.refresh_calls == 0


def __test_token_scoped_rate_limit_does_not_refresh_valid_token__():
    wire = _ReauthWire().script(
        _oa.ProtoOAGetAccountListByAccessTokenReq,
        CTraderProtocolError(
            "BLOCKED_PAYLOAD_TYPE",
            "injected token-scoped throttle",
            retry_after=3.0,
        ),
        _oa.ProtoOAGetAccountListByAccessTokenRes(),
    )
    broker = _RateLimitBroker(wire)

    accounts = asyncio.run(broker._get_accounts(wire))

    assert accounts == []
    assert [type(request).__name__ for request in wire.requests] == [
        "ProtoOAGetAccountListByAccessTokenReq",
        "ProtoOAGetAccountListByAccessTokenReq",
    ]
    assert broker.retry_delays == [3.0]
    assert wire.refresh_calls == 0


def __test_account_probe_uses_same_session_rate_limit_retry__():
    trader = _model.ProtoOATrader(
        ctidTraderAccountId=999,
        depositAssetId=7,
        accountType=_model.ProtoOAAccountType.HEDGED,
        moneyDigits=2,
    )
    wire = _ReauthWire().script(
        _oa.ProtoOATraderReq,
        CTraderProtocolError(
            "BLOCKED_PAYLOAD_TYPE",
            "injected account probe throttle",
            retry_after=2.0,
        ),
        _oa.ProtoOATraderRes(ctidTraderAccountId=999, trader=trader),
    )
    broker = _RateLimitBroker(wire)

    asyncio.run(broker._probe_account(999))

    assert [type(request).__name__ for request in wire.requests] == [
        "ProtoOATraderReq",
        "ProtoOATraderReq",
    ]
    assert broker.retry_delays == [2.0]
    assert broker._money_digits == 2
    assert broker._deposit_asset_id == 7
    assert broker.hedging_enabled is True


def __test_order_rate_limit_sets_local_prewrite_backoff__():
    wire = _ReauthWire().script(
        _oa.ProtoOANewOrderReq,
        CTraderProtocolError(
            "REQUEST_FREQUENCY_EXCEEDED", "injected bounded throttle",
        ),
    )
    broker = _ReauthBroker(wire)
    request = _oa.ProtoOANewOrderReq(
        ctidTraderAccountId=999,
        symbolId=1,
        orderType=_model.ProtoOAOrderType.LIMIT,
        tradeSide=_model.ProtoOATradeSide.BUY,
        volume=1000,
        limitPrice=1.0,
        clientOrderId="rate-limit-unit",
    )

    async def scenario():
        with pytest.raises(ExchangeConnectionError):
            await broker._dispatch_order(
                request, coid="rate-limit-unit", context="unit throttle",
            )
        sent = len(wire.requests)
        with pytest.raises(ExchangeConnectionError):
            await broker._dispatch_order(
                request, coid="rate-limit-unit", context="unit throttle",
            )
        assert len(wire.requests) == sent

    asyncio.run(scenario())


def __test_reconcile_reauths_and_retries_on_account_not_authorized__():
    # An idempotent read that hits the typed auth-loss re-auths on the same wire
    # and retries once, returning the snapshot — no raw protocol error escapes.
    wire = _ReauthWire().script(_oa.ProtoOAReconcileReq, _AUTH_LOST)
    broker = _ReauthBroker(wire)

    res = asyncio.run(broker._reconcile())

    assert isinstance(res, _oa.ProtoOAReconcileRes)
    assert wire.account_auth_calls == 1
    kinds = [type(r).__name__ for r in wire.requests]
    assert kinds == ['ProtoOAReconcileReq', 'ProtoOAAccountAuthReq', 'ProtoOAReconcileReq']


def __test_reconcile_reauths_on_generic_invalid_request__():
    # The exact shape that crashed the live run: errorCode INVALID_REQUEST with
    # "Trading account is not authorized" in the description (no typed code).
    wire = _ReauthWire().script(_oa.ProtoOAReconcileReq, _AUTH_LOST_GENERIC)
    broker = _ReauthBroker(wire)

    res = asyncio.run(broker._reconcile())

    assert isinstance(res, _oa.ProtoOAReconcileRes)
    assert wire.account_auth_calls == 1


def __test_reauth_treats_already_logged_in_as_success__():
    # A re-sent ProtoOAAccountAuthReq answered with ALREADY_LOGGED_IN means the
    # session is already held — recovery succeeds, the read retries and returns.
    wire = _ReauthWire().script(_oa.ProtoOAReconcileReq, _AUTH_LOST)
    wire.account_auth_outcome = CTraderProtocolError('ALREADY_LOGGED_IN', 'already')
    broker = _ReauthBroker(wire)

    res = asyncio.run(broker._reconcile())

    assert isinstance(res, _oa.ProtoOAReconcileRes)
    assert wire.account_auth_calls == 1


def __test_token_expired_refreshes_then_reauths__(monkeypatch):
    # A token-invalidated auth-loss refreshes the access token on the socket
    # BEFORE re-authorizing, and persists the rotated pair.
    saved: list = []
    monkeypatch.setattr(
        'pynecore_ctrader.session.save_session',
        lambda tokens, *, demo: saved.append(tokens),
    )
    wire = _ReauthWire().script(
        _oa.ProtoOAReconcileReq,
        CTraderProtocolError('OA_AUTH_TOKEN_EXPIRED', 'token expired'),
    )
    broker = _ReauthBroker(wire)

    res = asyncio.run(broker._reconcile())

    assert isinstance(res, _oa.ProtoOAReconcileRes)
    assert wire.refresh_calls == 1
    assert wire.account_auth_calls == 1
    assert broker._tokens.access_token == "fresh-access"
    assert saved and saved[-1].access_token == "fresh-access"


def __test_token_refresh_persists_even_when_budget_cancels_midflight__(monkeypatch):
    # The recovery budget (_REAUTH_TIMEOUT) is shorter than the wire request
    # timeout, so it can cancel the surrounding re-auth while a ProtoOARefreshTokenReq
    # is still in flight. The shielded refresh must finish and persist the ROTATED
    # refresh token anyway — dropping it would leave the on-disk token stale and
    # force a manual re-consent on every later auth.
    saved: list = []
    monkeypatch.setattr(
        'pynecore_ctrader.session.save_session',
        lambda tokens, *, demo: saved.append(tokens),
    )
    monkeypatch.setattr('pynecore_ctrader._base._REAUTH_TIMEOUT', 0.01)
    wire = _ReauthWire()
    wire.refresh_gate = asyncio.Event()
    broker = _ReauthBroker(wire)

    async def scenario():
        # force_refresh re-auth: the refresh blocks on the gate past the budget.
        recover = asyncio.ensure_future(
            broker._recover_account_session(
                CTraderProtocolError('OA_AUTH_TOKEN_EXPIRED', 'token expired'), 0,
            )
        )
        # Let the budget elapse and cancel the re-auth while the refresh is gated.
        with pytest.raises(ExchangeConnectionError):
            await recover
        assert not broker._reauth_lock.locked()  # the budget released the lock
        # The shielded refresh keeps running detached after the cancellation.
        detached = [t for t in asyncio.all_tasks()
                    if t is not asyncio.current_task() and not t.done()]
        assert detached, "the shielded refresh task must outlive the cancellation"
        # Now let it complete; it must still persist the rotated pair.
        wire.refresh_gate.set()
        await asyncio.gather(*detached)
        assert saved and saved[-1].refresh_token == "fresh-refresh"
        assert broker._tokens.refresh_token == "fresh-refresh"

    asyncio.run(scenario())


# === Write path: re-auth + safe retry =====================================


def __test_order_dispatch_reauths_and_retries_on_auth_loss__():
    # An order WRITE rejected for auth-loss is a definitive rejection (it never
    # executed), so after re-auth the SAME request is re-sent once and succeeds.
    wire = _ReauthWire()
    ok = _oa.ProtoOAExecutionEvent(
        executionType=_model.ProtoOAExecutionType.ORDER_ACCEPTED,
    )
    wire.script(_oa.ProtoOANewOrderReq, _AUTH_LOST, ok)
    broker = _ReauthBroker(wire)
    req = _oa.ProtoOANewOrderReq(ctidTraderAccountId=999, clientOrderId='abc')

    msg = asyncio.run(broker._dispatch_order(req, coid='abc', context='entry'))

    assert msg is ok
    assert wire.account_auth_calls == 1
    new_orders = sum(isinstance(r, _oa.ProtoOANewOrderReq) for r in wire.requests)
    assert new_orders == 2  # rejected original + retry, same COID (no duplicate)


def __test_order_dispatch_reauths_on_returned_auth_loss_event__():
    # An order WRITE auth-loss often comes back as a CORRELATED
    # ProtoOAOrderErrorEvent (returned by send_request, not raised as a
    # ProtoOAErrorRes). It must still drive re-auth + retry, not be mapped to a
    # permanent reject — otherwise a live order is dropped the session can re-win.
    wire = _ReauthWire()
    auth_loss_event = _oa.ProtoOAOrderErrorEvent(
        errorCode='ACCOUNT_NOT_AUTHORIZED', description='not authorized',
    )
    ok = _oa.ProtoOAExecutionEvent(
        executionType=_model.ProtoOAExecutionType.ORDER_ACCEPTED,
    )
    wire.script(_oa.ProtoOANewOrderReq, auth_loss_event, ok)
    broker = _ReauthBroker(wire)
    req = _oa.ProtoOANewOrderReq(ctidTraderAccountId=999, clientOrderId='abc')

    msg = asyncio.run(broker._dispatch_order(req, coid='abc', context='entry'))

    assert msg is ok
    assert wire.account_auth_calls == 1  # the session was re-won
    new_orders = sum(isinstance(r, _oa.ProtoOANewOrderReq) for r in wire.requests)
    assert new_orders == 2  # rejected original + retry, same COID (no duplicate)


# === Unrecoverable: surfaces ExchangeConnectionError ======================


def __test_unrecoverable_auth_loss_surfaces_exchange_connection_error__():
    # When the re-auth itself keeps failing, the loss surfaces as the
    # recoverable ExchangeConnectionError (handled by the reconnect machinery),
    # NEVER a raw CTraderProtocolError that would crash the run.
    wire = _ReauthWire().script(_oa.ProtoOAReconcileReq, _AUTH_LOST)
    wire.account_auth_outcome = CTraderProtocolError('CONNECTIONS_LIMIT_EXCEEDED', 'too many')
    broker = _ReauthBroker(wire)

    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker._reconcile())


def __test_dispatch_order_unrecoverable_surfaces_connection_error__():
    # The same on the write path: a failed recovery is ExchangeConnectionError,
    # not a reject (so the engine reconnects rather than dropping the order).
    wire = _ReauthWire().script(_oa.ProtoOANewOrderReq, _AUTH_LOST)
    wire.account_auth_outcome = CTraderProtocolError('CH_CLIENT_AUTH_FAILURE', 'denied')
    broker = _ReauthBroker(wire)
    req = _oa.ProtoOANewOrderReq(ctidTraderAccountId=999, clientOrderId='abc')

    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker._dispatch_order(req, coid='abc', context='entry'))


# === Connection loss (socket drop, not de-auth) ===========================


def __test_read_connection_loss_after_send_surfaces_exchange_connection_error__():
    # The reported crash: the net drops mid-sync, so a reconcile READ raises the
    # wire's CTraderRequestSentConnectionError. It must surface as the recoverable
    # ExchangeConnectionError the engine parks (retry next bar) — NOT a raw wire
    # error that escapes the broker_sync park and crashes the run. No re-auth is
    # attempted: the socket is gone, not the account session.
    wire = _ReauthWire().script(
        _oa.ProtoOAReconcileReq, CTraderRequestSentConnectionError("connection lost"),
    )
    broker = _ReauthBroker(wire)

    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker._reconcile())
    assert wire.account_auth_calls == 0


def __test_read_pre_write_drop_and_timeout_surface_exchange_connection_error__():
    # A clean pre-write CTraderConnectionError and a wire CTraderTimeoutError on an
    # idempotent read are equally recoverable — both park as ExchangeConnectionError.
    for wire_error in (CTraderConnectionError("not connected"),
                       CTraderTimeoutError("no response within 30s")):
        wire = _ReauthWire().script(_oa.ProtoOAReconcileReq, wire_error)
        broker = _ReauthBroker(wire)
        with pytest.raises(ExchangeConnectionError):
            asyncio.run(broker._reconcile())


def __test_order_dispatch_connection_loss_after_send_stays_disposition_unknown__():
    # The write path must NOT be converted to ExchangeConnectionError by the read
    # wrapper: a drop AFTER an order write was sent is ambiguous (cTrader may hold
    # it), so it stays OrderDispositionUnknownError. A blanket connection error
    # here would let the engine re-dispatch and duplicate the order.
    wire = _ReauthWire().script(
        _oa.ProtoOANewOrderReq, CTraderRequestSentConnectionError("connection lost"),
    )
    broker = _ReauthBroker(wire)
    req = _oa.ProtoOANewOrderReq(ctidTraderAccountId=999, clientOrderId='abc')

    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(broker._dispatch_order(req, coid='abc', context='entry'))


# === Single-flight coalescing =============================================


def __test_reauth_generation_coalesces_concurrent_callers__():
    # A caller that observed the pre-recovery generation must skip its own
    # re-auth once another caller has already re-won the session (single-flight),
    # while a caller seeing the current generation does re-auth.
    wire = _ReauthWire()
    broker = _ReauthBroker(wire)

    async def scenario():
        await broker._reauth_account()
        assert broker._reauth_generation == 1
        assert wire.account_auth_calls == 1
        # Stale observer (saw generation 0) — coalesced, no second auth.
        await broker._reauth_account(seen_generation=0)
        assert wire.account_auth_calls == 1
        # Current observer (saw generation 1) — re-auths.
        await broker._reauth_account(seen_generation=1)
        assert wire.account_auth_calls == 2

    asyncio.run(scenario())


# === De-auth push events trigger a proactive re-auth ======================


def __test_account_disconnect_push_triggers_reauth__():
    wire = _ReauthWire()
    wire.events = _OneShotEvents([_oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=999)])
    broker = _ReauthBroker(wire)

    async def scenario():
        try:
            await broker._event_router_loop(wire)
        except _StopLoop:
            pass
        if broker._reauth_task is not None:
            await broker._reauth_task

    asyncio.run(scenario())
    assert wire.account_auth_calls == 1


def __test_account_disconnect_for_other_account_ignored__():
    wire = _ReauthWire()
    wire.events = _OneShotEvents([_oa.ProtoOAAccountDisconnectEvent(ctidTraderAccountId=12345)])
    broker = _ReauthBroker(wire)

    async def scenario():
        try:
            await broker._event_router_loop(wire)
        except _StopLoop:
            pass
        if broker._reauth_task is not None:
            await broker._reauth_task

    asyncio.run(scenario())
    assert wire.account_auth_calls == 0


def __test_token_invalidated_push_refreshes_then_reauths__(monkeypatch):
    monkeypatch.setattr('pynecore_ctrader.session.save_session', lambda *a, **k: None)
    wire = _ReauthWire()
    evt = _oa.ProtoOAAccountsTokenInvalidatedEvent(reason="reconsent")
    evt.ctidTraderAccountIds.append(999)
    wire.events = _OneShotEvents([evt])
    broker = _ReauthBroker(wire)

    async def scenario():
        try:
            await broker._event_router_loop(wire)
        except _StopLoop:
            pass
        if broker._reauth_task is not None:
            await broker._reauth_task

    asyncio.run(scenario())
    assert wire.refresh_calls == 1
    assert wire.account_auth_calls == 1


def __test_client_disconnect_push_reauths_application_then_account__():
    # A client-disconnect terminates ALL account sessions because the
    # application connection was cancelled, so recovery must re-authenticate the
    # application BEFORE the account — a bare account re-auth would be rejected.
    wire = _ReauthWire()
    wire.events = _OneShotEvents([_oa.ProtoOAClientDisconnectEvent(reason="server recycle")])
    broker = _ReauthBroker(wire)

    async def scenario():
        try:
            await broker._event_router_loop(wire)
        except _StopLoop:
            pass
        if broker._reauth_task is not None:
            await broker._reauth_task

    asyncio.run(scenario())
    assert wire.app_auth_calls == 1
    assert wire.account_auth_calls == 1
    kinds = [type(r).__name__ for r in wire.requests]
    assert kinds.index('ProtoOAApplicationAuthReq') < kinds.index('ProtoOAAccountAuthReq')


# === Recurring loss / client-channel escalation / recovery timeout ========


def __test_is_client_auth_lost_only_for_client_codes__():
    assert is_client_auth_lost('CH_CLIENT_NOT_AUTHENTICATED')
    assert is_client_auth_lost('CH_CLIENT_AUTH_FAILURE')
    assert not is_client_auth_lost('ACCOUNT_NOT_AUTHORIZED')
    # Client codes are still detected as an auth loss (so recovery is triggered).
    assert is_account_auth_lost('CH_CLIENT_NOT_AUTHENTICATED', '')


def __test_recurring_auth_loss_on_retry_surfaces_connection_error__():
    # Recovery re-wins the session, but the re-sent request hits a FRESH
    # auth-loss (the session was recycled again). The guarded retry must surface
    # ExchangeConnectionError, never leak a raw protocol error that crashes.
    wire = _ReauthWire().script(_oa.ProtoOAReconcileReq, _AUTH_LOST, _AUTH_LOST)
    broker = _ReauthBroker(wire)

    with pytest.raises(ExchangeConnectionError):
        asyncio.run(broker._reconcile())
    assert wire.account_auth_calls == 1  # one re-auth, then the retry re-failed


def __test_client_auth_loss_reauths_application_first__():
    # A client-channel auth loss on a read re-authenticates the application
    # before the account, then the read retries and returns.
    wire = _ReauthWire().script(
        _oa.ProtoOAReconcileReq,
        CTraderProtocolError('CH_CLIENT_NOT_AUTHENTICATED', 'client not authenticated'),
    )
    broker = _ReauthBroker(wire)

    res = asyncio.run(broker._reconcile())

    assert isinstance(res, _oa.ProtoOAReconcileRes)
    assert wire.app_auth_calls == 1
    assert wire.account_auth_calls == 1


def __test_account_auth_client_error_escalates_to_app_reauth__():
    # The trigger is a plain account loss (no app re-auth up front), but the
    # account-auth send is itself rejected with a client-channel error — recovery
    # must escalate to a ProtoOAApplicationAuthReq, then re-auth and succeed.
    wire = _ReauthWire().script(_oa.ProtoOAReconcileReq, _AUTH_LOST)
    wire.account_auth_outcomes = [
        CTraderProtocolError('CH_CLIENT_NOT_AUTHENTICATED', 'client not authenticated'),
    ]  # first account-auth fails (client), second (post-app-auth) succeeds
    broker = _ReauthBroker(wire)

    res = asyncio.run(broker._reconcile())

    assert isinstance(res, _oa.ProtoOAReconcileRes)
    assert wire.app_auth_calls == 1
    assert wire.account_auth_calls == 2


def __test_recovery_timeout_surfaces_connection_error_and_releases_lock__(monkeypatch):
    # A recovery that overruns the budget is cancelled (releasing _reauth_lock)
    # and surfaces ExchangeConnectionError — so a slow recovery cannot be
    # abandoned still-locked and cascade-stall the next bar.
    monkeypatch.setattr('pynecore_ctrader._base._REAUTH_TIMEOUT', 0.05)
    wire = _ReauthWire().script(_oa.ProtoOAReconcileReq, _AUTH_LOST)
    wire.account_auth_hang = True
    broker = _ReauthBroker(wire)

    async def scenario():
        with pytest.raises(ExchangeConnectionError):
            await broker._reconcile()
        # The lock was released by the cancellation — a fresh re-auth proceeds.
        assert not broker._reauth_lock.locked()
        wire.account_auth_hang = False
        await broker._reauth_account()
        assert broker._reauth_generation == 1

    asyncio.run(scenario())


def __test_proactive_reauth_timeout_releases_lock__(monkeypatch):
    # The proactive (background) re-auth holds _reauth_lock too, so a slow cTrader
    # must NOT wedge it past the recovery budget — otherwise every foreground
    # _account_request that hits an auth loss keeps parking on the held lock even
    # though the socket is up. An overrun is cancelled (releasing the lock) and
    # swallowed by the best-effort body; a fresh re-auth then proceeds.
    monkeypatch.setattr('pynecore_ctrader._base._REAUTH_TIMEOUT', 0.05)
    wire = _ReauthWire()
    wire.account_auth_hang = True
    broker = _ReauthBroker(wire)

    async def scenario():
        await broker._proactive_reauth(force_refresh=False)
        # The hung re-auth was cancelled by the budget, not awaited forever.
        assert not broker._reauth_lock.locked()
        assert broker._reauth_generation == 0  # it never completed
        # A subsequent re-auth on a responsive socket proceeds cleanly.
        wire.account_auth_hang = False
        await broker._reauth_account()
        assert broker._reauth_generation == 1

    asyncio.run(scenario())
