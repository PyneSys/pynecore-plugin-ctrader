"""Persistence of the cTrader OAuth session (the long-lived auth state).

The token pair (``refresh_token`` plus the cached ``access_token``) is
machine-generated authentication state, not user configuration: the user never
hand-edits it, ``pyne ctrader auth`` mints it and the runtime rotates it. It
therefore lives in the workdir ``cache`` directory rather than the user-edited
plugin TOML, so the config file stays a clean, declarative, shareable artifact
that the program never rewrites.

Demo and live tokens are kept under separate keys in one ``ctrader.json`` file
(the access tokens are environment-specific), so authenticating one environment
never clobbers the other. The file is written ``0600`` and atomically (a temp
file unique to the writer, then a replace) because it holds bearer secrets.

Several processes can share the file — a live bot, a ``pyne data`` download, a
second bot on another account of the same grant. A ``ProtoOARefreshTokenReq``
rotates the refresh token, so a process refreshing with a pair another process
already rotated would lose the grant. :func:`acquire_session_lock` serializes
the read-check-refresh-save sequence across processes with an advisory lock on
a sidecar file (POSIX ``flock``; on a platform without it the lock is
process-local only).
"""
import json
import os
import threading
import uuid
from pathlib import Path

from .auth import TokenSet

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platform
    fcntl = None

#: Serializes the session lock inside one process; ``flock`` is per open file
#: description, so two threads of one process would not exclude each other.
_LOCAL_LOCK = threading.Lock()


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
    are preserved, then writes atomically with ``0600`` permissions. The temp
    file name is unique to the call, so two writers never write into each
    other's temp file.

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
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(raw, indent=2) + "\n")
        os.chmod(tmp, 0o600)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


class SessionLock:
    """The held cross-process session lock; :meth:`release` exactly once.

    Acquired by :func:`acquire_session_lock`. Every token refresh runs under
    it: re-read the file, refresh only when no other process rotated the pair
    meanwhile, save. The holder may release it from another thread than the
    one that acquired it.
    """

    def __init__(self, descriptor: int | None) -> None:
        self._descriptor = descriptor
        self._released = False

    def release(self) -> None:
        """Release the lock; a second call is a no-op."""
        if self._released:
            return
        self._released = True
        descriptor = self._descriptor
        try:
            if descriptor is not None and fcntl is not None:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)
        finally:
            _LOCAL_LOCK.release()


def acquire_session_lock() -> SessionLock:
    """Block until this process holds the session lock.

    Blocks the calling thread, so async callers run it in a worker thread.

    :return: The held lock.
    """
    path = _session_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    _LOCAL_LOCK.acquire()
    if fcntl is None:
        return SessionLock(None)
    try:
        descriptor = os.open(path.with_name(f"{path.name}.lock"), os.O_RDWR | os.O_CREAT, 0o600)
    except BaseException:
        _LOCAL_LOCK.release()
        raise
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
    except BaseException:
        os.close(descriptor)
        _LOCAL_LOCK.release()
        raise
    return SessionLock(descriptor)
