"""Cross-process rate limiting for FinMind requests.

FinMind bills one request per ``(stock_id, day)`` and rejects with HTTP 402
once the hourly quota is gone; its async client drops failed requests without
raising. Every caller therefore draws from one shared budget, kept in a
``flock``-guarded JSON ledger so separate processes cannot each spend the
full quota.
"""

import errno
import fcntl
import json
import os
import time

DEFAULT_CAPACITY = 600
DEFAULT_MARGIN = 0.7
# FinMind fans a batch out over many concurrent connections, so a budget alone
# is not a rate: a full bucket drains in seconds. Requests are also spaced to
# a sustained rate.
DEFAULT_BATCH = 25
DEFAULT_MAX_RPS = 1.0
WINDOW_SECONDS = 3600.0


def _env_float(name, default):
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def batch_size():
    """Chunk size for our own ``stock_id_list`` fan-outs."""
    return max(1, int(_env_float("DATA_SDK_FINMIND_BATCH", DEFAULT_BATCH)))


class FinMindRateLimiter:
    """Token bucket over a rolling hour, shared through a file lock.

    ``capacity``: ``DATA_SDK_FINMIND_RATE_LIMIT`` if set, else the account's
    ``api_request_limit``, else 600 (the documented verified-token tier).
    """

    def __init__(self, ledger_path, capacity=None, margin=None, max_rps=None):
        self._ledger_path = ledger_path
        self._margin = margin if margin is not None else _env_float(
            "DATA_SDK_FINMIND_RATE_MARGIN", DEFAULT_MARGIN
        )
        self._capacity = self._resolve_capacity(capacity)
        self._max_rps = max_rps if max_rps is not None else _env_float(
            "DATA_SDK_FINMIND_MAX_RPS", DEFAULT_MAX_RPS
        )
        os.makedirs(os.path.dirname(ledger_path) or ".", exist_ok=True)

    @property
    def capacity(self):
        return self._capacity

    def _resolve_capacity(self, capacity):
        env = os.environ.get("DATA_SDK_FINMIND_RATE_LIMIT")
        if env:
            try:
                parsed = int(float(env))
                if parsed > 0:
                    return max(1, int(parsed * self._margin))
            except ValueError:
                pass
        if capacity and capacity > 0:
            return max(1, int(capacity * self._margin))
        return max(1, int(DEFAULT_CAPACITY * self._margin))

    def _open_ledger(self):
        """Open the shared ledger, reporting whether we just created it.

        Mode 666: the ledger is shared accounting, and a writer running under
        a different account must draw from the same budget.
        """
        created = False
        try:
            fd = os.open(self._ledger_path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o666)
            created = True
        except FileExistsError:
            fd = os.open(self._ledger_path, os.O_RDWR)
        try:
            os.fchmod(fd, 0o666)
        except OSError as exc:
            # A ledger owned by another writer keeps its mode; that is fine as
            # long as we can still lock and update it.
            if exc.errno not in (errno.EPERM, errno.EROFS):
                raise
        return fd, created

    def _read_state(self, fd, created):
        os.lseek(fd, 0, os.SEEK_SET)
        raw = os.read(fd, 65536).decode("utf-8", "replace").strip()
        now = time.time()
        if created and not raw:
            return {"tokens": float(self._capacity), "updated": now}
        try:
            state = json.loads(raw)
            tokens = float(state["tokens"])
            updated = float(state["updated"])
            not_before = float(state.get("not_before", 0.0))
        except (ValueError, KeyError, TypeError):
            # An unreadable ledger means an unknown amount is already spent;
            # starting empty only costs waiting, starting full risks overrun.
            return {"tokens": 0.0, "updated": now, "not_before": now}
        # A ledger from the future (clock skew) must not grant free tokens.
        if updated > now:
            updated = now
        return {"tokens": tokens, "updated": updated, "not_before": not_before}

    def _write_state(self, fd, state):
        payload = json.dumps(
            {
                "tokens": round(state["tokens"], 4),
                "updated": state["updated"],
                "not_before": round(state.get("not_before", 0.0), 4),
                "capacity": self._capacity,
                "max_rps": self._max_rps,
            }
        ).encode("utf-8")
        os.lseek(fd, 0, os.SEEK_SET)
        os.ftruncate(fd, 0)
        os.write(fd, payload)

    def _refill(self, state, now):
        elapsed = max(0.0, now - state["updated"])
        refilled = state["tokens"] + elapsed * (self._capacity / WINDOW_SECONDS)
        return min(float(self._capacity), refilled)

    def _try_acquire(self, n):
        """Take ``n`` tokens, or report how long until they can be taken.

        Both the hourly budget and the sustained rate must allow the grant.
        """
        fd, created = self._open_ledger()
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            now = time.time()
            state = self._read_state(fd, created)
            tokens = self._refill(state, now)
            not_before = state.get("not_before", 0.0)

            if now < not_before:
                self._write_state(
                    fd, {"tokens": tokens, "updated": now, "not_before": not_before}
                )
                return not_before - now

            if tokens < n:
                deficit = n - tokens
                self._write_state(
                    fd, {"tokens": tokens, "updated": now, "not_before": not_before}
                )
                return deficit * (WINDOW_SECONDS / self._capacity)

            # Space the next grant so throughput averages the sustained rate.
            self._write_state(
                fd,
                {
                    "tokens": tokens - n,
                    "updated": now,
                    "not_before": now + (n / self._max_rps if self._max_rps > 0 else 0.0),
                },
            )
            return 0.0
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def acquire(self, n=1):
        """Block until ``n`` requests are affordable, then reserve them."""
        if n <= 0:
            return
        if n > self._capacity:
            raise ValueError(
                f"cannot reserve {n} requests: hourly capacity is {self._capacity}"
            )
        while True:
            wait = self._try_acquire(n)
            if wait <= 0:
                return
            time.sleep(min(wait, 60.0))
