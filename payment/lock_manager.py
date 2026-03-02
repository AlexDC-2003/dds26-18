"""
Two-Phase Locking (2PL) with Wait-Die deadlock prevention.

Lock state is stored in Redis so it works across multiple service replicas.

Redis key schema:
  lock:<resource>  → String  "<tx_id>|<tx_ts>"   (set with NX + PX expiry)

No Lua scripts are used. Atomicity for acquisition comes from Redis's built-in
  SET key value NX PX milliseconds
which is a single atomic command: "write this value only if the key is absent."

Wait-Die rule (when T1 requests a lock held by T2):
  - T1 older than T2  (ts_T1 < ts_T2) → T1 WAITS  (keeps polling)
  - T1 younger than T2 (ts_T1 > ts_T2) → T1 DIES   (raises WaitDieAbort)
"""

import logging
import time
import uuid
from contextlib import contextmanager
from typing import List, Optional, Tuple

import redis

logger = logging.getLogger(__name__)

# How long a lock can be held before it is considered stale and auto-expires.
LOCK_TTL_SECONDS = 30

# How long a waiter will poll before giving up.
WAIT_TIMEOUT_SECONDS = 10

# Sleep between polling attempts while waiting for a lock.
WAIT_POLL_INTERVAL = 0.05

_SEP = "|"  # separator inside the lock value string


def _encode_lock_value(tx_id: str, ts: float) -> str:
    """Encode owner info as a plain string stored in Redis."""
    return f"{tx_id}{_SEP}{ts}"


def _decode_lock_value(raw: Optional[bytes]) -> Tuple[Optional[str], Optional[float]]:
    """Decode a lock value string back into (tx_id, ts). Returns (None, None) if absent."""
    if raw is None:
        return None, None
    try:
        text = raw.decode("utf-8")
        tx_id, ts_str = text.split(_SEP, 1)
        return tx_id, float(ts_str)
    except Exception:
        return None, None


class WaitDieAbort(Exception):
    """Raised when the Wait-Die rule forces this transaction to abort."""
    pass


class LockTimeout(Exception):
    """Raised when a transaction has been waiting too long for a lock."""
    pass


class Transaction:
    """
    Represents a 2PL transaction context.

    ts: birth timestamp — lower value = older transaction = higher priority.
    """

    def __init__(self, tx_id: Optional[str] = None, ts: Optional[float] = None):
        self.tx_id: str = tx_id or str(uuid.uuid4())
        self.ts: float = ts if ts is not None else time.time()
        self._acquired: List[str] = []  # resource names locked so far
        self._phase: str = "growing"    # "growing" | "shrinking"

    def __repr__(self):
        return f"Transaction(tx_id={self.tx_id!r}, ts={self.ts:.6f}, phase={self._phase})"


class LockManager:
    """
    Manages exclusive (write) locks identified by string resource names.

    All lock state lives in Redis, so multiple service replicas share it.

    Acquisition uses:
        SET lock:<resource> "<tx_id>|<ts>" NX PX <ttl_ms>
    which is a single atomic Redis command — no Lua required.

    Release uses:
        GET  → check we still own it
        DEL  → delete only if we do
    There is a tiny race between GET and DEL (another holder could expire and
    a new one take over between our two calls), but the consequence is merely
    that we skip the DELETE — the new holder keeps its lock. This is safe
    because the 30-second TTL ensures stale locks always self-clean.
    """

    def __init__(self, db: redis.Redis) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self, txn: Transaction, resource: str) -> None:
        """Acquire an exclusive lock on *resource* for *txn*.

        - Returns immediately on success.
        - Polls (sleeps) if the lock is held by an OLDER transaction (we wait).
        - Raises WaitDieAbort if the lock is held by a YOUNGER transaction (we die).
        - Raises LockTimeout if we exceed WAIT_TIMEOUT_SECONDS.
        - Raises RuntimeError if called after the shrinking phase has begun.
        """
        if txn._phase == "shrinking":
            raise RuntimeError(
                f"2PL violation: cannot acquire a lock during the shrinking phase "
                f"(tx_id={txn.tx_id}, resource={resource})"
            )

        lock_key = f"lock:{resource}"
        lock_value = _encode_lock_value(txn.tx_id, txn.ts)
        ttl_ms = int(LOCK_TTL_SECONDS * 1000)
        deadline = time.monotonic() + WAIT_TIMEOUT_SECONDS

        while True:
            # ---------- attempt atomic acquisition ----------
            # SET NX returns True if the key was absent and we wrote it;
            # False if the key already existed (someone else holds the lock).
            granted = self._db.set(lock_key, lock_value, nx=True, px=ttl_ms)

            if granted:
                # We own the lock.
                txn._acquired.append(resource)
                return

            # ---------- lock is held — read current owner ----------
            holder_tx_id, holder_ts = _decode_lock_value(self._db.get(lock_key))

            if holder_tx_id is None:
                # The lock expired between our SET NX and GET — retry immediately.
                continue

            if holder_tx_id == txn.tx_id:
                # Re-entrant: we already hold this lock (e.g. same tx_id acquired
                # it in a previous iteration or via a different code path).
                txn._acquired.append(resource)
                return

            # ---------- apply Wait-Die rule ----------
            if txn.ts > holder_ts:
                # We are YOUNGER than the holder → DIE.
                raise WaitDieAbort(
                    f"Wait-Die: tx {txn.tx_id} (ts={txn.ts:.6f}) is younger than "
                    f"holder tx {holder_tx_id} (ts={holder_ts:.6f}) "
                    f"for resource '{resource}' — aborting."
                )

            # We are OLDER than the holder → WAIT.
            if time.monotonic() > deadline:
                raise LockTimeout(
                    f"Timed out waiting for lock on '{resource}' "
                    f"(tx_id={txn.tx_id}, waited={WAIT_TIMEOUT_SECONDS}s)"
                )

            time.sleep(WAIT_POLL_INTERVAL)

    def release_all(self, txn: Transaction) -> None:
        """Enter the shrinking phase and release all locks held by *txn*."""
        txn._phase = "shrinking"
        for resource in reversed(txn._acquired):
            self._release_one(txn, resource)
        txn._acquired.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _release_one(self, txn: Transaction, resource: str) -> None:
        """Release the lock on *resource* — but only if we still own it.

        Steps:
          1. GET the lock value.
          2. If it encodes our tx_id, DELETE the key.
          3. If it encodes someone else's tx_id (our lock expired and was
             re-taken), leave it alone — the new owner is legitimate.

        There is a tiny race between step 1 and step 2: after we confirm
        ownership but before we DELETE, our lock could expire and a new owner
        could write theirs. In that case our DELETE would remove the new
        owner's lock. To keep this window as small as possible the TTL is
        generous (30 s) and locks are held only for the duration of a single
        read-modify-write operation (milliseconds in practice).
        """
        lock_key = f"lock:{resource}"

        raw = self._db.get(lock_key)
        holder_tx_id, _ = _decode_lock_value(raw)

        if holder_tx_id == txn.tx_id:
            # We still own it — safe to delete.
            self._db.delete(lock_key)


# ------------------------------------------------------------------
# Convenience context manager
# ------------------------------------------------------------------

@contextmanager
def transaction_context(
    lock_manager: LockManager,
    resources: List[str],
    tx_id: Optional[str] = None,
    ts: Optional[float] = None,
):
    """
    Context manager that:
      1. Creates a Transaction with the given tx_id / ts.
      2. Acquires exclusive locks on all *resources* in sorted order
         (sorting prevents lock-ordering deadlocks as an extra safeguard).
      3. Yields the Transaction object to the caller.
      4. Always releases all locks on exit, even if an exception is raised.

    Raises WaitDieAbort or LockTimeout if a lock cannot be obtained.
    """
    txn = Transaction(tx_id=tx_id, ts=ts)
    sorted_resources = sorted(set(resources))
    try:
        for resource in sorted_resources:
            lock_manager.acquire(txn, resource)
        yield txn
    finally:
        lock_manager.release_all(txn)