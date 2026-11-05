import logging
import time
import uuid
from contextlib import contextmanager
from typing import List, Optional, Tuple

import redis

logger = logging.getLogger(__name__)

LOCK_TTL_SECONDS = 30

WAIT_TIMEOUT_SECONDS = 10

WAIT_POLL_INTERVAL = 0.05

_SEP = "|"  # separator inside the lock value string


def _encode_lock_value(tx_id: str, ts: float) -> str:
    return f"{tx_id}{_SEP}{ts}"


def _decode_lock_value(raw: Optional[bytes]) -> Tuple[Optional[str], Optional[float]]:
    if raw is None:
        return None, None
    try:
        text = raw.decode("utf-8")
        tx_id, ts_str = text.split(_SEP, 1)
        return tx_id, float(ts_str)
    except Exception:
        return None, None


class WaitDieAbort(Exception):
    pass


class LockTimeout(Exception):
    pass


class Transaction:

    def __init__(self, tx_id: Optional[str] = None, ts: Optional[float] = None):
        self.tx_id: str = tx_id or str(uuid.uuid4())
        self.ts: float = ts if ts is not None else time.time()
        self._acquired: List[str] = []  # resource names locked so far
        self._phase: str = "growing"    # "growing" | "shrinking"

    def __repr__(self):
        return f"Transaction(tx_id={self.tx_id!r}, ts={self.ts:.6f}, phase={self._phase})"


class LockManager:

    def __init__(self, db: redis.Redis) -> None:
        self._db = db

    def acquire(self, txn: Transaction, resource: str) -> None:
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
            granted = self._db.set(lock_key, lock_value, nx=True, px=ttl_ms)

            if granted:
                txn._acquired.append(resource)
                return

            holder_tx_id, holder_ts = _decode_lock_value(self._db.get(lock_key))

            if holder_tx_id is None:
                continue

            if holder_tx_id == txn.tx_id:
                txn._acquired.append(resource)
                return

            if txn.ts > holder_ts:
                raise WaitDieAbort(
                    f"Wait-Die: tx {txn.tx_id} (ts={txn.ts:.6f}) is younger than "
                    f"holder tx {holder_tx_id} (ts={holder_ts:.6f}) "
                    f"for resource '{resource}' — aborting."
                )

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


@contextmanager
def transaction_context(
    lock_manager: LockManager,
    resources: List[str],
    tx_id: Optional[str] = None,
    ts: Optional[float] = None,
):
    txn = Transaction(tx_id=tx_id, ts=ts)
    sorted_resources = sorted(set(resources))
    try:
        for resource in sorted_resources:
            lock_manager.acquire(txn, resource)
        yield txn
    finally:
        lock_manager.release_all(txn)