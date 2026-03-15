"""
payment/lock_manager.py

2PL with Wait-then-Die deadlock prevention.
Lock state stored in Redis to work for multiple service replicas.
Redis key schema: lock:<resource> -> String "<tx_id>|<tx_ts>" (set with NX + PX expiry)
Every transactionfirst waits by polling. If lock not acquired until WAIT_TIMEOUT_SECONDS, tx dies (WaitDieAbort exception).
"""

import logging
import random
import time
import uuid
import redis
from contextlib import contextmanager
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

LOCK_TTL_SECONDS = 30       # How long lock is held before auto-expire.
WAIT_TIMEOUT_SECONDS = 10   # How long transaction waits before dying.
# Sleep between polling attempts while waiting for a lock.
WAIT_POLL_INTERVAL_MIN = 0.05
WAIT_POLL_INTERVAL_MAX = 0.1
_SEP = "|"  

def _encode_lock_value(tx_id: str, ts: float) -> str:
    """Encode owner info as string stored in Redis."""
    return f"{tx_id}{_SEP}{ts}"


def _decode_lock_value(raw: Optional[bytes]) -> Tuple[Optional[str], Optional[float]]:
    """Decode lock value str into (tx_id, ts)."""
    if raw is None:
        return None, None
    try:
        text = raw.decode("utf-8")
        tx_id, ts_str = text.split(_SEP, 1)
        return tx_id, float(ts_str)
    except Exception:
        return None, None

class WaitDieAbort(Exception):
    """Transaction has waited too long and dies."""
    pass

class LockTimeout(Exception):
    """Raised when a transaction has been waiting too long for a lock."""
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
    """
    For write locks identified by string resource names.
    All lock state lives in Redis, so multiple service replicas share it.
    Acquisition: SET lock:<resource> "<tx_id>|<ts>" NX PX <ttl_ms>
    Release: GET  → check we still own it / DEL  → delete only if we do
    """

    def __init__(self, db: redis.Redis) -> None:
        self._db = db

    def acquire(self, txn: Transaction, resource: str) -> None:
        """Acquire an exclusive lock on resource for txn. Wait-then-Die strategy:
        - Polls until lock is free or wait is exhausted.
        - Raises WaitDieAbort if WAIT_TIMEOUT_SECONDS elapses.
        - Raises RuntimeError if called after shrinking phase begun.
        """
        if txn._phase == "shrinking":
            raise RuntimeError(
                f"2PL violation: cannot acquire a lock during the shrinking phase "
                f"(tx_id={txn.tx_id}, resource={resource})")

        lock_key = f"lock:{resource}"
        lock_value = _encode_lock_value(txn.tx_id, txn.ts)
        ttl_ms = int(LOCK_TTL_SECONDS * 1000)
        deadline = time.monotonic() + WAIT_TIMEOUT_SECONDS

        while True:
            granted = self._db.set(lock_key, lock_value, nx=True, px=ttl_ms)
            if granted:
                txn._acquired.append(resource) # attempt atomic acquisition
                return

            holder_tx_id, holder_ts = _decode_lock_value(self._db.get(lock_key))  # lock held, read current owner
            if holder_tx_id is None: # lock expired between SET NX and GET - retry immediately.
                continue 

            if holder_tx_id == txn.tx_id: # Reentrant: already hold this lock.
                txn._acquired.append(resource) 
                return
            
            # raises WaitDieAbort for everyone once budget is gone.
            if time.monotonic() > deadline:
                raise WaitDieAbort(
                    f"Wait-then-Die: tx {txn.tx_id} could not acquire lock on "
                    f"'{resource}' within {WAIT_TIMEOUT_SECONDS}s - dying."
                )
            time.sleep(random.uniform(WAIT_POLL_INTERVAL_MIN, WAIT_POLL_INTERVAL_MAX))

    def release_all(self, txn: Transaction) -> None:
        """Enter the shrinking phase and release all locks held by txn."""
        txn._phase = "shrinking"
        for resource in reversed(txn._acquired):
            self._release_one(txn, resource)
        txn._acquired.clear()

    def _release_one(self, txn: Transaction, resource: str) -> None:
        """Release lock on resource, if we still own it."""
        lock_key = f"lock:{resource}"
        raw = self._db.get(lock_key)
        holder_tx_id, _ = _decode_lock_value(raw)
        if holder_tx_id == txn.tx_id:
            self._db.delete(lock_key)

@contextmanager
def transaction_context(lock_manager: LockManager, resources: List[str], tx_id: Optional[str] = None, ts: Optional[float] = None):
    """
    Creates a Transaction with given tx_id / ts.
        Acquires exclusive locks on all resources in sorted order.
        Yields the transaction to the caller.
        Always releases all locks on exit, even if exception is raised.
        Raises WaitDieAbort if wait is exhausted for any resource.
    """
    txn = Transaction(tx_id=tx_id, ts=ts)
    sorted_resources = sorted(set(resources))
    try:
        for resource in sorted_resources:
            lock_manager.acquire(txn, resource)
        yield txn
    finally:
        lock_manager.release_all(txn)