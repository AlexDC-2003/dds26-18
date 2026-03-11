import random
import time
from typing import Optional, Tuple

import redis

# Match payment service lock lifecycle and wait budget.
LOCK_TTL_SECONDS: float = 30.0
WAIT_TIMEOUT_SECONDS: float = 10.0
WAIT_POLL_INTERVAL_SECONDS: float = 0.05

# Random back-off range after a wait-die abort (desynchronises competing txs).
RETRY_BACKOFF_MIN_SECONDS: float = 0.1
RETRY_BACKOFF_MAX_SECONDS: float = 0.5

# Max retries in kafka_infra before giving up and sending an error reply.
MAX_RETRIES: int = 5

_SEP = "|"
_redis_client: Optional[redis.Redis] = None


def set_redis_client(client) -> None:
    global _redis_client
    _redis_client = client


def _encode_lock_value(tx_id: str, ts: float) -> str:
    return f"{tx_id}{_SEP}{ts}"


def _decode_lock_value(raw: Optional[object]) -> Tuple[Optional[str], Optional[float]]:
    if raw is None:
        return None, None

    if isinstance(raw, bytes):
        text = raw.decode("utf-8")
    else:
        text = str(raw)

    # Backward-compatible with legacy lock values that stored only tx_id.
    if _SEP not in text:
        return text, None

    tx_id, ts_str = text.split(_SEP, 1)
    try:
        return tx_id, float(ts_str)
    except Exception:
        return tx_id, None


class WaitDieAbort(Exception):
    pass


class LockTimeout(Exception):
    pass


# Backward-compatible alias used by existing retry logic.
LockDeadlockAbort = WaitDieAbort


def acquire_lock(item_id: str, tx_id: str, *, tx_ts: Optional[float] = None) -> None:
    if _redis_client is None:
        raise RuntimeError("Redis client is not configured for lock manager")

    requester_ts = time.time()
    lock_key = f"lock:item:{item_id}"
    lock_value = _encode_lock_value(tx_id, requester_ts)
    ttl_ms = int(LOCK_TTL_SECONDS * 1000)
    deadline = time.monotonic() + WAIT_TIMEOUT_SECONDS

    while True:
        requester_ts = time.time()

        acquired = _redis_client.set(lock_key, lock_value, nx=True, px=ttl_ms)
        if acquired:
            return True, None
        # else:
        #     return False, None

        holder_tx_id, holder_ts = _decode_lock_value(_redis_client.get(lock_key))
        if holder_tx_id is None:
            # Holder expired between SET NX and GET.
            continue

        if holder_tx_id == tx_id:
            # Re-entrant acquire by same transaction.
            return False, 'the same timestamp'
        # print(f'[2PL] Lock on item {item_id} is held by tx {holder_tx_id} (ts={holder_ts:.6f}), requester tx {tx_id} (ts={requester_ts:.6f})')
        # Wait-Die:
        # younger requester dies, older requester waits.
        if holder_ts is not None and requester_ts > holder_ts:
            # r
            return False, f"wait-die abort: tx {tx_id} (ts={requester_ts:.6f}) is younger than holder tx {holder_tx_id} (ts={holder_ts:.6f}) for item {item_id}"

        if time.monotonic() >= deadline:
            raise LockTimeout(
                f"Timed out waiting for lock on item '{item_id}' "
                f"(tx_id={tx_id}, waited={WAIT_TIMEOUT_SECONDS}s)"
                f"{requester_ts}, holder_ts={holder_ts})"
            )

        time.sleep(WAIT_POLL_INTERVAL_SECONDS)


def release_lock(item_id: str, tx_id: str) -> None:
    if _redis_client is None:
        raise RuntimeError("Redis client is not configured for lock manager")

    lock_key = f"lock:item:{item_id}"
    holder_tx_id, _ = _decode_lock_value(_redis_client.get(lock_key))
    if holder_tx_id == tx_id:
        _redis_client.delete(lock_key)


def random_backoff() -> float:
    return random.uniform(RETRY_BACKOFF_MIN_SECONDS, RETRY_BACKOFF_MAX_SECONDS)
