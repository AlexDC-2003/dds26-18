import time
import random
import redis

# How long a lock is held before Redis auto-expires it (crashed holder safety net)
LOCK_TTL_SECONDS: float = 10.0

# How long to poll before assuming deadlock and aborting
LOCK_ACQUIRE_TIMEOUT_SECONDS: float = 2.0

# How often to retry acquiring the lock while waiting
LOCK_POLL_INTERVAL_SECONDS: float = 0.05

# Random back-off range after a deadlock abort (desynchronises competing txs)
RETRY_BACKOFF_MIN_SECONDS: float = 0.1
RETRY_BACKOFF_MAX_SECONDS: float = 0.5

# Max retries in kafka_infra before giving up and sending an error reply
MAX_RETRIES: int = 5

_redis_client = None


def set_redis_client(client) -> None:
    global _redis_client
    _redis_client = client


class LockDeadlockAbort(Exception):
    pass


def acquire_lock(item_id: str, tx_id: str) -> None:
    deadline = time.monotonic() + LOCK_ACQUIRE_TIMEOUT_SECONDS
    lock_key = f"lock:item:{item_id}"
    ttl_ms = int(LOCK_TTL_SECONDS * 1000)

    while True:
        # SET NX PX is a single atomic Redis command — no Lua needed
        acquired = _redis_client.set(lock_key, tx_id, nx=True, px=ttl_ms)
        if acquired:
            return

        # Re-entrant: if we already hold the lock, no-op
        holder = _redis_client.get(lock_key)
        if holder == tx_id:
            return

        if time.monotonic() >= deadline:
            raise LockDeadlockAbort(
                f"tx {tx_id} could not acquire lock on item {item_id} "
                f"within {LOCK_ACQUIRE_TIMEOUT_SECONDS}s — assuming deadlock"
            )

        time.sleep(LOCK_POLL_INTERVAL_SECONDS)


def release_lock(item_id: str, tx_id: str) -> None:
    lock_key = f"lock:item:{item_id}"
    holder = _redis_client.get(lock_key)
    if holder == tx_id:
        _redis_client.delete(lock_key)


def random_backoff() -> float:
    return random.uniform(RETRY_BACKOFF_MIN_SECONDS, RETRY_BACKOFF_MAX_SECONDS)
