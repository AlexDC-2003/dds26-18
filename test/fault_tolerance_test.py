"""
Fault tolerance + high-load test for the WDM microservices.

Usage:
    pip install aiohttp
    python test/fault_tolerance_test.py

Docker must be running and `docker compose up` must already be running.

NOTE: To hit 10k checkout RPS the services need more workers.
With default single-worker gunicorn on stock/payment the ceiling is ~100-300 checkout/s.
Scale via docker-compose: gunicorn -w 4 (or more) on stock/payment services.
"""

import asyncio
import aiohttp
import subprocess
import sys
import time
import threading

BASE_URL = "http://127.0.0.1:8000"

# ── scale parameters ──────────────────────────────────────────────────────────
NUM_ITEMS        = 20
ITEM_STOCK       = 1000    # 20k total units
ITEM_PRICE       = 10
NUM_USERS        = 500
USER_CREDIT      = 500
ORDERS_PER_USER  = 4       # 2000 orders total

CHECKOUT_SEM     = 500     # concurrent in-flight checkout requests
LOAD_EXTRA_SEC   = 20      # extra seconds of checkout load after kills finish
CHECKOUT_RETRIES = 15      # retry attempts for unpaid orders in Phase 3
CHECKOUT_RETRY_WAIT = 3    # seconds between Phase 3 retries

CONTAINERS = [
    "dds26-18-stock-service-1",
    "dds26-18-payment-service-1",
    "dds26-18-order-service-1",
    "dds26-18-stock-db-1",
    "dds26-18-payment-db-1",
    "dds26-18-order-db-1",
]
KILL_INTERVAL_SEC = 3


# ── async HTTP helpers ────────────────────────────────────────────────────────

TIMEOUT = aiohttp.ClientTimeout(total=20)

async def _parse(r):
    ct = r.content_type or ""
    body = await r.json() if "json" in ct else await r.text()
    return r.status, body

async def apost(session, path, retries=3):
    for attempt in range(retries):
        try:
            async with session.post(f"{BASE_URL}{path}", timeout=TIMEOUT) as r:
                return await _parse(r)
        except (aiohttp.ServerDisconnectedError, aiohttp.ClientConnectionError):
            if attempt == retries - 1:
                raise
            await asyncio.sleep(0.5)

async def aget(session, path, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(f"{BASE_URL}{path}", timeout=TIMEOUT) as r:
                return await _parse(r)
        except (aiohttp.ServerDisconnectedError, aiohttp.ClientConnectionError):
            if attempt == retries - 1:
                raise
            await asyncio.sleep(0.5)


# ── setup (fully async, high concurrency) ────────────────────────────────────

async def setup(session):
    sem = asyncio.Semaphore(100)

    async def create_item():
        async with sem:
            sc, j = await apost(session, f"/stock/item/create/{ITEM_PRICE}")
            item_id = j["item_id"]
            await apost(session, f"/stock/add/{item_id}/{ITEM_STOCK}")
            return item_id

    async def create_user():
        async with sem:
            sc, j = await apost(session, "/payment/create_user")
            uid = j["user_id"]
            await apost(session, f"/payment/add_funds/{uid}/{USER_CREDIT}")
            return uid

    import random
    print(f"  Creating {NUM_ITEMS} items...")
    item_ids = await asyncio.gather(*[create_item() for _ in range(NUM_ITEMS)])

    print(f"  Creating {NUM_USERS} users...")
    user_ids = await asyncio.gather(*[create_user() for _ in range(NUM_USERS)])

    print(f"  Creating {NUM_USERS * ORDERS_PER_USER} orders...")

    async def create_order(uid):
        async with sem:
            sc, j = await apost(session, f"/orders/create/{uid}")
            oid = j["order_id"]
            iid = random.choice(item_ids)
            await apost(session, f"/orders/addItem/{oid}/{iid}/1")
            return oid

    all_uids = [uid for uid in user_ids for _ in range(ORDERS_PER_USER)]
    order_ids = await asyncio.gather(*[create_order(uid) for uid in all_uids])

    return list(item_ids), list(user_ids), list(order_ids)


# ── fault injection (runs in a background thread) ─────────────────────────────

def fault_injector(kill_done_event):
    for name in CONTAINERS:
        result = subprocess.run(["docker", "kill", name], capture_output=True, text=True)
        status = "killed" if result.returncode == 0 else "not found"
        print(f"  [KILL] {name} ({status})")
        time.sleep(KILL_INTERVAL_SEC)
    kill_done_event.set()
    print("  [KILLS] Done.")


# ── checkout workers ──────────────────────────────────────────────────────────

async def checkout_worker(session, order_id, sem, results, counters, stop_event):
    """Retry checkout until success, definitive 4xx, or stop_event."""
    while not stop_event.is_set():
        async with sem:
            try:
                async with session.post(
                    f"{BASE_URL}/orders/checkout/{order_id}",
                    timeout=TIMEOUT
                ) as r:
                    counters["reqs"] += 1
                    sc = r.status
                    if 200 <= sc < 300:
                        counters["ok"] += 1
                        results[order_id] = True
                        return
                    if 400 <= sc < 500:
                        counters["fail"] += 1
                        results[order_id] = False
                        return
                    # 5xx / unexpected → retry
            except Exception:
                counters["reqs"] += 1
        await asyncio.sleep(0.2)
    results[order_id] = False


async def rps_reporter(counters, stop_event):
    prev_reqs = 0
    prev_ok   = 0
    t0 = time.time()
    while not stop_event.is_set():
        await asyncio.sleep(1)
        r = counters["reqs"]
        ok = counters["ok"]
        elapsed = time.time() - t0
        print(f"  t={elapsed:5.1f}s | RPS: {r - prev_reqs:5d} req/s  "
              f"| checkout ok: {ok - prev_ok:4d}/s  | total ok: {ok}")
        prev_reqs = r
        prev_ok   = ok


# ── recovery polling ──────────────────────────────────────────────────────────

async def wait_for_recovery(session, item_ids, user_ids, timeout=120):
    print("\nWaiting for all services to recover...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            await aget(session, f"/stock/find/{item_ids[0]}")
            await aget(session, f"/payment/find_user/{user_ids[0]}")
            print("  Services healthy.")
            return True
        except Exception:
            await asyncio.sleep(2)
    return False


# ── consistency check ─────────────────────────────────────────────────────────

async def consistency_check(session, item_ids, user_ids, order_ids,
                             initial_stock_total, initial_credit_total):
    sem = asyncio.Semaphore(50)

    async def get_stock(iid):
        async with sem:
            sc, j = await aget(session, f"/stock/find/{iid}")
            return j["stock"]

    async def get_credit(uid):
        async with sem:
            sc, j = await aget(session, f"/payment/find_user/{uid}")
            return j["credit"]

    async def get_order(oid):
        async with sem:
            sc, j = await aget(session, f"/orders/find/{oid}")
            return j

    stocks  = await asyncio.gather(*[get_stock(iid) for iid in item_ids])
    credits = await asyncio.gather(*[get_credit(uid) for uid in user_ids])
    orders  = await asyncio.gather(*[get_order(oid) for oid in order_ids])

    remaining_stock   = sum(stocks)
    current_credit    = sum(credits)
    paid_count        = sum(1 for o in orders if o.get("paid"))
    paid_cost_total   = sum(o.get("total_cost", ITEM_PRICE) for o in orders if o.get("paid"))

    print(f"  remaining_stock_total = {remaining_stock}")
    print(f"  paid_orders           = {paid_count}  (total cost = {paid_cost_total})")
    print(f"  stock_check  : {remaining_stock} + {paid_count} = {remaining_stock + paid_count}  (expected {initial_stock_total})")
    print(f"  credit_check : {initial_credit_total} - {paid_cost_total} = "
          f"{initial_credit_total - paid_cost_total}  (actual {current_credit})")

    stock_ok  = (remaining_stock + paid_count) == initial_stock_total
    credit_ok = abs((initial_credit_total - paid_cost_total) - current_credit) < 0.01

    return stock_ok, credit_ok


# ── main ──────────────────────────────────────────────────────────────────────

async def main():
    print("=" * 60)
    print("Fault Tolerance Test (High Load, Async)")
    print("=" * 60)

    connector = aiohttp.TCPConnector(limit=0)  # no connection cap
    async with aiohttp.ClientSession(connector=connector) as session:

        # ── Phase 1: Setup ───────────────────────────────────────
        print("\n[Phase 1] Setting up test data...")
        item_ids, user_ids, order_ids = await setup(session)

        initial_stock_total  = NUM_ITEMS * ITEM_STOCK

        # Measure actual initial credit from DB (add_funds may silently fail)
        _sem = asyncio.Semaphore(50)
        async def _get_credit(uid):
            async with _sem:
                sc, j = await aget(session, f"/payment/find_user/{uid}")
                return j["credit"]
        _credits = await asyncio.gather(*[_get_credit(uid) for uid in user_ids])
        initial_credit_total = sum(_credits)

        print(f"  orders={len(order_ids)}  total_stock={initial_stock_total}"
              f"  total_credit={initial_credit_total}"
              f"  (expected {NUM_USERS * USER_CREDIT})")

        # ── Phase 2: Load + fault injection ──────────────────────
        print(f"\n[Phase 2] Killing containers immediately + running load...")
        print(f"  {CHECKOUT_SEM} concurrent checkout slots | "
              f"{len(CONTAINERS)} kills × {KILL_INTERVAL_SEC}s chaos\n")

        kill_done = threading.Event()
        stop_event = asyncio.Event()
        counters = {"reqs": 0, "ok": 0, "fail": 0}
        results  = {}

        killer = threading.Thread(target=fault_injector, args=(kill_done,), daemon=True)
        killer.start()

        sem = asyncio.Semaphore(CHECKOUT_SEM)
        workers = [
            asyncio.create_task(checkout_worker(session, oid, sem, results, counters, stop_event))
            for oid in order_ids
        ]
        reporter_task = asyncio.create_task(rps_reporter(counters, stop_event))

        # Wait for kills to finish
        await asyncio.get_event_loop().run_in_executor(None, kill_done.wait)

        # Wait for recovery
        recovered = await wait_for_recovery(session, item_ids, user_ids)
        if not recovered:
            stop_event.set()
            print("\nFAIL — services never recovered.")
            sys.exit(1)

        # Let load run a bit more after recovery, then drain
        print(f"\n  Continuing load for {LOAD_EXTRA_SEC}s post-recovery...\n")
        await asyncio.sleep(LOAD_EXTRA_SEC)
        stop_event.set()
        reporter_task.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

        total_ok   = counters["ok"]
        total_fail = counters["fail"]
        total_reqs = counters["reqs"]
        print(f"\n  Total requests  : {total_reqs}")
        print(f"  Checkouts ok    : {total_ok}")
        print(f"  Checkout 4xx    : {total_fail}  (out of stock / no credit — expected)")

        # ── Phase 3: Retry all unpaid orders + consistency check ─
        print("\n[Phase 3] Retrying all unpaid orders...")

        # Fetch current order state — any order that shows paid=False needs a retry.
        # This catches orders where checkout returned 200 but the order.paid write
        # was lost in the Redis AOF fsync window (everysec), leaving the charge
        # orphaned in payment-db without a corresponding paid order.
        fetch_sem = asyncio.Semaphore(50)
        async def fetch_paid(oid):
            async with fetch_sem:
                sc, j = await aget(session, f"/orders/find/{oid}")
            return oid, j.get("paid", False)

        paid_states = dict(await asyncio.gather(*[fetch_paid(oid) for oid in order_ids]))
        unpaid = [oid for oid, paid in paid_states.items() if not paid]
        print(f"  {len(unpaid)} orders still unpaid — retrying...")

        for oid in unpaid:
            for _ in range(CHECKOUT_RETRIES):
                sc, _ = await apost(session, f"/orders/checkout/{oid}")
                if 200 <= sc < 300:
                    break
                if 400 <= sc < 500:
                    break
                await asyncio.sleep(CHECKOUT_RETRY_WAIT)

        print("\n[Phase 3] Consistency Check...")
        stock_ok, credit_ok = await consistency_check(
            session, item_ids, user_ids, order_ids,
            initial_stock_total, initial_credit_total
        )

    print()
    if stock_ok and credit_ok:
        print("PASS — no stock or money lost.")
    else:
        if not stock_ok:
            print("FAIL — stock inconsistency detected.")
        if not credit_ok:
            print("FAIL — credit inconsistency detected.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
