"""
Manual kill test: populate → slow rolling checkouts → consistency check.

Usage:
    python test/kill_test.py

Gives you ~60s of checkout load to manually kill/recover containers one at a time.
Kill a container with: docker kill <name>
Docker watchdog will restart it automatically.

Consistency check at the end verifies no stock or credit was lost.
"""

import asyncio
import aiohttp
import random
import time

BASE_URL = "http://localhost:8000"
TIMEOUT = aiohttp.ClientTimeout(total=15)

# Same scale as the benchmark consistency test
NUM_ITEMS    = 1
ITEM_STOCK   = 1000
ITEM_PRICE   = 1
NUM_USERS    = 1000
USER_CREDIT  = 1

# How long to keep firing checkouts (use this window to kill services)
LOAD_DURATION_SEC = 100


async def post(session, path):
    async with session.post(f"{BASE_URL}{path}", timeout=TIMEOUT) as r:
        try:
            return r.status, await r.json()
        except Exception:
            return r.status, {}

async def get(session, path, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(f"{BASE_URL}{path}", timeout=TIMEOUT) as r:
                try:
                    return r.status, await r.json()
                except Exception:
                    return r.status, {}
        except (aiohttp.ServerDisconnectedError, aiohttp.ClientConnectionError):
            if attempt == retries - 1:
                return 503, {}
            await asyncio.sleep(0.5)


async def setup(session):
    print("Setting up...")
    item_ids, user_ids, order_ids = [], [], []

    for _ in range(NUM_ITEMS):
        sc, j = await post(session, f"/stock/item/create/{ITEM_PRICE}")
        iid = j["item_id"]
        await post(session, f"/stock/add/{iid}/{ITEM_STOCK}")
        item_ids.append(iid)

    tasks = []
    for _ in range(NUM_USERS):
        tasks.append(post(session, "/payment/create_user"))
    results = await asyncio.gather(*tasks)
    for sc, j in results:
        uid = j["user_id"]
        await post(session, f"/payment/add_funds/{uid}/{USER_CREDIT}")
        user_ids.append(uid)

    for uid in user_ids:
        sc, j = await post(session, f"/orders/create/{uid}")
        oid = j["order_id"]
        iid = random.choice(item_ids)
        await post(session, f"/orders/addItem/{oid}/{iid}/1")
        order_ids.append(oid)

    print(f"  {NUM_ITEMS} items, {NUM_USERS} users, {len(order_ids)} orders ready.")
    return item_ids, user_ids, order_ids


async def run_load(session, order_ids):
    print(f"\nFiring checkouts for {LOAD_DURATION_SEC}s — kill services now!")
    print("  docker kill dds26-18-stock-service-1  (then wait for recovery)")
    print("  docker kill dds26-18-payment-service-1")
    print("  docker kill dds26-18-order-service-1\n")

    deadline = time.time() + LOAD_DURATION_SEC
    ok = fail = errors = 0

    while time.time() < deadline:
        batch = random.sample(order_ids, min(50, len(order_ids)))
        tasks = [post(session, f"/orders/checkout/{oid}") for oid in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                errors += 1
            else:
                sc, _ = r
                if 200 <= sc < 300:
                    ok += 1
                elif 400 <= sc < 500:
                    fail += 1
                else:
                    errors += 1
        remaining = int(deadline - time.time())
        print(f"  {remaining:3d}s left | ok={ok} fail={fail} errors={errors}", end="\r")
        await asyncio.sleep(1)

    print(f"\n  Done. ok={ok} fail={fail} errors={errors}")


async def consistency_check(session, item_ids, user_ids, order_ids):
    print("\nConsistency check...")
    sem = asyncio.Semaphore(50)

    async def bounded_get(path):
        async with sem:
            return await get(session, path)

    stocks  = await asyncio.gather(*[bounded_get(f"/stock/find/{iid}") for iid in item_ids])
    credits = await asyncio.gather(*[bounded_get(f"/payment/find_user/{uid}") for uid in user_ids])
    orders  = await asyncio.gather(*[bounded_get(f"/orders/find/{oid}") for oid in order_ids])

    remaining_stock = sum(j.get("stock", 0) for _, j in stocks)
    remaining_credit = sum(j.get("credit", 0) for _, j in credits)
    paid_count = sum(1 for _, j in orders if j.get("paid"))
    paid_cost = sum(j.get("total_cost", ITEM_PRICE) for _, j in orders if j.get("paid"))

    initial_stock = NUM_ITEMS * ITEM_STOCK
    initial_credit = NUM_USERS * USER_CREDIT

    print(f"  remaining_stock = {remaining_stock}")
    print(f"  paid_orders     = {paid_count}  (cost = {paid_cost})")
    print(f"  stock_check : {remaining_stock} + {paid_count} = {remaining_stock + paid_count}  (expected {initial_stock})")
    print(f"  credit_check: {initial_credit} - {paid_cost} = {initial_credit - paid_cost}  (actual {remaining_credit})")

    stock_ok = (remaining_stock + paid_count) == initial_stock
    credit_ok = abs((initial_credit - paid_cost) - remaining_credit) < 0.01

    print()
    if stock_ok and credit_ok:
        print("PASS")
    else:
        if not stock_ok:
            print("FAIL — stock inconsistency")
        if not credit_ok:
            print("FAIL — credit inconsistency")


async def main():
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        item_ids, user_ids, order_ids = await setup(session)
        await run_load(session, order_ids)
        print("\nWaiting 30s for in-flight transactions to settle...")
        await asyncio.sleep(30)
        await consistency_check(session, item_ids, user_ids, order_ids)


if __name__ == "__main__":
    asyncio.run(main())
