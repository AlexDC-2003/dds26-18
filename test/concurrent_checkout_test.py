"""
Simple concurrent checkout test: 2 users, 1 item with stock=1.
Both users race to checkout the same item — exactly one should succeed.

Usage:
    pip install aiohttp
    python test/concurrent_checkout_test.py

Docker must be running and `docker compose up` must already be running.
"""

import asyncio
import aiohttp

BASE_URL = "http://localhost:8000"
TIMEOUT = aiohttp.ClientTimeout(total=20)

ITEM_PRICE  = 10
USER_CREDIT = 100


async def _parse(r):
    ct = r.content_type or ""
    body = await r.json() if "json" in ct else await r.text()
    return r.status, body

async def apost(session, path):
    async with session.post(f"{BASE_URL}{path}", timeout=TIMEOUT) as r:
        return await _parse(r)

async def aget(session, path):
    async with session.get(f"{BASE_URL}{path}", timeout=TIMEOUT) as r:
        return await _parse(r)


async def setup(session):
    # Create item with stock = 1
    sc, j = await apost(session, f"/stock/item/create/{ITEM_PRICE}")
    item_id = j["item_id"]
    await apost(session, f"/stock/add/{item_id}/1")
    print(f"  item_id={item_id}  stock=1  price={ITEM_PRICE}")

    # Create 2 users with enough credit
    user_ids = []
    for i in range(2):
        sc, j = await apost(session, "/payment/create_user")
        uid = j["user_id"]
        await apost(session, f"/payment/add_funds/{uid}/{USER_CREDIT}")
        user_ids.append(uid)
    print(f"  user_ids={user_ids}  credit={USER_CREDIT} each")

    # Create one order per user, each containing the same item
    order_ids = []
    for uid in user_ids:
        sc, j = await apost(session, f"/orders/create/{uid}")
        oid = j["order_id"]
        await apost(session, f"/orders/addItem/{oid}/{item_id}/1")
        order_ids.append(oid)
    print(f"  order_ids={order_ids}")

    return item_id, user_ids, order_ids


async def checkout_concurrently(session, order_ids):
    """Fire both checkouts at the exact same time."""
    async def do_checkout(oid):
        sc, body = await apost(session, f"/orders/checkout/{oid}")
        return oid, sc, body

    results = await asyncio.gather(
        do_checkout(order_ids[0]),
        do_checkout(order_ids[1]),
    )
    return results


async def main():
    print("=" * 50)
    print("Concurrent Checkout Test (2 users, 1 item)")
    print("=" * 50)

    async with aiohttp.ClientSession() as session:

        # Setup
        print("\n[Setup]")
        item_id, user_ids, order_ids = await setup(session)

        # Concurrent checkout
        print("\n[Test] Both users checkout concurrently...")
        results = await checkout_concurrently(session, order_ids)

        success_count = 0
        failure_count = 0
        for oid, sc, body in results:
            status = "OK" if 200 <= sc < 300 else "FAIL"
            if 200 <= sc < 300:
                success_count += 1
            else:
                failure_count += 1
            print(f"  order={oid}  status={sc} ({status})  body={body}")

        # Verify final state
        print("\n[Check] Final state:")
        _, stock_info = await aget(session, f"/stock/find/{item_id}")
        remaining_stock = stock_info["stock"]
        print(f"  remaining stock = {remaining_stock}  (expected 0)")

        credits = []
        for uid in user_ids:
            _, user_info = await aget(session, f"/payment/find_user/{uid}")
            credits.append(user_info["credit"])
        print(f"  user credits = {credits}")
        total_credit = sum(credits)
        expected_total_credit = USER_CREDIT * 2 - ITEM_PRICE  # one paid
        print(f"  total credit = {total_credit}  (expected {expected_total_credit})")

        # Assertions
        print()
        passed = True

        if success_count != 1:
            print(f"FAIL — expected exactly 1 successful checkout, got {success_count}")
            passed = False

        if failure_count != 1:
            print(f"FAIL — expected exactly 1 failed checkout, got {failure_count}")
            passed = False

        if remaining_stock != 0:
            print(f"FAIL — stock should be 0, got {remaining_stock}")
            passed = False

        if total_credit != expected_total_credit:
            print(f"FAIL — total credit should be {expected_total_credit}, got {total_credit}")
            passed = False

        if passed:
            print("PASS — exactly one user acquired the item, no stock or money lost.")


if __name__ == "__main__":
    asyncio.run(main())
