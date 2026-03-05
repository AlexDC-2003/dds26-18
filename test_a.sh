#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

echo "== Creating user =="
USER_ID=$(curl -s -X POST "$BASE_URL/payment/create_user" | python3 -c 'import sys,json; print(json.load(sys.stdin)["user_id"])')
echo "USER_ID=$USER_ID"

echo "== Adding funds =="
curl -s -X POST "$BASE_URL/payment/add_funds/$USER_ID/100" >/dev/null
echo "Funds added."

echo "== Creating item (price=1) =="
ITEM_ID=$(curl -s -X POST "$BASE_URL/stock/item/create/1" | python3 -c 'import sys,json; print(json.load(sys.stdin)["item_id"])')
echo "ITEM_ID=$ITEM_ID"

echo "== Adding stock (10) =="
curl -s -X POST "$BASE_URL/stock/add/$ITEM_ID/10" >/dev/null
echo "Stock added."

echo "== Creating order =="
ORDER_ID=$(curl -s -X POST "$BASE_URL/orders/create/$USER_ID" | python3 -c 'import sys,json; print(json.load(sys.stdin)["order_id"])')
echo "ORDER_ID=$ORDER_ID"

echo "== Adding item qty=3 to order =="
curl -s -X POST "$BASE_URL/orders/addItem/$ORDER_ID/$ITEM_ID/3" >/dev/null
echo "Item added."

echo "== Checkout (1st time) =="
curl -i -s -X POST "$BASE_URL/orders/checkout/$ORDER_ID" | sed -n '1,8p'
echo

echo "== Checkout (2nd time, should be idempotent) =="
curl -i -s -X POST "$BASE_URL/orders/checkout/$ORDER_ID" | sed -n '1,8p'
echo

echo "== Verify order =="
curl -s -X GET "$BASE_URL/orders/find/$ORDER_ID"
echo
echo

echo "== Verify stock (expect 7) =="
curl -s -X GET "$BASE_URL/stock/find/$ITEM_ID"
echo
echo

echo "== Verify user credit (expect 97) =="
curl -s -X GET "$BASE_URL/payment/find_user/$USER_ID"
echo
echo

echo "Done."
