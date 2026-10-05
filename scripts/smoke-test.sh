#!/usr/bin/env bash
# End-to-end test of the full order lifecycle through the UI gateway.
# Usage: ./scripts/smoke-test.sh            (after: docker compose up --build -d)
set -euo pipefail
API=${API:-http://localhost:8080/api}

status_of() { curl -s "$API/order/orders/$1" | grep -o '"status":"[A-Z_]*"' | head -1 | cut -d'"' -f4; }
wait_for() { # orderId expectedStatus timeoutSeconds
  for _ in $(seq 1 "$3"); do
    s=$(status_of "$1"); [ "$s" = "$2" ] && { echo "   -> $2"; return 0; }; sleep 1
  done
  echo "   !! timed out waiting for $2 (last status: $s)"; exit 1
}

echo "1. Restaurants:"; curl -s "$API/restaurant/restaurants"; echo
echo "2. Surge-priced quote for N\$130:"; curl -s "$API/order/orders/quote?subtotal=130"; echo

echo "3. Placing order (customer c1, Kapana King, deliver to Wanaheda)..."
ORDER=$(curl -s -X POST "$API/order/orders" -H 'Content-Type: application/json' -d '{
  "customerId":"c1","restaurantId":"r1","deliveryAddress":"Wanaheda Ext 2","deliveryNodeId":"wanaheda",
  "items":[{"menuItemId":"m1","quantity":2},{"menuItemId":"m4","quantity":1}]}')
echo "$ORDER"
ID=$(echo "$ORDER" | grep -o '"orderId":"[^"]*"' | head -1 | cut -d'"' -f4)
[ -n "$ID" ] || { echo "Order was not created"; exit 1; }

echo "4. Payment (Kafka: orders.created -> payments.completed)"; wait_for "$ID" CONFIRMED 30
echo "5. Kitchen starts cooking";  curl -s -X PATCH "$API/restaurant/kitchen/orders/$ID/start" >/dev/null; wait_for "$ID" PREPARING 15
echo "6. Kitchen marks ready";     curl -s -X PATCH "$API/restaurant/kitchen/orders/$ID/ready" >/dev/null; wait_for "$ID" READY 15
echo "7. Driver dispatched + driving to restaurant (watch the map at http://localhost:8080)"; wait_for "$ID" OUT_FOR_DELIVERY 180
echo "8. Driving to customer"; wait_for "$ID" DELIVERED 180

echo "9. Delivery record:";       curl -s "$API/delivery/deliveries/order/$ID" | head -c 400; echo " ..."
echo "10. Customer notifications:"; curl -s "$API/notification/notifications?recipientId=c1" | grep -o '"message":"[^"]*"' | tail -8
echo "11. Admin reports:";        curl -s "$API/admin/reports/summary"; echo; curl -s "$API/admin/reports/restaurants"; echo; curl -s "$API/admin/reports/drivers"; echo

echo "12. Failure path: declined card -> CANCELLED"
BAD=$(curl -s -X POST "$API/order/orders" -H 'Content-Type: application/json' -d '{
  "customerId":"c-declined","restaurantId":"r3","deliveryAddress":"Olympia","deliveryNodeId":"olympia",
  "items":[{"menuItemId":"m8","quantity":1}]}' | grep -o '"orderId":"[^"]*"' | head -1 | cut -d'"' -f4)
wait_for "$BAD" CANCELLED 30
echo "ALL CHECKS PASSED"
