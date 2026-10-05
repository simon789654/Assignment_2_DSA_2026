# End-to-end test of the full order lifecycle (Windows PowerShell).
# Usage:  powershell -ExecutionPolicy Bypass -File scripts\smoke-test.ps1
$ErrorActionPreference = "Stop"
$API = "http://localhost:8080/api"

function Wait-Status($id, $expected, $timeout) {
    for ($i = 0; $i -lt $timeout; $i++) {
        $s = (Invoke-RestMethod "$API/order/orders/$id").status
        if ($s -eq $expected) { Write-Host "   -> $expected" -ForegroundColor Green; return }
        Start-Sleep 1
    }
    throw "Timed out waiting for $expected (last status: $s)"
}

Write-Host "1. Restaurants:"; Invoke-RestMethod "$API/restaurant/restaurants" | Format-Table restaurantId, name, nodeId, isOpen
Write-Host "2. Surge quote:"; Invoke-RestMethod "$API/order/orders/quote?subtotal=130" | Format-List

Write-Host "3. Placing order..."
$body = @{ customerId = "c1"; restaurantId = "r1"; deliveryAddress = "Wanaheda Ext 2"; deliveryNodeId = "wanaheda";
           items = @(@{ menuItemId = "m1"; quantity = 2 }, @{ menuItemId = "m4"; quantity = 1 }) } | ConvertTo-Json -Depth 5
$order = Invoke-RestMethod -Method Post "$API/order/orders" -ContentType "application/json" -Body $body
$id = $order.orderId; Write-Host "   orderId $id  total N`$$($order.total)  surge x$($order.surgeMultiplier)"

Write-Host "4. Payment";        Wait-Status $id "CONFIRMED" 30
Write-Host "5. Start cooking";  Invoke-RestMethod -Method Patch "$API/restaurant/kitchen/orders/$id/start" | Out-Null; Wait-Status $id "PREPARING" 15
Write-Host "6. Ready";          Invoke-RestMethod -Method Patch "$API/restaurant/kitchen/orders/$id/ready" | Out-Null; Wait-Status $id "READY" 15
Write-Host "7. Driver en route to restaurant (open http://localhost:8080)"; Wait-Status $id "OUT_FOR_DELIVERY" 180
Write-Host "8. Driving to customer"; Wait-Status $id "DELIVERED" 180

Write-Host "9. Notifications:"; Invoke-RestMethod "$API/notification/notifications?recipientId=c1" | Select-Object -Last 8 | Format-Table channel, message
Write-Host "10. Reports:"; Invoke-RestMethod "$API/admin/reports/restaurants" | Format-Table; Invoke-RestMethod "$API/admin/reports/drivers" | Format-Table

Write-Host "11. Declined card -> CANCELLED"
$bad = @{ customerId = "c-declined"; restaurantId = "r3"; deliveryAddress = "Olympia"; deliveryNodeId = "olympia";
          items = @(@{ menuItemId = "m8"; quantity = 1 }) } | ConvertTo-Json -Depth 5
$b = Invoke-RestMethod -Method Post "$API/order/orders" -ContentType "application/json" -Body $bad
Wait-Status $b.orderId "CANCELLED" 30
Write-Host "ALL CHECKS PASSED" -ForegroundColor Green
