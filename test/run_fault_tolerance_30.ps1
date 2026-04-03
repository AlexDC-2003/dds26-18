$passes = 0
$failures = 0

for ($i = 1; $i -le 30; $i++) {
    Write-Host "`n=== Run $i / 30 ===" -ForegroundColor Cyan
    python "$PSScriptRoot\fault_tolerance_test.py"
    if ($LASTEXITCODE -eq 0) {
        $passes++
        Write-Host "PASS" -ForegroundColor Green
    } else {
        $failures++
        Write-Host "FAIL (exit $LASTEXITCODE)" -ForegroundColor Red
    }
}

Write-Host "`n=== Results: $passes / 30 passed, $failures failed ===" -ForegroundColor $(if ($failures -eq 0) { "Green" } else { "Yellow" })
