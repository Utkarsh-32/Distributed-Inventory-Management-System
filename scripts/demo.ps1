# Walk through the project for a demo:
#   1. warehouse stock, orders, supplier restocks and delivery updates
#   2. leader failure: stop the leader node, show a new leader is elected
#      and still accepts orders, then bring the old node back
#
# Start the cluster first with .\scripts\start.ps1 (ideally -Fresh).
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Invoke-Demo {
    param([string[]]$Arguments)
    $output = & docker compose exec -T web python scripts/demo.py @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "demo.py $($Arguments -join ' ') failed"
    }
    return $output
}

function Wait-ForEnter {
    Write-Host ""
    Read-Host "Press Enter to continue" | Out-Null
}

Invoke-Demo @("features") | Write-Host
Wait-ForEnter

Write-Host ""
Write-Host "================================================================"
Write-Host "6. Leader failover"
Write-Host "================================================================"

$leader = (Invoke-Demo @("leader", "--timeout", "30") | Select-Object -Last 1).Trim()
Write-Host "Current leader: $leader. Stopping its container (Raft + app server)..."
docker compose stop $leader

Write-Host "Waiting for the remaining nodes to elect a new leader..."
Start-Sleep -Seconds 6
Invoke-Demo @("order") | Write-Host

Wait-ForEnter

Write-Host ""
Write-Host "Restarting $leader; it rejoins the cluster as a follower..."
docker compose start $leader
Start-Sleep -Seconds 6
Invoke-Demo @("status") | Write-Host
