# Build and start the whole cluster in Docker, then wait for a leader.
#
#   .\scripts\start.ps1            start (keeps existing node data)
#   .\scripts\start.ps1 -Fresh     wipe all node data first
param(
    [switch]$Fresh
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Invoke-Checked {
    param([string]$Command, [string[]]$Arguments)
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $Command $($Arguments -join ' ')"
    }
}

if ($Fresh) {
    Write-Host "Removing existing containers and node data..."
    Invoke-Checked docker @("compose", "down", "--volumes", "--remove-orphans")
}

Invoke-Checked docker @("compose", "up", "-d", "--build")

Write-Host ""
Write-Host "Waiting for the Raft nodes to elect a leader..."
Invoke-Checked docker @("compose", "exec", "-T", "web", "python", "scripts/demo.py", "wait", "--timeout", "60")

Write-Host ""
Invoke-Checked docker @("compose", "exec", "-T", "web", "python", "scripts/demo.py", "status")

Write-Host ""
Write-Host "Dashboard: http://localhost:8000   (alice / password)"
Write-Host "Logs:      docker compose logs -f"
Write-Host "Demo:      .\scripts\demo.ps1"
Write-Host "Stop:      .\scripts\stop.ps1"
