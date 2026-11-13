# Stop the cluster.
#
#   .\scripts\stop.ps1          stop, keeping each node's data
#   .\scripts\stop.ps1 -Wipe    stop and delete all node data
param(
    [switch]$Wipe
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if ($Wipe) {
    docker compose down --volumes --remove-orphans
    Write-Host "Stopped and removed all node data."
}
else {
    docker compose down --remove-orphans
    Write-Host "Stopped. Node data is kept; use -Wipe to delete it."
}
