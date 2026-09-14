$root = $PWD.Path
.\.venv\Scripts\Activate.ps1

# Resolve the workspace/lakehouse GUIDs from the Variable Library (single workspace, no
# value sets) and export them for ingest/destination.py.
$vl = python "$root\.deploy\fabric_vl.py" | ConvertFrom-Json
$env:WORKSPACE_ID = $vl.workspace_id
$env:LH_BRONZE_ID = $vl.lh_bronze
Write-Host "Variable Library resolved (workspace $($vl.workspace_id))." -ForegroundColor Green

# The storage token lives ~60-90 min; the helpers below refresh it before every OneLake
# call (az returns a cached token instantly while the CLI session is alive).
function Update-FabricToken {
    $token = az account get-access-token --resource https://storage.azure.com --query accessToken -o tsv 2>$null
    if (-not $token) {
        Write-Host 'Storage token refresh failed - run az login first.' -ForegroundColor Red
        return $false
    }
    $env:FABRIC_STORAGE_TOKEN = $token
    return $true
}

$token = az account get-access-token --resource https://storage.azure.com --query accessToken -o tsv 2>$null
if (-not $token) {
    az login
    $token = az account get-access-token --resource https://storage.azure.com --query accessToken -o tsv
}
if ($token) {
    $env:FABRIC_STORAGE_TOKEN = $token
    Write-Host 'FABRIC_STORAGE_TOKEN set.' -ForegroundColor Green
} else {
    Write-Host 'Could not acquire token.' -ForegroundColor Red
}

# ingest github [--resources issues] [--full-refresh]   run a source locally -> OneLake
function ingest {
    if (Update-FabricToken) { python -m ingest.run @args }
}

# replay github <run_id> [--dataset ...]                reload archived raw JSON
function replay {
    if (Update-FabricToken) { python -m ingest.replay @args }
}

# deploy                                                upload ingest/ to LH_Bronze/Files/
function deploy {
    if (Update-FabricToken) { python "$root\.deploy\deploy_ingest_files.py" @args }
}
