# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   }
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "vCores": 2
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# %%configure above pins the session to the smallest Python-notebook size (2 vCores / 16 GB —
# REST extraction is I/O-bound, that's plenty); it's a cell magic, so it must stand alone as
# the first cell.
#
# Generic dlt runner: contains NO pipeline logic. It authenticates, downloads the ingest/
# package from LH_Bronze/Files/ingest/ (uploaded by the `deploy` terminal helper) and calls
# the same run_source() entrypoint the local CLI uses — what was tested locally is
# byte-identical to what runs here.
#
# Pinned to the same version as requirements.txt so Fabric runs what the code was written
# against. [az] = adlfs for dlt's filesystem operations, [deltalake] = delta-rs Delta writer.

%pip install -q "dlt[az,deltalake]==1.29.0"

# The pip install upgrades pyarrow/deltalake, which the Fabric kernel image ships in older
# versions; a session restart makes sure the upgraded versions are actually loaded.
import notebookutils
notebookutils.session.restartPython()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

# Defaults for standalone execution. A Fabric Data Pipeline can override these per activity.
dlt_source = "github"       # source name from ingest/sources/__init__.py
dlt_resources = ""          # comma-separated resource names; empty = all resources
dlt_full_refresh = "false"  # "true" drops this source's tables + stored state, reloads from scratch

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import os
import sys
import tempfile

import notebookutils
from azure.core.credentials import AccessToken, TokenCredential
from azure.storage.filedatalake import DataLakeServiceClient

vl = notebookutils.variableLibrary.getLibrary("VL")
WORKSPACE_ID = vl.workspace_id
LH_BRONZE_ID = vl.lh_bronze

# Fetch the storage token explicitly via notebookutils instead of relying on a credential
# chain that would try (and fail) to reach IMDS from here. The token is OneLake-wide: the
# same one downloads the project files and writes raw JSON + Delta output.
STORAGE_TOKEN = notebookutils.credentials.getToken("storage")

# ingest/destination.py reads exactly these three — the notebook's only contract with the code.
os.environ["WORKSPACE_ID"] = WORKSPACE_ID
os.environ["LH_BRONZE_ID"] = LH_BRONZE_ID
os.environ["FABRIC_STORAGE_TOKEN"] = STORAGE_TOKEN


class _StaticToken(TokenCredential):
    def get_token(self, *_, **__):
        return AccessToken(STORAGE_TOKEN, 9999999999)


def download_project_from_onelake(local_root):
    """Mirror LH_Bronze/Files/ingest from OneLake into a local dir, no lakehouse mount needed."""
    fs = DataLakeServiceClient(
        "https://onelake.dfs.fabric.microsoft.com",
        credential=_StaticToken(),
    ).get_file_system_client(WORKSPACE_ID)
    rel = f"{LH_BRONZE_ID}/Files/ingest"
    for p in fs.get_paths(path=rel, recursive=True):
        if p.is_directory:
            continue
        suffix = os.path.relpath(p.name, rel)
        target = os.path.join(local_root, "ingest", suffix)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as fh:
            fh.write(fs.get_file_client(p.name).download_file().readall())


PROJECT_ROOT = os.path.join(tempfile.gettempdir(), "dlt_project")
download_project_from_onelake(PROJECT_ROOT)
sys.path.insert(0, PROJECT_ROOT)

print(f"ingest package: {PROJECT_ROOT}\\ingest")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Resolve the source's optional SECRETS mapping ({env_var: key_vault_secret_name}) from Key
# Vault into env vars, BEFORE the source is built. getSecret authenticates as whoever runs
# the notebook — that identity needs Get permission on the vault's secrets. A secret that
# cannot be resolved is a warning, not a failure: sources decide themselves whether a
# credential is mandatory (require_env) or optional (os.environ.get, like the GitHub token).

from ingest.sources import SOURCES

if dlt_source not in SOURCES:
    raise ValueError(f"Unknown source '{dlt_source}'. Available: {', '.join(sorted(SOURCES))}")

for env_var, secret_name in getattr(SOURCES[dlt_source], "SECRETS", {}).items():
    if os.environ.get(env_var):
        continue
    try:
        os.environ[env_var] = notebookutils.credentials.getSecret(vl.key_vault_url, secret_name)
        print(f"resolved secret '{secret_name}' -> ${env_var}")
    except Exception as exc:  # optional secrets: missing vault/permission must not kill the run
        print(f"WARNING: could not resolve Key Vault secret '{secret_name}' for ${env_var}: {exc}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Same contract as the dbt runner: run_source() never raises on a failed LOAD (an exception
# would fail the notebook activity before an orchestrating pipeline could read a structured
# exitValue) — it reports status "ingestion_failed". Failures before the pipeline exists
# (config download, auth) still raise and are caught by the notebook activity's on-failure
# path as the fallback alert.

import json

from ingest.run import run_source

print(f"Running dlt source '{dlt_source}'")
payload = run_source(
    dlt_source,
    resources=dlt_resources,
    full_refresh=str(dlt_full_refresh).lower() == "true",
)

exit_value = json.dumps(payload, ensure_ascii=False)
print(f"dlt source finished with status '{payload['status']}'")
print(exit_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# notebookutils.notebook.exit() raises to halt the run, which Fabric renders as a failed
# cell even on a successful pipeline — kept in its own cell so that stray traceback doesn't
# get attributed to the ingestion logic above.
notebookutils.notebook.exit(exit_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
