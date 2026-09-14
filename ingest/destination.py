"""OneLake plumbing shared by every execution path (local CLI and NB_dlt_runner).

Both paths provide the same three environment variables before anything here runs:

    WORKSPACE_ID          Fabric workspace GUID
    LH_BRONZE_ID          LH_Bronze lakehouse GUID
    FABRIC_STORAGE_TOKEN  short-lived OneLake bearer token
                          (locally: az account get-access-token --resource https://storage.azure.com,
                           acquired by .vscode/terminal-init.ps1; in Fabric:
                           notebookutils.credentials.getToken("storage"))

Missing variable -> loud failure instead of writing to the wrong place (no defaults).
"""

import os

from azure.core.credentials import AccessToken, TokenCredential
from dlt.common.configuration.specs import AzureCredentials
from dlt.destinations import filesystem

ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Environment variable {name} is not set. Locally: open a new 'dlt (dev)' "
            "terminal (it resolves the Variable Library and acquires a storage token); "
            "in Fabric this is set by NB_dlt_runner."
        )
    return value


class StaticTokenCredential(TokenCredential):
    """Wraps an already-acquired OneLake bearer token as an azure-core TokenCredential,
    so the same object serves the ADLS SDK, adlfs and dlt regardless of where the token
    came from (az CLI locally, notebookutils in Fabric)."""

    def __init__(self, token: str):
        self._token = token

    def get_token(self, *_, **__):
        return AccessToken(self._token, 9999999999)


def storage_credential() -> StaticTokenCredential:
    return StaticTokenCredential(require_env("FABRIC_STORAGE_TOKEN"))


def bronze_destination():
    """dlt filesystem destination writing Delta tables to LH_Bronze/Tables.

    dlt reaches OneLake on two paths, and the "external session" credential feeds both:
      * adlfs (fsspec) for dlt's own bookkeeping files — gets the credential OBJECT, plus
        the explicit account_host because adlfs only auto-detects *.core.windows.net;
      * delta-rs (object_store) for the Delta writes — dlt freezes the credential into a
        bearer token; use_fabric_endpoint makes the OneLake host explicit rather than
        relying on delta-rs URL sniffing.
    """
    credentials = AzureCredentials.from_credential(storage_credential())
    credentials.azure_storage_account_name = "onelake"

    return filesystem(
        bucket_url=(
            f"abfss://{require_env('WORKSPACE_ID')}"
            f"@onelake.dfs.fabric.microsoft.com/{require_env('LH_BRONZE_ID')}/Tables"
        ),
        credentials=credentials,
        kwargs={"account_host": "onelake.blob.fabric.microsoft.com"},
        deltalake_storage_options={"use_fabric_endpoint": "true"},
    )
