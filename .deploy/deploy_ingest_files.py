"""Upload the ingest/ project to LH_Bronze/Files/ingest/ — the files NB_dlt_runner
downloads at runtime. A plain copy: the files carry no GUIDs or secrets, everything is
resolved from env vars at runtime.

Usage:
    python .deploy/deploy_ingest_files.py     (or the `deploy` terminal helper)
"""

from pathlib import Path

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

import fabric_vl

ROOT = Path(__file__).parent.parent
PROJECT_DIR = ROOT / "ingest"
TARGET_SUBDIR = "ingest"

SKIP_DIRS = {"__pycache__"}


def project_files() -> list[Path]:
    return [
        f
        for f in sorted(PROJECT_DIR.rglob("*"))
        if f.is_file()
        and f.suffix != ".pyc"
        and not (SKIP_DIRS & set(f.relative_to(PROJECT_DIR).parts))
    ]


def main():
    variables = fabric_vl.get_variables()
    workspace_id = variables["workspace_id"]
    lh_bronze_id = variables["lh_bronze"]

    fs = DataLakeServiceClient(
        "https://onelake.dfs.fabric.microsoft.com", credential=AzureCliCredential()
    ).get_file_system_client(workspace_id)

    try:
        fs.get_directory_client(f"{lh_bronze_id}/Files/{TARGET_SUBDIR}").delete_directory()
        print(f"deleted existing Files/{TARGET_SUBDIR}/")
    except ResourceNotFoundError:
        pass

    files = project_files()
    for f in files:
        rel = Path(TARGET_SUBDIR) / f.relative_to(PROJECT_DIR)
        fs.get_directory_client(
            f"{lh_bronze_id}/Files/{rel.parent.as_posix()}"
        ).create_directory()
        fs.get_directory_client(
            f"{lh_bronze_id}/Files/{rel.parent.as_posix()}"
        ).get_file_client(f.name).upload_data(f.read_bytes(), overwrite=True)
        print(f"uploaded {rel.as_posix()}")

    print(f"\nDeployed {len(files)} file(s) to workspace {workspace_id}.")


if __name__ == "__main__":
    main()
