"""Raw response archive: every API response body lands byte-identical in the lakehouse,
BEFORE dlt normalizes/types/flattens anything.

Layout under LH_Bronze:

    Files/raw/<pipeline_name>/<run_id>/<resource>/page_00001.json
    Files/raw/<pipeline_name>/<run_id>/_manifest.json

The manifest records per page the request URL, status code and fetch time — enough to
debug "why does this row look like that" against the source, or to reload a run without
calling the API again (see replay.py). Archiving happens during extract, so even a run
that later fails in normalize/load leaves its raw pages behind — that is the point.
"""

import json
from datetime import datetime, timezone

from azure.storage.filedatalake import DataLakeServiceClient

from .destination import ONELAKE_DFS, require_env, storage_credential

RAW_ROOT = "Files/raw"


class RawArchive:
    def __init__(self, pipeline_name: str, run_id: str | None = None):
        self.pipeline_name = pipeline_name
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self._entries: list[dict] = []
        self._created_dirs: set[str] = set()
        fs = DataLakeServiceClient(
            ONELAKE_DFS, credential=storage_credential()
        ).get_file_system_client(require_env("WORKSPACE_ID"))
        self._run_dir = fs.get_directory_client(
            f"{require_env('LH_BRONZE_ID')}/{RAW_ROOT}/{self.pipeline_name}/{self.run_id}"
        )

    def store(self, resource: str, page_number: int, response) -> str:
        """Persist one requests.Response body verbatim. Called from inside a resource's
        pagination loop, one call per page."""
        if resource not in self._created_dirs:
            self._run_dir.get_sub_directory_client(resource).create_directory()
            self._created_dirs.add(resource)
        rel_path = f"{resource}/page_{page_number:05d}.json"
        self._run_dir.get_file_client(rel_path).upload_data(response.content, overwrite=True)
        self._entries.append(
            {
                "resource": resource,
                "page": page_number,
                "path": rel_path,
                "url": response.url,
                "status_code": response.status_code,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        return rel_path

    def finalize(self, status: str) -> None:
        """Write the manifest. Always called (also for zero-page and failed runs), so every
        run leaves a folder documenting that it happened and what it fetched."""
        manifest = {
            "pipeline": self.pipeline_name,
            "run_id": self.run_id,
            "status": status,
            "finalized_at": datetime.now(timezone.utc).isoformat(),
            "page_count": len(self._entries),
            "pages": self._entries,
        }
        self._run_dir.get_file_client("_manifest.json").upload_data(
            json.dumps(manifest, indent=2).encode("utf-8"), overwrite=True
        )
