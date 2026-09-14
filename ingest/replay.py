"""Reload an archived raw run into Delta tables WITHOUT calling the API — the "worst
case" path the raw archive exists for: the source is gone/broken/rate-limited, but the
bytes it once returned are still in Files/raw/.

    python -m ingest.replay github 20260914T101530Z
    python -m ingest.replay github 20260914T101530Z --dataset github_fix

Semantics, deliberately different from a normal run:
  * no incremental filtering — exactly what was archived is loaded, nothing else;
  * writes to a SEPARATE dataset (default: <DATASET_NAME>_replay) and pipeline, so the
    original bronze tables and the stored incremental state stay untouched. Point
    --dataset at the real dataset only if you know why (e.g. after a full wipe).
"""

import argparse
import json
import os
import sys
from collections import defaultdict

os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")

import dlt
from azure.storage.filedatalake import DataLakeServiceClient

from .destination import ONELAKE_DFS, bronze_destination, require_env, storage_credential
from .raw_archive import RAW_ROOT
from .sources import SOURCES


def load_archived_items(pipeline_name: str, run_id: str) -> dict[str, list]:
    """Return {resource: [item, ...]} from one archived run, pages in order."""
    fs = DataLakeServiceClient(
        ONELAKE_DFS, credential=storage_credential()
    ).get_file_system_client(require_env("WORKSPACE_ID"))
    base = f"{require_env('LH_BRONZE_ID')}/{RAW_ROOT}/{pipeline_name}/{run_id}"

    page_files: dict[str, list[str]] = defaultdict(list)
    for p in fs.get_paths(path=base, recursive=True):
        if p.is_directory or p.name.endswith("_manifest.json"):
            continue
        rel = p.name[len(base) + 1 :]  # <resource>/page_NNNNN.json
        page_files[rel.split("/")[0]].append(p.name)

    if not page_files:
        raise FileNotFoundError(f"No archived pages under {base}")

    items: dict[str, list] = {}
    for resource, paths in page_files.items():
        rows: list = []
        for path in sorted(paths):
            data = json.loads(fs.get_file_client(path).download_file().readall())
            rows.extend(data if isinstance(data, list) else [data])
        items[resource] = rows
    return items


def replay(source_name: str, run_id: str, dataset: str | None = None) -> dict:
    module = SOURCES[source_name]
    items = load_archived_items(module.PIPELINE_NAME, run_id)

    pipeline = dlt.pipeline(
        pipeline_name=f"{module.PIPELINE_NAME}_replay",
        destination=bronze_destination(),
        dataset_name=dataset or f"{module.DATASET_NAME}_replay",
    )
    resources = [
        dlt.resource(rows, name=resource, write_disposition="append")
        for resource, rows in items.items()
    ]
    pipeline.run(resources, table_format="delta")

    row_counts = {resource: len(rows) for resource, rows in items.items()}
    return {
        "status": "success",
        "pipeline": pipeline.pipeline_name,
        "dataset": pipeline.dataset_name,
        "replayed_run_id": run_id,
        "row_counts": row_counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Reload an archived raw run into Delta.")
    parser.add_argument("source", help=f"one of: {', '.join(sorted(SOURCES))}")
    parser.add_argument("run_id", help="raw run id, e.g. 20260914T101530Z (see Files/raw/)")
    parser.add_argument("--dataset", default=None,
                        help="target dataset (default: <DATASET_NAME>_replay)")
    args = parser.parse_args()

    payload = replay(args.source, args.run_id, args.dataset)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
