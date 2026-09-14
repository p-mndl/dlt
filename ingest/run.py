"""Shared entrypoint: the local CLI and NB_dlt_runner both call run_source(), so what is
tested locally is byte-identical to what runs in Fabric.

Local usage (from the repo root, env vars set by the 'dlt (dev)' terminal profile):

    python -m ingest.run github
    python -m ingest.run github --resources issues
    python -m ingest.run github --full-refresh      # drops tables AND stored state

Returns/prints a compact JSON payload (status, row_counts, raw_run_id, alert_message) —
the same contract the dbt runner uses, so a Fabric Data Pipeline can switch on `status`.
"""

import argparse
import json
import os
import sys

# Must be set before dlt is imported; the run reports below carry everything we need.
os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")

import dlt
from dlt.pipeline.exceptions import PipelineStepFailed

from .destination import bronze_destination
from .raw_archive import RawArchive
from .sources import SOURCES


def run_source(source_name: str, resources: str = "", full_refresh: bool = False) -> dict:
    if source_name not in SOURCES:
        raise ValueError(
            f"Unknown source '{source_name}'. Available: {', '.join(sorted(SOURCES))}"
        )
    module = SOURCES[source_name]

    archive = RawArchive(module.PIPELINE_NAME)
    source = module.build_source(archive)
    if resources.strip():
        source = source.with_resources(*[r.strip() for r in resources.split(",")])

    pipeline = dlt.pipeline(
        pipeline_name=module.PIPELINE_NAME,
        destination=bronze_destination(),
        dataset_name=module.DATASET_NAME,  # -> Tables/<dataset>/<table>
    )

    # drop_sources wipes this source's tables AND its stored incremental state, so the
    # reload starts over from INITIAL_CURSOR — the EL equivalent of dbt --full-refresh.
    run_kwargs = {"refresh": "drop_sources"} if full_refresh else {}

    # A failed LOAD does not raise (in Fabric an exception would fail the notebook activity
    # before the orchestrating pipeline could read a structured exitValue) — it reports
    # status "ingestion_failed" instead. Failures before the pipeline exists (auth, config)
    # still raise and are the caller's on-failure fallback.
    status, error_message = "success", ""
    try:
        pipeline.run(source, table_format="delta", **run_kwargs)
    except PipelineStepFailed as exc:
        status = "ingestion_failed"
        error_message = f"{exc.step}: {str(exc)[:500]}"

    archive.finalize(status)

    # Rows that reached the destination in THIS run, per table (incremental no-op runs
    # report an empty dict). _dlt_pipeline_state is dlt bookkeeping — dropped here.
    normalize_info = pipeline.last_trace.last_normalize_info if pipeline.last_trace else None
    row_counts = {
        table: count
        for table, count in (normalize_info.row_counts if normalize_info else {}).items()
        if not table.startswith("_dlt")
    }

    icon = {"success": "✅", "ingestion_failed": "🔴"}[status]
    lines = [f"{icon} dlt {module.PIPELINE_NAME} — {status.replace('_', ' ')}"]
    if row_counts:
        lines += [f"- **{table}**: {row_counts[table]} rows" for table in sorted(row_counts)]
    elif status == "success":
        lines.append("- no new rows (incremental cursor up to date)")
    if error_message:
        lines.append(f"- {error_message}")

    return {
        "status": status,
        "pipeline": module.PIPELINE_NAME,
        "source": source_name,
        "raw_run_id": archive.run_id,  # Files/raw/<pipeline>/<raw_run_id>/
        "row_counts": row_counts,
        "alert_message": "\n".join(lines),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one dlt ingestion source.")
    parser.add_argument("source", help=f"one of: {', '.join(sorted(SOURCES))}")
    parser.add_argument("--resources", default="", help="comma-separated resource names; empty = all")
    parser.add_argument("--full-refresh", action="store_true",
                        help="drop this source's tables and stored state, reload from scratch")
    args = parser.parse_args()

    payload = run_source(args.source, args.resources, args.full_refresh)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0 if payload["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
