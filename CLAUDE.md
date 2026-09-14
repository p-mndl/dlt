# CLAUDE.md

Single-workspace PoC: dlt ingests REST APIs into a Fabric lakehouse (raw JSON under
Files/raw/ + Delta tables under Tables/). The Fabric workspace is git-synced with root
folder set to `fabric/` — a Fabric portal setting, not captured anywhere in the repo.

`ingest/` is the deployable unit: it must stay free of GUIDs/secrets (everything resolves
from env vars at runtime) because `deploy` uploads it as-is to LH_Bronze/Files/ingest/,
where NB_dlt_runner executes it. The dlt version pin exists twice by design:
requirements.txt (local) and fabric/NB_dlt_runner.Notebook/notebook-content.py (Fabric) —
change both together.

Sibling repo: C:\git\fabric-dbt-duckrun (dbt/duckrun for bronze→gold; also demonstrates
the declarative-YAML dlt flavor this repo deliberately does not use).
