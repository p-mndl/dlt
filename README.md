# dlt on Microsoft Fabric — REST APIs → Bronze

A template for ingesting REST APIs into a Microsoft Fabric lakehouse with
[dlt](https://dlthub.com/) — **without Spark**. Sources are plain Python modules; every
run persists **both** the raw API responses (byte-identical JSON files) and typed **Delta
tables**, written directly to OneLake via delta-rs. One project, two execution paths: the
same code runs locally from the terminal and inside Fabric through a parameterized runner
notebook.

This is the EL counterpart to the
[fabric-dbt-duckrun](https://github.com/philippmeindl/fabric-dbt-duckrun) template (dbt
transforms bronze → gold via duckrun); it deliberately mirrors that repo's patterns
(Variable Library, runner notebook, deploy script, exit-JSON contract) but is scoped to a
**single workspace** — the focus is understanding dlt and how it fits into Fabric, not
multi-environment promotion.

---

## Table of contents

- [Architecture](#architecture)
- [One project, two execution paths](#one-project-two-execution-paths)
- [Raw JSON archive & replay](#raw-json-archive--replay)
- [How dlt reaches OneLake](#how-dlt-reaches-onelake)
- [Setup guide](#setup-guide)
- [Authenticating to source APIs](#authenticating-to-source-apis)
- [Adding a new source](#adding-a-new-source)
- [Handover to dbt](#handover-to-dbt)
- [Design decisions](#design-decisions)
- [Troubleshooting](#troubleshooting)
- [Placeholder reference](#placeholder-reference)
- [Dependencies](#dependencies)

---

## Architecture

One workspace, one lakehouse. `LH_Bronze` holds three things:

| Location | Content | Written by |
|---|---|---|
| `Tables/<dataset>/<table>` | Typed, flattened Delta tables (append-only raw log) | dlt → delta-rs |
| `Files/raw/<pipeline>/<run_id>/` | Byte-identical API response bodies + `_manifest.json` | `ingest/raw_archive.py`, during extract |
| `Files/ingest/` | The deployed `ingest/` package (what `NB_dlt_runner` executes) | `deploy` terminal helper |

Fabric items shipped in this repo (git-sync root: `fabric/` — a portal setting):

| Item | Role |
|---|---|
| `LH_Bronze` | The one lakehouse (see table above) |
| `NB_dlt_runner` | Parameterized runner notebook: downloads `Files/ingest/`, calls `run_source()` — no pipeline logic of its own. Parameters: `dlt_source`, `dlt_resources`, `dlt_full_refresh` |
| `VL` (Variable Library) | Single source of truth for the GUIDs (`workspace_id`, `lh_bronze`) + `key_vault_url`; read locally by the terminal profile and at runtime by the notebook |

Repository layout:

```
.
├── requirements.txt              dlt[az,deltalake] (pinned) + azure SDKs
├── .vscode/
│   ├── settings.json             Terminal profile "dlt (dev)" (default profile)
│   └── terminal-init.ps1         Startup: venv, GUID env vars from VL, storage token;
│                                 helpers: ingest / replay / deploy
├── .deploy/
│   ├── fabric_vl.py              Reads fabric/VL.VariableLibrary locally
│   └── deploy_ingest_files.py    Uploads ingest/ to LH_Bronze/Files/ingest/
├── fabric/                       Git-sync root of the workspace (portal setting!)
│   ├── VL.VariableLibrary/
│   ├── LH_Bronze.Lakehouse/
│   └── NB_dlt_runner.Notebook/
└── ingest/                       The dlt project — deployed as-is, no GUIDs inside
    ├── run.py                    run_source() entrypoint + CLI (python -m ingest.run)
    ├── replay.py                 reload archived raw JSON (python -m ingest.replay)
    ├── destination.py            OneLake filesystem destination + credential plumbing
    ├── raw_archive.py            RawArchive: raw pages + manifest to Files/raw/
    └── sources/
        ├── __init__.py           SOURCES registry
        └── github.py             demo source (GitHub issues, incremental)
```

## One project, two execution paths

Same idea as running dbt locally and via its runner notebook — both paths execute the
identical project files:

| | Local | Fabric |
|---|---|---|
| Trigger | `ingest github` in the `dlt (dev)` terminal | `NB_dlt_runner` (manual, scheduled, or from a Data Pipeline) |
| Project files | `ingest/` in the working tree | `Files/ingest/` (uploaded by `deploy`) |
| GUIDs | Terminal profile reads `fabric/VL.VariableLibrary` | Notebook reads the workspace's Variable Library |
| OneLake token | `az account get-access-token` | `notebookutils.credentials.getToken("storage")` |
| API secrets | Env vars (e.g. `$env:GITHUB_TOKEN`) | Key Vault via each source's `SECRETS` mapping |
| Entrypoint | `ingest.run:run_source()` | the same |
| Result | JSON on stdout, exit code | same JSON via `notebookutils.notebook.exit()` for pipelines to switch on |

Both paths write to the **same** bronze lakehouse — and because dlt stores each pipeline's
incremental state **in the destination** (`_dlt_pipeline_state` under `Tables/<dataset>/`),
local runs and Fabric runs share one cursor: whichever ran last, the next run continues
from there. No state files to sync.

The typical loop: develop and run locally until the output looks right → `deploy` → run
`NB_dlt_runner` in Fabric as the final test → schedule it.

## Raw JSON archive & replay

Every page of every API response is written **byte-identical** to
`Files/raw/<pipeline>/<run_id>/<resource>/page_NNNNN.json` *before* dlt normalizes, types,
or flattens anything (`run_id` = UTC timestamp, also reported in the run's output JSON as
`raw_run_id`). A `_manifest.json` per run records URL, status code and fetch time of each
page — including for zero-page and failed runs, so the archive doubles as a run log.

Why: when a bronze row looks wrong, you can diff it against what the API actually returned
(the archive is *closer to the source* than bronze — bronze is already typed and
flattened); and if a load needs to be re-done, the bytes are still there even if the API
is down, rate-limited, or no longer returns that data.

Replay reloads an archived run into Delta without touching the API:

```powershell
replay github 20260914T101530Z                    # -> Tables/github_replay/
replay github 20260914T101530Z --dataset github_fix
```

Replay deliberately bypasses incremental state (it loads exactly what was archived) and
targets a **separate dataset** by default, so the real bronze tables and the stored cursor
stay untouched. There is no automatic retention — prune old `Files/raw/` runs manually or
with a scheduled cleanup when the archive grows.

## How dlt reaches OneLake

Everything environment-specific arrives through three env vars — `WORKSPACE_ID`,
`LH_BRONZE_ID`, `FABRIC_STORAGE_TOKEN` — set by the terminal profile locally and by
`NB_dlt_runner` in Fabric. A missing variable fails loudly (deliberately no defaults).

dlt touches OneLake on two paths, both fed from the same token
(`ingest/destination.py`): **adlfs**/fsspec for dlt's bookkeeping files (needs the
explicit `account_host: onelake.blob.fabric.microsoft.com`, since adlfs only auto-detects
`*.core.windows.net` accounts) and **delta-rs** for the Delta writes (gets a frozen bearer
token plus `use_fabric_endpoint`). The raw archive and the deploy script use the ADLS Gen2
SDK with the same token. OneLake paths use **GUIDs, not friendly names** (names in abfss
paths are unreliable upstream).

## Setup guide

### 0. Prerequisites

- A Microsoft Fabric capacity (trial works) and permission to create workspaces.
- **Python 3.12**, Azure CLI (`az`), VS Code.
- This repo pushed to a git host Fabric can sync with (Azure DevOps Repos or GitHub).

### 1. Create the workspace and connect git

1. Create a workspace (e.g. `DEV_dlt`), assign it to your capacity.
2. Workspace settings → **Git integration** → connect to this repo, and set the
   **folder to `fabric/`**. This is a portal-only setting — without it the sync would try
   to treat the whole repo as Fabric items.
3. Sync. Fabric creates `LH_Bronze`, `NB_dlt_runner` and the Variable Library.

### 2. Fill in the GUIDs

All placeholders live in **one file**: `fabric/VL.VariableLibrary/variables.json`. After
the first sync, collect the real GUIDs (visible in the portal URL when the item is open)
and fill in `workspace_id` and `lh_bronze`. `key_vault_url` is only needed once a source
declares mandatory `SECRETS` (the GitHub demo runs without it). Commit and let the
workspace sync the updated Variable Library.

### 3. Local development

One-time setup:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Daily use: open a VS Code terminal — the default profile `dlt (dev)` activates the venv,
exports the GUID env vars from the Variable Library, acquires `FABRIC_STORAGE_TOKEN`
(runs `az login` first if there is no Azure session), and defines three helpers, each of
which refreshes the token before touching OneLake:

```powershell
ingest github                     # run the source; raw JSON + Delta land in LH_Bronze
ingest github --full-refresh      # drop tables + stored state, reload from scratch
replay github <run_id>            # reload archived raw JSON (see above)
deploy                            # upload ingest/ to LH_Bronze/Files/ingest/
```

Run `ingest github` twice: the second run reports few or zero rows — the incremental
cursor at work (state lives in the destination, see above).

### 4. Run in Fabric

1. `deploy` — uploads the `ingest/` package.
2. Run `NB_dlt_runner` (defaults run the `github` source). Check `Tables/github/` and
   `Files/raw/github_issues/` in `LH_Bronze`.
3. For scheduling: either schedule the notebook directly, or call it from a Data Pipeline
   notebook activity and switch on the exit JSON's `status`
   (`success` / `ingestion_failed`) — same pattern as the dbt runner in the duckrun repo,
   including `alert_message` for a Teams/Outlook activity.

**Deploy before running**: the notebook executes what is in `Files/ingest/`, not your
working tree. Local edits are invisible in Fabric until the next `deploy`.

## Authenticating to source APIs

Two separate concerns — do not mix them up:

- **OneLake auth** (where the data lands) is fully handled by the plumbing above.
- **Source API auth** (where the data comes from) is each source module's business.

dlt's `RESTClient` ships auth classes for the common schemes: `BearerTokenAuth` (static
tokens/PATs — the GitHub demo), `APIKeyAuth` (key in a header or query param),
`HttpBasicAuth`, and `OAuth2ClientCredentials` (client_id/secret → token endpoint →
auto-refreshed bearer — the usual case for enterprise APIs):

```python
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from ingest.destination import require_env  # loud failure if the secret is missing

auth = OAuth2ClientCredentials(
    access_token_url="https://api.example.com/oauth/token",
    client_id="my-client-id",                    # not secret, fine as a literal
    client_secret=require_env("MYAPI_CLIENT_SECRET"),
)
client = RESTClient(base_url="https://api.example.com", auth=auth, ...)
```

Where the secret value comes from is uniform across all schemes:

- **Locally**: an env var (`$env:MYAPI_CLIENT_SECRET = "..."` in the terminal; never in git).
- **In Fabric**: the source's `SECRETS` mapping (`{env_var: key_vault_secret_name}`) —
  `NB_dlt_runner` resolves each entry from the Key Vault named in the Variable Library
  into env vars *before* building the source. The identity running the notebook needs
  **Get** permission on the vault's secrets (RBAC-mode vaults: *Key Vault Secrets User*).
  An unresolvable secret is a warning, not a failure — sources choose per credential
  whether it is optional (`os.environ.get`, like the GitHub token) or mandatory
  (`require_env`).

For token endpoints that predate OAuth2 (credentials POSTed as JSON, token somewhere else
in the response), subclass `OAuth2ClientCredentials` — the duckrun repo's `NB_dlt_runner`
contains a worked `token_exchange` example (Personio v1).

## Adding a new source

1. Copy `ingest/sources/github.py`, adjust `PIPELINE_NAME`, `DATASET_NAME`, `SECRETS`,
   the client (base_url, auth, paginator) and the resource(s). Keep the pattern: one
   `archive.store(...)` per page, *then* `yield page`.
2. Register it in `ingest/sources/__init__.py`.
3. Run locally: `ingest <name>`. Then `deploy` and run `NB_dlt_runner` with
   `dlt_source=<name>`.

Conventions the pattern gives you for free: `write_disposition="append"` (bronze is an
immutable log — dedupe downstream), incremental cursor via `dlt.sources.incremental`
(state in the destination), nested JSON flattened into child tables (e.g.
`issues__labels`) by dlt's normalizer, schema evolution on source changes.

## Handover to dbt

dlt owns Extract + Load; everything semantic is dbt's job. With the duckrun template,
point a dbt source at the bronze table
(`meta.delta_table_path` → `Tables/<dataset>/<table>`) and make the first staging model
the dedupe (keep the latest row version per primary key — bronze is append-only). See
`stg_github_issues.sql` in the duckrun repo for the worked example against exactly the
table this PoC produces.

## Design decisions

- **Python-first sources instead of declarative `rest_api` YAML.** The duckrun repo
  demonstrates the declarative flavor (config file + generic runner). Here each source is
  explicit code because (a) the raw archive needs a hook per page, before dlt processes
  it — the declarative path offers no clean seam for that; (b) the PoC's goal is
  *understanding* dlt: pagination, incremental wiring and auth are visible instead of
  derived. dlt's `RESTClient` still does the heavy lifting (paginators, auth, retries).
- **Raw archive at extract time, page-granular.** Archiving inside the pagination loop
  (rather than a separate "download first, load second" pass) means one API pass total,
  and a run that fails in normalize/load still leaves its raw pages behind.
- **Replay to a separate dataset.** Reloading archived data must never corrupt the
  incremental cursor or duplicate rows in the real bronze tables by accident.
- **One lakehouse.** Raw files and tables are two representations of the same bronze data;
  splitting them across lakehouses buys nothing in a single-workspace PoC.
- **State in the destination, not in files.** Survives ephemeral Fabric sessions *and*
  makes local + Fabric runs share one cursor. `--full-refresh` (`refresh="drop_sources"`)
  drops tables and state together.
- **The runner notebook contains no pipeline logic.** It authenticates, downloads,
  resolves secrets, and calls the same entrypoint as the CLI — sources are added by
  editing the repo, never the notebook.
- **Exit-JSON contract identical to the dbt runner** (`status`, `row_counts`,
  `alert_message`), so orchestration and alerting patterns from the duckrun repo apply
  unchanged.

## Troubleshooting

- **401/403 on OneLake locally** — token expired (~60–90 min). The helpers refresh it per
  call; a long-running `ingest` can still outlive it. Re-run; open a new terminal if the
  az session itself is gone.
- **`RuntimeError: Environment variable ... is not set`** — you are in a plain terminal.
  Open the `dlt (dev)` profile (or set `WORKSPACE_ID`, `LH_BRONZE_ID`,
  `FABRIC_STORAGE_TOKEN` yourself).
- **GitHub 403 with `rate limit exceeded`** — unauthenticated ceiling (60 req/h). Set
  `$env:GITHUB_TOKEN` (any fine-grained PAT, no scopes needed for public repos).
- **Notebook fails in cell 1 on pyarrow/deltalake imports** — the `restartPython()` after
  `%pip install` was removed; put it back.
- **`_dlt_loads` / `_dlt_pipeline_state` / `_dlt_version` show as *Unidentified*** in the
  Lakehouse explorer — dlt bookkeeping stored next to the tables. Harmless; don't delete
  it, the incremental state lives there.
- **Fabric run executes stale code** — `deploy` was forgotten; the notebook runs
  `Files/ingest/`, not your working tree.

## Placeholder reference

| Placeholder | Meaning | File |
|---|---|---|
| `11111111-1111-1111-1111-111111111111` | Workspace ID | `fabric/VL.VariableLibrary/variables.json` |
| `22222222-2222-2222-2222-222222222222` | `LH_Bronze` lakehouse ID | `fabric/VL.VariableLibrary/variables.json` |
| `https://REPLACE-ME.vault.azure.net/` | Key Vault for source API secrets (optional for the demo) | `fabric/VL.VariableLibrary/variables.json` |

The `ingest/` package contains no GUIDs at all — everything resolves from env vars at
runtime.

## Dependencies

| Package | Purpose | Version |
|---|---|---|
| dlt (`[az,deltalake]`) | EL framework; adlfs + delta-rs writer | pinned in `requirements.txt` and `NB_dlt_runner` (keep in sync) |
| azure-storage-file-datalake | Raw archive, replay, deploy, notebook download | >= 12.14.0 |
| azure-identity | `AzureCliCredential` for the deploy script | >= 1.17.0 |

## License

[MIT](LICENSE)
