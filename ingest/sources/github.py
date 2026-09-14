"""GitHub issues — the demo source and the pattern every real source follows.

Python-first on purpose: the pagination loop is explicit (dlt's RESTClient still does
paginator/auth/retries), which buys two things a fully declarative config cannot give us:
the raw response of every page is archived before dlt touches it, and incremental wiring
is visible instead of magic. Copy this module to add a source; the registry in
sources/__init__.py is the only other place to touch.

Auth: works unauthenticated (60 requests/h — plenty for incremental runs). If GITHUB_TOKEN
is set it is sent as a bearer token (5000 requests/h). Locally: export the env var (or put
it in the terminal); in Fabric: NB_dlt_runner resolves the SECRETS mapping below from Key
Vault into env vars before the source is built — missing secret = warning, not failure,
because the token is optional here. For mandatory credentials at work, fail loudly instead:
require_env() from ingest.destination does exactly that.
"""

import os

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

PIPELINE_NAME = "github_issues"
DATASET_NAME = "github"  # -> LH_Bronze/Tables/github/
SECRETS = {"GITHUB_TOKEN": "github-token"}  # env var -> Key Vault secret name (optional)

# A busy public repo so the demo has data; swap for any owner/repo. Note GitHub's /issues
# endpoint includes pull requests — filtering them out is a downstream (dbt) concern.
REPO = "dlt-hub/dlt"

# First-ever run starts here; later runs continue from the cursor stored in the
# destination (_dlt_pipeline_state), which survives ephemeral Fabric sessions.
INITIAL_CURSOR = "2026-06-01T00:00:00Z"


def build_source(archive):
    token = os.environ.get("GITHUB_TOKEN")

    @dlt.source(name="github")
    def github():
        # write_disposition="append": bronze stays a raw, immutable log — an issue updated
        # after ingestion arrives as a second row version; deduplication is dbt's job.
        # primary_key lets dlt's incremental dedupe rows sitting exactly on the cursor
        # boundary across runs.
        @dlt.resource(name="issues", write_disposition="append", primary_key="id")
        def issues(
            updated_at=dlt.sources.incremental("updated_at", initial_value=INITIAL_CURSOR)
        ):
            client = RESTClient(
                base_url="https://api.github.com",
                auth=BearerTokenAuth(token) if token else None,
                paginator=HeaderLinkPaginator(),  # GitHub paginates via the Link header
            )
            params = {
                "state": "all",
                "per_page": 100,
                # GitHub returns issues updated at or after `since`; dlt's incremental
                # tracks the highest updated_at seen and filters boundary duplicates.
                "since": updated_at.last_value,
            }
            for page_number, page in enumerate(
                client.paginate(f"/repos/{REPO}/issues", params=params), start=1
            ):
                archive.store("issues", page_number, page.response)
                yield page

        return issues

    return github()
