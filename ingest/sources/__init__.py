"""Source registry — the single place a new source gets wired in.

Each entry maps a CLI/notebook name to a module exposing:
    PIPELINE_NAME  keys dlt's state in the destination (renaming orphans the cursor)
    DATASET_NAME   schema folder under LH_Bronze/Tables/
    SECRETS        optional {env_var: key_vault_secret_name} resolved by NB_dlt_runner
    build_source(archive) -> DltSource
"""

from . import github

SOURCES = {
    "github": github,
}
