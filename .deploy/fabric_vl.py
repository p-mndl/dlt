"""Read the Fabric Variable Library (fabric/VL.VariableLibrary) outside of a Fabric runtime.

Single-workspace setup: no value sets, just the base variables.json. Callers:
.vscode/terminal-init.ps1 (CLI mode prints the values as JSON so the terminal profile can
export them as env vars) and .deploy/deploy_ingest_files.py.
"""

import json
from pathlib import Path

_VL_DIR = Path(__file__).parent.parent / "fabric" / "VL.VariableLibrary"


def get_variables() -> dict:
    base = json.loads((_VL_DIR / "variables.json").read_text(encoding="utf-8"))
    return {v["name"]: v["value"] for v in base["variables"]}


if __name__ == "__main__":
    print(json.dumps(get_variables()))
