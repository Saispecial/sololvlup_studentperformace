"""
MCP Neon Bridge - Thin wrapper around the Neon MCP run_sql tool.

When running inside Kiro (agent context), SQL is executed via the
NeonDB MCP server. When running standalone (e.g. scheduled job),
it falls back to the @neondatabase/serverless driver via a Node.js
helper script so no psycopg2 / hardcoded credentials are needed.
"""
import os
import json
import subprocess
from typing import Any

PROJECT_ID = "rough-paper-04751160"  # SoloLvlUp Intelligence Engine
TABLE_NAME = "master_quest_log_import"  # active data table
DATABASE   = "neondb"


def run_sql(sql: str) -> Any:
    """
    Execute SQL against NeonDB.
    Delegates to Node.js neon_runner.js which uses @neondatabase/serverless
    and reads DATABASE_URL from the environment (set via .env or system env).
    """
    runner = os.path.join(os.path.dirname(__file__), "neon_runner.js")
    payload = json.dumps({"sql": sql, "projectId": PROJECT_ID, "database": DATABASE})

    result = subprocess.run(
        ["node", runner],
        input=payload,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"[mcp_neon] SQL execution failed:\n{result.stderr}")

    try:
        data = json.loads(result.stdout)
        return data.get("rows", data)
    except json.JSONDecodeError:
        return []
