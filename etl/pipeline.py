"""
ETL Pipeline - Orchestrates extract → transform → load.

Automation logic:
  - Checks if source CSV was modified since last run (file mtime)
  - Skips run if no new data detected
  - Stores last-run timestamp in .pipeline_state.json
"""
import os
import json
import time
from datetime import datetime

STATE_FILE = ".pipeline_state.json"
DATA_FILE  = "./01_data/master_quest_log.csv"


def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def _save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def has_new_data() -> bool:
    state = _load_state()
    last_mtime = state.get("last_mtime", 0)
    current_mtime = os.path.getmtime(DATA_FILE)
    return current_mtime > last_mtime


def run_pipeline(force: bool = False):
    print(f"\n{'='*50}")
    print(f"[pipeline] SoloLvlUp ETL — {datetime.now()}")
    print(f"{'='*50}")

    if not force and not has_new_data():
        print("[pipeline] No new data detected. Skipping run.")
        return

    from extract import extract
    from transform import transform
    from load import load

    df = extract()
    df = transform(df)
    inserted = load(df)

    # Update state
    state = _load_state()
    state["last_mtime"]  = os.path.getmtime(DATA_FILE)
    state["last_run"]    = datetime.now().isoformat()
    state["rows_inserted"] = inserted
    _save_state(state)

    print(f"[pipeline] Complete. {inserted} rows inserted.")


if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv
    run_pipeline(force=force)
