"""
Scheduler - Runs the ETL pipeline on a schedule or when new data is detected.

Usage:
  python scheduler.py              # watch mode (checks every 5 min)
  python scheduler.py --once       # single run
  python scheduler.py --force      # force run ignoring mtime check
"""
import sys
import time
import subprocess
from datetime import datetime

INTERVAL_SECONDS = 300  # 5 minutes


def run_pipeline(force: bool = False):
    cmd = [sys.executable, "etl/pipeline.py"]
    if force:
        cmd.append("--force")
    print(f"\n[scheduler] Triggering pipeline at {datetime.now()}")
    result = subprocess.run(cmd, cwd=".")
    if result.returncode != 0:
        print("[scheduler] Pipeline exited with errors.")
    else:
        print("[scheduler] Pipeline completed successfully.")


if __name__ == "__main__":
    force = "--force" in sys.argv
    once  = "--once"  in sys.argv

    if once or force:
        run_pipeline(force=force)
    else:
        print(f"[scheduler] Watch mode — checking every {INTERVAL_SECONDS}s")
        while True:
            run_pipeline()
            time.sleep(INTERVAL_SECONDS)
