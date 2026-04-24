"""
ETL Load - Insert transformed data into NeonDB via MCP.

All DB operations go through mcp_neon.py which wraps the Neon MCP tools.
No psycopg2, no hardcoded credentials.
"""
import pandas as pd
from datetime import datetime
from mcp_neon import run_sql

# ---------------------------------------------------------------------------
# Table schema
# ---------------------------------------------------------------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fact_user_activity (
    id                     SERIAL PRIMARY KEY,
    user_id                VARCHAR(50),
    timestamp              TIMESTAMP,
    day_of_week            VARCHAR(20),
    hour_of_day            INT,
    activity_type          VARCHAR(50),
    time_spent_minutes     FLOAT,
    quiz_score             FLOAT,
    streak_days            INT,
    quests_completed_today INT,
    xp_earned_today        FLOAT,
    cumulative_xp          FLOAT,
    module_type            VARCHAR(50),
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

DB_COLS = [
    "user_id", "timestamp", "day_of_week", "hour_of_day",
    "activity_type", "time_spent_minutes", "quiz_score",
    "streak_days", "quests_completed_today", "xp_earned_today",
    "cumulative_xp", "module_type",
]


def ensure_table():
    print("[load] Ensuring fact_user_activity table exists...")
    run_sql(CREATE_TABLE_SQL)
    print("[load] Table ready.")


def get_latest_timestamp() -> datetime | None:
    rows = run_sql("SELECT MAX(timestamp) AS latest FROM fact_user_activity;")
    if rows and isinstance(rows, list) and rows[0].get("latest"):
        return pd.to_datetime(rows[0]["latest"])
    return None


def _quote(col: str, val) -> str:
    """Format a value for SQL insertion."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "NULL"
    if col == "timestamp":
        return f"'{pd.to_datetime(val)}'"
    if col in ("user_id", "day_of_week", "activity_type", "module_type"):
        return f"'{str(val).replace(chr(39), chr(39)*2)}'"
    return str(val)


def load(df: pd.DataFrame) -> int:
    ensure_table()

    # Smart insert: only rows newer than latest DB timestamp
    latest = get_latest_timestamp()
    if latest:
        print(f"[load] Latest DB timestamp: {latest}. Filtering new rows...")
        df = df[df["timestamp"] > latest]

    if df.empty:
        print("[load] No new rows to insert.")
        return 0

    print(f"[load] Inserting {len(df)} new rows...")

    rows_sql = []
    for _, row in df.iterrows():
        values = ", ".join(_quote(c, row.get(c)) for c in DB_COLS)
        rows_sql.append(f"({values})")

    cols_str = ", ".join(DB_COLS)
    insert_sql = (
        f"INSERT INTO fact_user_activity ({cols_str}) VALUES\n"
        + ",\n".join(rows_sql) + ";"
    )

    run_sql(insert_sql)
    print(f"[load] Done. {len(df)} rows inserted.")
    return len(df)


if __name__ == "__main__":
    from extract import extract
    from transform import transform
    df = transform(extract())
    load(df)
