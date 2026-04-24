"""
Direct insert into NeonDB using the Neon HTTP SQL API.
No psycopg2. No subprocess. Just urllib + the connection string from .env
"""
import os
import json
import urllib.request
import urllib.parse
import pandas as pd
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

# Parse DATABASE_URL to get Neon HTTP API endpoint + auth
DATABASE_URL = os.environ['DATABASE_URL']
# Format: postgresql://user:pass@host/db?...
# Neon HTTP API: https://<host>/sql  with Basic auth user:pass
import re
m = re.match(r'postgresql://([^:]+):([^@]+)@([^/]+)/([^?]+)', DATABASE_URL)
if not m:
    raise ValueError(f"Cannot parse DATABASE_URL: {DATABASE_URL}")
DB_USER, DB_PASS, DB_HOST, DB_NAME = m.group(1), m.group(2), m.group(3), m.group(4)

API_URL = f"https://{DB_HOST}/sql"

def run_sql(sql: str):
    payload = json.dumps({"query": sql}).encode()
    req = urllib.request.Request(
        API_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {DB_PASS}",
            "Neon-Connection-String": DATABASE_URL,
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise RuntimeError(f"HTTP {e.code}: {body}")


def main():
    # Load and transform CSV
    csv_path = os.path.join(os.path.dirname(__file__), '../01_data/master_quest_log.csv')
    df = pd.read_csv(csv_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.fillna(0)
    if 'student_id' in df.columns:
        df = df.rename(columns={'student_id': 'user_id'})

    DB_COLS = [
        'user_id', 'timestamp', 'day_of_week', 'hour_of_day', 'activity_type',
        'time_spent_minutes', 'quiz_score', 'streak_days', 'quests_completed_today',
        'xp_earned_today', 'cumulative_xp', 'module_type'
    ]
    STR_COLS = {'user_id', 'day_of_week', 'activity_type', 'module_type'}

    def q(col, val):
        if col == 'timestamp':
            return "'" + str(val) + "'"
        if col in STR_COLS:
            return "'" + str(val).replace("'", "''") + "'"
        return str(val)

    # Check latest timestamp to avoid duplicates
    try:
        result = run_sql("SELECT MAX(timestamp) AS latest FROM fact_user_activity;")
        latest_raw = result.get('rows', [{}])[0].get('latest')
        latest = pd.to_datetime(latest_raw) if latest_raw else None
    except Exception as e:
        print(f"[warn] Could not check latest timestamp: {e}")
        latest = None

    if latest:
        print(f"[insert] Latest DB timestamp: {latest}. Filtering new rows...")
        df = df[df['timestamp'] > latest]

    if df.empty:
        print("[insert] No new rows to insert.")
        return

    print(f"[insert] Inserting {len(df)} rows in batches of 200...")

    batch_size = 200
    total_inserted = 0
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        rows = []
        for _, row in batch.iterrows():
            vals = ', '.join(q(c, row[c]) for c in DB_COLS)
            rows.append('(' + vals + ')')
        sql = ('INSERT INTO fact_user_activity (' + ', '.join(DB_COLS) + ') VALUES\n'
               + ',\n'.join(rows) + ';')
        try:
            run_sql(sql)
            total_inserted += len(batch)
            print(f"[insert] Batch {i//batch_size + 1}: {total_inserted}/{len(df)} rows inserted")
        except Exception as e:
            print(f"[insert] Batch {i//batch_size + 1} FAILED: {e}")
            raise

    print(f"[insert] Done. {total_inserted} rows inserted into NeonDB.")


if __name__ == '__main__':
    main()
