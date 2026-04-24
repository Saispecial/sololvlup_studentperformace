"""
One-time seed script: loads all CSV data into NeonDB using psycopg2.
This is a utility script only — production load.py uses MCP.
"""
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

try:
    import psycopg2
except ImportError:
    print("Installing psycopg2-binary...")
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "-q"])
    import psycopg2

DATABASE_URL = os.environ['DATABASE_URL']

df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../01_data/master_quest_log.csv'))
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.fillna(0)
if 'student_id' in df.columns:
    df = df.rename(columns={'student_id': 'user_id'})

DB_COLS = [
    'user_id', 'timestamp', 'day_of_week', 'hour_of_day', 'activity_type',
    'time_spent_minutes', 'quiz_score', 'streak_days', 'quests_completed_today',
    'xp_earned_today', 'cumulative_xp', 'module_type'
]

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Check latest timestamp
cur.execute("SELECT MAX(timestamp) FROM fact_user_activity;")
latest = cur.fetchone()[0]
if latest:
    import pandas as pd
    latest = pd.to_datetime(latest)
    print(f"Latest DB timestamp: {latest}. Filtering new rows...")
    df = df[df['timestamp'] > latest]

if df.empty:
    print("No new rows to insert.")
    cur.close(); conn.close()
    exit(0)

print(f"Inserting {len(df)} rows...")

cols = ', '.join(DB_COLS)
placeholders = ', '.join(['%s'] * len(DB_COLS))
insert_sql = f"INSERT INTO fact_user_activity ({cols}) VALUES ({placeholders})"

batch_size = 500
for i in range(0, len(df), batch_size):
    batch = df.iloc[i:i+batch_size]
    rows = [tuple(row[c] for c in DB_COLS) for _, row in batch.iterrows()]
    cur.executemany(insert_sql, rows)
    conn.commit()
    print(f"  Inserted rows {i+1}–{min(i+batch_size, len(df))}")

cur.close()
conn.close()
print(f"Done. {len(df)} rows inserted into NeonDB.")
