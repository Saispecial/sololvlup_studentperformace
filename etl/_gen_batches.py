import pandas as pd
import os

df = pd.read_csv('./01_data/master_quest_log.csv')
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
        ts = str(val)
        return "'" + ts + "'"
    if col in STR_COLS:
        return "'" + str(val).replace("'", "''") + "'"
    return str(val)

batch_size = 500
batches = [df[i:i+batch_size] for i in range(0, len(df), batch_size)]
print(f"Total rows: {len(df)}, batches: {len(batches)}")

for idx, batch in enumerate(batches):
    rows = []
    for _, row in batch.iterrows():
        vals = ', '.join(q(c, row[c]) for c in DB_COLS)
        rows.append('(' + vals + ')')
    sql = ('INSERT INTO fact_user_activity (' + ', '.join(DB_COLS) + ') VALUES\n'
           + ',\n'.join(rows) + ';')
    path = os.path.join('./etl', f'_batch_{idx:03d}.sql')
    with open(path, 'w', encoding='utf-8') as f:
        f.write(sql)

print("Done. Batch SQL files written to etl/")
