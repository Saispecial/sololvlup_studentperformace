"""
ETL Transform - Clean and enrich raw quest log data
"""
import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    print(f"[transform] Processing {len(df)} rows...")

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date']      = df['timestamp'].dt.date
    df['hour']      = df['timestamp'].dt.hour
    df              = df.fillna(0)

    # Normalize column name: student_id -> user_id
    if 'student_id' in df.columns and 'user_id' not in df.columns:
        df = df.rename(columns={'student_id': 'user_id'})

    # Ensure hour_of_day exists
    if 'hour_of_day' not in df.columns:
        df['hour_of_day'] = df['hour']

    print(f"[transform] Done. Final columns: {list(df.columns)}")
    return df

if __name__ == "__main__":
    from extract import extract
    print(transform(extract()).head())
