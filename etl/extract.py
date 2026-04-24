"""
ETL Extract - Load raw CSV data from 01_data/
"""
import pandas as pd
import os

DATA_PATH = "./01_data/"

def extract() -> pd.DataFrame:
    filepath = os.path.join(DATA_PATH, "master_quest_log.csv")
    print(f"[extract] Loading: {filepath}")
    df = pd.read_csv(filepath)
    print(f"[extract] {len(df)} rows loaded. Columns: {list(df.columns)}")
    return df

if __name__ == "__main__":
    print(extract().head())
