"""
Retrain all 3 models from master_quest_log_import.csv and save fresh .pkl files.
"""
import pandas as pd
import pickle
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.preprocessing import LabelEncoder

DATA_PATH    = "./01_data/master_quest_log_import.csv"
MODELS_02    = "./02_models/"
MODELS_03    = "./03_models/"

FEATURES = [
    "hour_of_day", "time_spent_minutes", "quiz_score",
    "streak_days", "quests_completed_today",
    "xp_earned_today", "cumulative_xp"
]

print("Loading data...")
df = pd.read_csv(DATA_PATH)
df = df.fillna(0)

X = df[FEATURES].values

# --- Engagement model (classifier: high/low engagement) ---
# Label: quiz_score >= 60 → engaged
y_engagement = (df["quiz_score"] >= 60).astype(int).values
engagement_model = RandomForestClassifier(n_estimators=100, random_state=42)
engagement_model.fit(X, y_engagement)
with open(MODELS_02 + "engagement_model_v2.pkl", "wb") as f:
    pickle.dump(engagement_model, f)
print("engagement_model_v2.pkl saved")

# --- Dropout model (classifier: at-risk if streak_days <= 1 and low xp) ---
y_dropout = ((df["streak_days"] <= 1) & (df["xp_earned_today"] < 200)).astype(int).values
dropout_model = RandomForestClassifier(n_estimators=100, random_state=42)
dropout_model.fit(X, y_dropout)
with open(MODELS_02 + "dropout_model_v2.pkl", "wb") as f:
    pickle.dump(dropout_model, f)
print("dropout_model_v2.pkl saved")

# --- XP regression model ---
y_xp = df["xp_earned_today"].values
xp_model = GradientBoostingRegressor(n_estimators=100, random_state=42)
xp_model.fit(X, y_xp)
with open(MODELS_03 + "xp_regression_model.pkl", "wb") as f:
    pickle.dump(xp_model, f)
print("xp_regression_model.pkl saved")

print("\nAll models retrained and saved successfully.")
