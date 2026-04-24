"""
ML Inference Service (FastAPI)
Models are loaded ONCE at startup — not per request.

Run with:
  uvicorn ml_service:app --host 0.0.0.0 --port 8000
"""
import pickle
import os
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

MODELS_PATH_02 = "./02_models/"
MODELS_PATH_03 = "./03_models/"

app = FastAPI(title="SoloLvlUp ML Service", version="1.0.0")

# Load models ONCE at startup
def _load(path: str):
    with open(path, "rb") as f:
        return pickle.load(f)

engagement_model = _load(os.path.join(MODELS_PATH_02, "engagement_model_v2.pkl"))
dropout_model    = _load(os.path.join(MODELS_PATH_02, "dropout_model_v2.pkl"))
xp_model         = _load(os.path.join(MODELS_PATH_03, "xp_regression_model.pkl"))

print("[ml_service] Models loaded: engagement v2, dropout v2, xp regression")

class PredictRequest(BaseModel):
    hour_of_day:            float
    time_spent_minutes:     float
    quiz_score:             float
    streak_days:            float
    quests_completed_today: float
    xp_earned_today:        float
    cumulative_xp:          float

class PredictResponse(BaseModel):
    engagement_score: float
    dropout_risk:     float
    predicted_xp:     float

@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    features = np.array([[
        req.hour_of_day, req.time_spent_minutes, req.quiz_score,
        req.streak_days, req.quests_completed_today,
        req.xp_earned_today, req.cumulative_xp,
    ]])
    try:
        engagement_score = float(engagement_model.predict(features)[0])
        dropout_risk     = float(dropout_model.predict_proba(features)[0][1])
        predicted_xp     = float(xp_model.predict(features)[0])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return PredictResponse(
        engagement_score=engagement_score,
        dropout_risk=dropout_risk,
        predicted_xp=predicted_xp,
    )

@app.get("/health")
def health():
    return {"status": "ok"}
