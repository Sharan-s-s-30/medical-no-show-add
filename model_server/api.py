from fastapi import FastAPI
from fastapi import Request
import joblib

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from pydantic import BaseModel
from typing import List, Union

model = joblib.load("models/no_show_model.joblib")
load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))
app = FastAPI()
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
class FeatureRow(BaseModel):
    age: int
    wait_days: int
    scheduled_hour: int
    appointment_weekday: int
    gender: str
    neighbourhood: str
    age_group: str


@app.get("/")
def health():
    return {"status": "ok"}
    
@app.get("/processed-data")
def processed_data(limit: int = 100, offset: int = 0):
    df = pd.read_sql(
        "SELECT * FROM processed_appointments ORDER BY appointment_id "
        f"LIMIT {limit} OFFSET {offset}",
        engine)
    return df.to_dict(orient="records")
    
@app.post("/predict")
async def predict(request: Request):
    body = await request.json()
    if isinstance(body, dict):
        body = [body]
    df = pd.DataFrame(body)
    probs = model.predict_proba(df)[:, 1].tolist()
    return [{"prediction": p} for p in probs]


@app.get("/test-data/random")
def random_test_data():
    df = pd.read_sql(
        "SELECT * FROM test_appointments ORDER BY random() LIMIT 1",
        engine)
    return df.iloc[0].to_dict()