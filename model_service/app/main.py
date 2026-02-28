import sys
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import random

from app.database import get_db, Base, engine, Prediction
from app.schemas import (
    BatchPredictionRequest, BatchPredictionResponse,
    PredictionResult, PastPrediction
)
from app.model import load_model, predict, train_and_save

# Global model state
model_state = {"model": None, "version": "v1.0"}

DATASET_PATH = "/data/cloud_outages_dataset.csv"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: load or train model
    Base.metadata.create_all(bind=engine)
    model, version = load_model()
    if model is None:
        print("No model found — training from dataset...")
        if os.path.exists(DATASET_PATH):
            model = train_and_save(DATASET_PATH)
            version = "v1.0"
        else:
            print("WARNING: No dataset found. Predictions will fail until model is trained.")
    model_state["model"] = model
    model_state["version"] = version
    print(f"Model loaded: {version}")
    yield
    # Shutdown cleanup (nothing needed)


app = FastAPI(title="Cloud Outage Prediction API", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "healthy", "model_loaded": model_state["model"] is not None}


@app.post("/predict", response_model=BatchPredictionResponse)
def make_predictions(request: BatchPredictionRequest, db: Session = Depends(get_db)):
    if model_state["model"] is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    results = []
    for feat in request.features:
        features_dict = feat.model_dump()
        try:
            hours = predict(model_state["model"], features_dict)
            is_anomaly = hours > 5.0
            # Generate random future start_time (1-90 days from now)
            days_ahead = random.randint(1, 90)
            hours_ahead = random.randint(0, 23)
            minutes_ahead = random.randint(0, 59)
            start_time = (datetime.now() + timedelta(days=days_ahead, hours=hours_ahead, minutes=minutes_ahead)).replace(microsecond=0)
            predicted_end_time = (start_time + timedelta(hours=hours)).replace(microsecond=0)

            # Save to DB
            record = Prediction(
                model_version=model_state["version"],
                source=request.source,
                cloud_provider=feat.cloud_provider,
                service=feat.service,
                severity=feat.severity,
                start_time=start_time,
                system_load_before_outage=feat.system_load_before_outage,
                number_of_customers_affected=feat.number_of_customers_affected,
                ticket_count=feat.ticket_count,
                backup_system_triggered=feat.backup_system_triggered,
                predicted_hours=hours,
                is_anomaly=is_anomaly,
                predicted_end_time=predicted_end_time,
            )
            db.add(record)

            results.append(PredictionResult(
                cloud_provider=feat.cloud_provider,
                service=feat.service,
                severity=feat.severity,
                start_time=start_time,
                system_load_before_outage=feat.system_load_before_outage,
                number_of_customers_affected=feat.number_of_customers_affected,
                ticket_count=feat.ticket_count,
                backup_system_triggered=feat.backup_system_triggered,
                predicted_hours=round(hours, 2),
                is_anomaly=is_anomaly,
                model_version=model_state["version"],
                predicted_end_time=predicted_end_time,
            ))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    db.commit()
    return BatchPredictionResponse(predictions=results)


@app.get("/past-predictions", response_model=List[PastPrediction])
def past_predictions(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    source: Optional[str] = Query(default="all"),
    limit: int = Query(default=100, le=500),
    db: Session = Depends(get_db),
):
    query = db.query(Prediction)
    if source and source != "all":
        query = query.filter(Prediction.source == source)
    if start_date:
        query = query.filter(Prediction.timestamp >= start_date)
    if end_date:
        query = query.filter(Prediction.timestamp <= end_date)
    return query.order_by(Prediction.timestamp.desc()).limit(limit).all()
