import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from app.database import get_db, Base, engine, Prediction
from app.schemas import (
    BatchPredictionRequest, BatchPredictionResponse,
    PredictionResult, PastPrediction
)
from app.model import load_model, predict, train_and_save

model_state = {"model": None, "version": "v1.0"}

DATASET_PATH = "/data/cloud_outages_dataset.csv"

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.getenv("MODEL_NAME", "cloud_outage_duration")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


def load_champion_from_registry():
    """
    Try to load the @champion-aliased model from the MLflow model registry.
    Returns (model, version) or (None, None) if no champion has been
    promoted yet (e.g. the training DAG hasn't run a first successful pass).
    """
    try:
        model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
        client = MlflowClient()
        mv = client.get_model_version_by_alias(MODEL_NAME, "champion")
        return model, mv.version
    except (MlflowException, OSError) as e:
        print(f"[mlflow] No champion model available yet: {e}")
        return None, None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler for the FastAPI application.
    Creates database tables, then tries to load the current @champion model
    from the MLflow registry. If no champion has been promoted yet (first
    ever startup, before the training DAG has run), falls back to the local
    bootstrap model so the API is usable immediately.
    """
    # Create database tables if they do not exist
    Base.metadata.create_all(bind=engine)

    model, version = load_champion_from_registry()
    if model is None:
        # Bootstrap fallback: load or train a local model from the static
        # dataset so the API works before any training DAG run exists.
        model, version = load_model()
        if model is None and os.path.exists(DATASET_PATH):
            model = train_and_save(DATASET_PATH)
            version = "bootstrap-v1"

    # Update global model state
    model_state["model"] = model
    model_state["version"] = str(version) if version else "unknown"
    yield


app = FastAPI(title="Cloud Outage Prediction API", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "healthy", "model_loaded": model_state["model"] is not None}


@app.post("/reload-model")
def reload_model():
    """
    Reload the current @champion model from the MLflow registry without
    restarting the container. Called by the training DAG's
    notify_api_reload task after a successful promotion.
    """
    model, version = load_champion_from_registry()
    if model is None:
        raise HTTPException(status_code=404, detail="No champion model found in MLflow registry")

    model_state["model"] = model
    model_state["version"] = str(version)
    return {"status": "reloaded", "model_version": model_state["version"]}


@app.post("/predict", response_model=BatchPredictionResponse)
def make_predictions(request: BatchPredictionRequest, db: Session = Depends(get_db)):
    """
    Accepts single or batch prediction requests.
    Saves predictions to PostgreSQL and returns the results.
    """
    if model_state["model"] is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    results = []
    
    # Iterate over batch of features
    for feat in request.features:
        features_dict = feat.model_dump()
        try:
            # Predict the outage duration
            hours = predict(model_state["model"], features_dict)
            is_anomaly = hours > 5.0 # Consider > 5 hours an anomaly

            # Create a database record for this prediction
            record = Prediction(
                model_version=model_state["version"],
                source=request.source,
                cloud_provider=feat.cloud_provider,
                service=feat.service,
                severity=feat.severity,
                system_load_before_outage=feat.system_load_before_outage,
                number_of_customers_affected=feat.number_of_customers_affected,
                ticket_count=feat.ticket_count,
                backup_system_triggered=feat.backup_system_triggered,
                predicted_hours=hours,
                is_anomaly=is_anomaly,
            )
            db.add(record)

            results.append(PredictionResult(
                **features_dict,
                predicted_hours=round(hours, 2),
                is_anomaly=is_anomaly,
                model_version=model_state["version"],
            ))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Commit all new predictions to the DB
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
    """
    Retrieve historical predictions from the database with optional filtering 
    by date and prediction source (e.g., scheduled vs webapp).
    """
    query = db.query(Prediction)
    
    # Apply filters dynamically 
    if source and source != "all":
        query = query.filter(Prediction.source == source)
    if start_date:
        query = query.filter(Prediction.timestamp >= start_date)
    if end_date:
        query = query.filter(Prediction.timestamp <= end_date)
        
    return query.order_by(Prediction.timestamp.desc()).limit(limit).all()
