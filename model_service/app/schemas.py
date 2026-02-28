from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class PredictionRequest(BaseModel):
    cloud_provider: str
    service: str
    severity: str
    start_time: Optional[datetime] = None
    system_load_before_outage: Optional[int] = 50
    number_of_customers_affected: int
    ticket_count: int
    backup_system_triggered: str


class BatchPredictionRequest(BaseModel):
    features: List[PredictionRequest]
    source: str = "webapp"


class PredictionResult(BaseModel):
    cloud_provider: str
    service: str
    severity: str
    start_time: Optional[datetime]
    system_load_before_outage: Optional[int]
    number_of_customers_affected: int
    ticket_count: int
    backup_system_triggered: str
    predicted_hours: float
    is_anomaly: bool
    model_version: str
    predicted_end_time: Optional[datetime] = None


class BatchPredictionResponse(BaseModel):
    predictions: List[PredictionResult]


class PastPrediction(BaseModel):
    id: int
    timestamp: datetime
    model_version: str
    source: str
    cloud_provider: Optional[str]
    service: Optional[str]
    severity: Optional[str]
    start_time: Optional[datetime]
    system_load_before_outage: Optional[int]
    number_of_customers_affected: Optional[int]
    ticket_count: Optional[int]
    backup_system_triggered: Optional[str]
    predicted_hours: float
    is_anomaly: bool
    predicted_end_time: Optional[datetime] = None

    class Config:
        from_attributes = True
