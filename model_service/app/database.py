import os
from sqlalchemy import create_engine, Column, Integer, Float, Boolean, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    model_version = Column(String(50), nullable=False)
    source = Column(String(20), default="webapp")
    cloud_provider = Column(String(50))
    service = Column(String(50))
    severity = Column(String(20))
    start_time = Column(DateTime(timezone=True), nullable=True)
    system_load_before_outage = Column(Integer)
    number_of_customers_affected = Column(Integer)
    ticket_count = Column(Integer)
    backup_system_triggered = Column(String(5))
    predicted_hours = Column(Float, nullable=False)
    is_anomaly = Column(Boolean, default=False)
    predicted_end_time = Column(DateTime(timezone=True), nullable=True)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
