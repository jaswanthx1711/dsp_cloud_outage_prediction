import pandas as pd
import numpy as np
import joblib
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer
from sklearn.model_selection import train_test_split

MODEL_PATH = "/data/model.pkl"
MODEL_VERSION = "v1.0"

TIME_FEATURES = ["start_hour", "start_dayofweek", "start_month"]
NUMERIC_FEATURES = [
    "system_load_before_outage",
    "number_of_customers_affected",
    "ticket_count",
    "backup_system_triggered_enc",
] + TIME_FEATURES
CATEGORICAL_FEATURES = ["cloud_provider", "service", "severity"]


def encode_backup(X):
    X = X.copy()
    X["backup_system_triggered_enc"] = X["backup_system_triggered"].map({"Yes": 1, "No": 0}).fillna(0)
    if "start_time" in X.columns:
        start_times = pd.to_datetime(X["start_time"], errors="coerce")
        start_times = start_times.fillna(pd.Timestamp.utcnow())
    else:
        start_times = pd.Series(pd.Timestamp.utcnow(), index=X.index)
    X["start_hour"] = start_times.dt.hour
    X["start_dayofweek"] = start_times.dt.dayofweek
    X["start_month"] = start_times.dt.month
    return X


def train_and_save(csv_path: str):
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=["duration_minutes", "cloud_provider", "service", "severity",
                            "system_load_before_outage", "number_of_customers_affected",
                            "ticket_count", "backup_system_triggered"])

    df["backup_system_triggered_enc"] = df["backup_system_triggered"].map({"Yes": 1, "No": 0}).fillna(0)

    X = df[["cloud_provider", "service", "severity", "system_load_before_outage",
            "number_of_customers_affected", "ticket_count", "backup_system_triggered", "start_time"]]
    y = df["duration_minutes"] / 60.0  # predict hours

    preprocessor = ColumnTransformer([
        ("num", StandardScaler(), NUMERIC_FEATURES),
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
    ])

    encoder = FunctionTransformer(encode_backup, validate=False)

    pipeline = Pipeline([
        ("encode_backup", encoder),
        ("preprocessor", preprocessor),
        ("model", RandomForestRegressor(n_estimators=100, random_state=42)),
    ])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    pipeline.fit(X_train, y_train)

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)
    print(f"Model trained and saved to {MODEL_PATH}")
    return pipeline


def load_model():
    if os.path.exists(MODEL_PATH):
        return joblib.load(MODEL_PATH), MODEL_VERSION
    return None, None


def predict(model, features: dict) -> float:
    df = pd.DataFrame([features])
    return float(model.predict(df)[0])
