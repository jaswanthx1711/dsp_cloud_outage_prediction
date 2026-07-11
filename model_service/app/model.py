import time
import pandas as pd
import numpy as np
import joblib
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

MODEL_PATH = "/data/model.pkl"
MODEL_VERSION = "v1.0"

NUMERIC_FEATURES = ["system_load_before_outage", "number_of_customers_affected", "ticket_count", "backup_system_triggered_enc"]
CATEGORICAL_FEATURES = ["cloud_provider", "service", "severity"]
FEATURE_COLUMNS = ["cloud_provider", "service", "severity", "system_load_before_outage",
                    "number_of_customers_affected", "ticket_count", "backup_system_triggered"]
TARGET_COLUMN = "duration_minutes"


def encode_backup(X):
    """
    Transforms the 'backup_system_triggered' feature from 'Yes'/'No'
    strings into binary (1/0) numerical values.
    """
    X = X.copy()
    X["backup_system_triggered_enc"] = X["backup_system_triggered"].map({"Yes": 1, "No": 0}).fillna(0)
    return X


def build_pipeline() -> Pipeline:
    """
    Construct the (unfit) preprocessing + model pipeline. This is the single
    source of truth for the feature pipeline shape, shared by the API's
    bootstrap trainer and the Airflow training DAG so serving and training
    never drift apart.
    """
    preprocessor = ColumnTransformer([
        ("num", StandardScaler(), ["system_load_before_outage", "number_of_customers_affected", "ticket_count", "backup_system_triggered_enc"]),
        ("cat", OneHotEncoder(handle_unknown="ignore"), ["cloud_provider", "service", "severity"]),
    ])
    encoder = FunctionTransformer(encode_backup, validate=False)

    return Pipeline([
        ("encode_backup", encoder),
        ("preprocessor", preprocessor),
        ("model", RandomForestRegressor(n_estimators=100, random_state=42)),
    ])


def prepare_training_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Drop incomplete rows and split an already-loaded dataframe into (X, y)."""
    df = df.dropna(subset=["duration_minutes", "cloud_provider", "service", "severity",
                            "system_load_before_outage", "number_of_customers_affected",
                            "ticket_count", "backup_system_triggered"])
    X = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN] / 60.0  # predict hours
    return X, y


def load_training_data(csv_path: str) -> tuple[pd.DataFrame, pd.Series]:
    """Read a dataset CSV and return (X, y) ready for train_test_split."""
    return prepare_training_frame(pd.read_csv(csv_path))


def compute_metrics(y_true, y_pred) -> dict:
    """RMSE (primary promotion metric), MAE, and R2 on a held-out test set."""
    return {
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
    }


def train_and_evaluate(data, test_size: float = 0.2, random_state: int = 42):
    """
    Train a fresh pipeline on `data` and return (pipeline, metrics).

    `data` may be a CSV path (str) or an already-loaded pd.DataFrame — the
    training DAG passes a DataFrame it has already assembled in memory, the
    API's bootstrap trainer passes a path.

    metrics includes rmse/mae/r2 on the held-out test split, plus
    inference_ms_per_row (average single-row prediction latency) used by the
    training DAG's promotion criteria, and n_train/n_test for traceability.
    """
    X, y = load_training_data(data) if isinstance(data, str) else prepare_training_frame(data)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    metrics = compute_metrics(y_test, y_pred)

    # Average per-row inference latency, measured the same way the API serves single rows.
    start = time.perf_counter()
    for _, row in X_test.head(50).iterrows():
        pipeline.predict(pd.DataFrame([row]))
    elapsed = time.perf_counter() - start
    metrics["inference_ms_per_row"] = round((elapsed / min(50, len(X_test))) * 1000, 4)
    metrics["n_train"] = len(X_train)
    metrics["n_test"] = len(X_test)

    return pipeline, metrics


def train_and_save(csv_path: str):
    """
    Defense-1-style bootstrap trainer: train, log metrics, persist to a local
    file. Used only as a cold-start fallback so the API always has a model
    even before a champion exists in the MLflow registry.
    """
    pipeline, metrics = train_and_evaluate(csv_path)
    print(f"[train_and_save] test metrics: {metrics}")

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)
    print(f"Model trained and saved to {MODEL_PATH}")
    return pipeline


def load_model():
    """
    Attempts to load a previously trained ML model from the data volume.
    Returns (model, version) or (None, None) if not found.
    """
    if os.path.exists(MODEL_PATH):
        return joblib.load(MODEL_PATH), MODEL_VERSION
    return None, None


def predict(model, features: dict) -> float:
    """
    Transforms dictionary features into a DataFrame and returns the
    single prediction produced by the trained Random Forest model.
    """
    df = pd.DataFrame([features])
    return float(model.predict(df)[0])
