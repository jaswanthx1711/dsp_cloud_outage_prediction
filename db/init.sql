-- Predictions table
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    model_version VARCHAR(50) NOT NULL,
    source VARCHAR(20) DEFAULT 'webapp',
    cloud_provider VARCHAR(50),
    service VARCHAR(50),
    severity VARCHAR(20),
    start_time TIMESTAMPTZ,
    system_load_before_outage INT,
    number_of_customers_affected INT,
    ticket_count INT,
    backup_system_triggered VARCHAR(5),
    predicted_hours FLOAT NOT NULL,
    is_anomaly BOOLEAN DEFAULT FALSE,
    predicted_end_time TIMESTAMPTZ
);

-- Ingestion stats table
CREATE TABLE IF NOT EXISTS ingestion_stats (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    filename VARCHAR(255),
    total_rows INT,
    valid_rows INT,
    invalid_rows INT,
    criticality VARCHAR(10),
    error_types JSONB
);

-- Training runs table (Defense 2): one row per training DAG execution.
-- is_champion marks the currently promoted model; only its feature stats
-- (see training_stats below) should be used as the drift baseline.
CREATE TABLE IF NOT EXISTS training_runs (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMPTZ DEFAULT NOW(),
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(20) NOT NULL,
    is_champion BOOLEAN DEFAULT FALSE,
    rmse FLOAT,
    mae FLOAT,
    r2 FLOAT,
    inference_ms_per_row FLOAT,
    n_train INT,
    n_test INT,
    promoted_at TIMESTAMPTZ
);

-- Training feature stats table (Defense 2): per-feature training-time
-- statistics for the drift dashboard baseline. One row per feature per
-- training run. Numeric features populate stat_mean/std/min/max;
-- categorical features populate category_distribution instead.
CREATE TABLE IF NOT EXISTS training_stats (
    id SERIAL PRIMARY KEY,
    training_run_id INT NOT NULL REFERENCES training_runs(id) ON DELETE CASCADE,
    feature_name VARCHAR(100) NOT NULL,
    feature_type VARCHAR(20) NOT NULL,
    stat_mean FLOAT,
    stat_std FLOAT,
    stat_min FLOAT,
    stat_max FLOAT,
    category_distribution JSONB
);

CREATE INDEX IF NOT EXISTS idx_training_runs_champion ON training_runs (is_champion);
CREATE INDEX IF NOT EXISTS idx_training_stats_feature ON training_stats (feature_name);
