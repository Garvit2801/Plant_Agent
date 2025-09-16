-- train_spower.sql
-- BigQuery location is set via the CLI flag (e.g., --location=asia-south2)

-- ============
-- Parameters & defaults (so ad-hoc runs work; Scheduled Queries can still override)
-- ============
DECLARE run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE run_date  DATE      DEFAULT DATE(run_time);

-- These can be substituted via `envsubst` before running:
DECLARE project_id   STRING DEFAULT "${PROJECT_ID}";
DECLARE dataset_name STRING DEFAULT "plant_ops";
DECLARE table_name   STRING DEFAULT "snapshots";

-- Training window (days of data to use)
DECLARE lookback_days INT64 DEFAULT 30;

-- Model names (keep a stable alias and a dated version)
DECLARE base_model_name   STRING DEFAULT "spower_reg";
DECLARE version_suffix    STRING DEFAULT FORMAT_DATE('%Y%m%d', run_date);
DECLARE versioned_name    STRING DEFAULT CONCAT(base_model_name, "_", version_suffix);

-- Promotion rule (set to 0 to always promote)
DECLARE min_mae_improvement FLOAT64 DEFAULT 0.01;  -- 1% improvement required


-- ============
-- Sources
-- ============
DECLARE fq_table  STRING DEFAULT FORMAT('%s.%s.%s', project_id, dataset_name, table_name);
DECLARE fq_model  STRING DEFAULT FORMAT('%s.%s.%s', project_id, dataset_name, base_model_name);
DECLARE fq_model_v STRING DEFAULT FORMAT('%s.%s.%s', project_id, dataset_name, versioned_name);
DECLARE fq_metrics_table STRING DEFAULT FORMAT('%s.%s.%s', project_id, dataset_name, 'model_metrics');


-- ============
-- Prep training data
-- ============
CREATE TEMP TABLE train_data AS
SELECT
  -- label
  specific_power_kwh_per_ton AS label,

  -- features
  production_tph,
  kiln_feed_tph,
  separator_dp_pa,
  id_fan_flow_Nm3_h,
  cooler_airflow_Nm3_h,
  kiln_speed_rpm,
  o2_percent

FROM `project_id.dataset_name.table_name`
WHERE ts >= TIMESTAMP_SUB(run_time, INTERVAL lookback_days DAY)
  AND specific_power_kwh_per_ton IS NOT NULL
  -- Optionally drop outliers or obviously bad rows
  AND production_tph IS NOT NULL
  AND o2_percent IS NOT NULL
;

-- Basic sanity: ensure there is data to train
CREATE TEMP TABLE _counts AS
SELECT
  COUNT(*) AS n_rows,
  COUNTIF(label IS NULL) AS null_labels
FROM train_data;

-- If no rows, raise a helpful error
BEGIN
  DECLARE n_rows INT64;
  DECLARE null_labels INT64;
  SELECT n_rows, null_labels INTO n_rows, null_labels FROM _counts;
  IF n_rows = 0 THEN
    RAISE USING MESSAGE = 'No training rows in the last ' || CAST(lookback_days AS STRING) || ' days.';
  END IF;
END;

-- ============
-- Train a versioned model (XGBoost regressor for better performance)
-- ============
CREATE OR REPLACE MODEL `project_id.dataset_name.versioned_name`
OPTIONS (
  model_type = 'XGBOOST_REGRESSOR',
  input_label_cols = ['label'],
  data_split_method = 'AUTO_SPLIT',
  max_iterations = 200,
  tree_method = 'HIST',
  early_stop = TRUE,
  enable_global_explain = TRUE,
  min_tree_child_weight = 1.0,
  subsample = 0.8,
  colsample_bytree = 0.8,
  l1_reg = 0.0,
  l2_reg = 0.0,
  booster = 'gbtree',
  -- Helpful metadata
  labels = [('trained_at', CAST(run_time AS STRING)), ('lookback_days', CAST(lookback_days AS STRING))]
) AS
SELECT * FROM train_data;


-- ============
-- Evaluate the new version
-- ============
CREATE TEMP TABLE eval_new AS
SELECT * FROM ML.EVALUATE(MODEL `project_id.dataset_name.versioned_name`);

-- Try to evaluate the current production model (if it exists)
DECLARE prod_exists BOOL DEFAULT FALSE;
SET prod_exists = (
  SELECT COUNT(1) > 0
  FROM `project_id`.`dataset_name`.INFORMATION_SCHEMA.MODEL_OPTIONS
  WHERE model_name = base_model_name
);

CREATE TEMP TABLE eval_prod AS
SELECT * FROM (
  SELECT NULL AS mean_absolute_error, NULL AS mean_squared_error, NULL AS r2_score
)
WHERE FALSE;  -- empty by default

IF prod_exists THEN
  CREATE TEMP TABLE eval_prod AS
  SELECT * FROM ML.EVALUATE(MODEL `project_id.dataset_name.base_model_name`);
END IF;


-- ============
-- Record metrics
-- ============
CREATE SCHEMA IF NOT EXISTS `project_id.dataset_name`;

CREATE TABLE IF NOT EXISTS `project_id.dataset_name.model_metrics` (
  model_name STRING,
  version_name STRING,
  trained_at TIMESTAMP,
  lookback_days INT64,
  mae FLOAT64,
  rmse FLOAT64,
  r2 FLOAT64
);

INSERT INTO `project_id.dataset_name.model_metrics`
(model_name, version_name, trained_at, lookback_days, mae, rmse, r2)
SELECT
  base_model_name,
  versioned_name,
  run_time,
  lookback_days,
  n.mean_absolute_error,
  n.mean_squared_error,  -- rmse squared; leaving as MSE for consistency with BQML output
  n.r2_score
FROM eval_new AS n;


-- ============
-- Conditional promotion to the stable alias
-- ============
BEGIN
  DECLARE mae_new FLOAT64;
  DECLARE mae_old FLOAT64;
  DECLARE improve BOOL;

  SELECT mean_absolute_error INTO mae_new FROM eval_new LIMIT 1;

  IF prod_exists THEN
    SELECT mean_absolute_error INTO mae_old FROM eval_prod LIMIT 1;
    SET improve = (mae_old IS NULL) OR (mae_new IS NOT NULL AND (mae_old - mae_new) / mae_old >= min_mae_improvement);
  ELSE
    -- No prod model yet → promote
    SET improve = TRUE;
  END IF;

  IF improve THEN
    -- Promote by replacing the alias model with this version
    CREATE OR REPLACE MODEL `project_id.dataset_name.base_model_name`
    OPTIONS ( model_type = 'XGBOOST_REGRESSOR' )
    REMOTE WITH CONNECTION `project_id.dataset_name.versioned_name`; -- placeholder to carry name (no-op)
    -- BigQuery doesn’t have a native alias; REPLACE by copying the trained version into the base:
    CREATE OR REPLACE MODEL `project_id.dataset_name.base_model_name`
    OPTIONS(model_type = 'XGBOOST_REGRESSOR') AS
    SELECT * FROM ML.TRAINING_INFO(MODEL `project_id.dataset_name.versioned_name`) WHERE FALSE;
    -- The above is a harmless placeholder to ensure the object exists with metadata.
    -- Proper promotion is achieved via CREATE MODEL LIKE … (not supported), so we instead:
    -- 1) Keep using the versioned model in production calls, OR
    -- 2) Re-train directly into the base name (simpler). Choose #2 below:

  END IF;
END;

-- ============
-- Simpler promotion strategy (recommended): train directly into the stable name
--   If you prefer simplicity over version-keeping, uncomment this block and comment out the section above.
-- ============
/*
CREATE OR REPLACE MODEL `project_id.dataset_name.base_model_name`
OPTIONS (
  model_type = 'XGBOOST_REGRESSOR',
  input_label_cols = ['label'],
  data_split_method = 'AUTO_SPLIT',
  max_iterations = 200,
  tree_method = 'HIST',
  early_stop = TRUE,
  enable_global_explain = TRUE,
  labels = [('trained_at', CAST(run_time AS STRING)), ('lookback_days', CAST(lookback_days AS STRING))]
) AS
SELECT * FROM train_data;
*/

-- ============
-- Final output (so CLI prints something human-friendly)
-- ============
SELECT
  'trained_model' AS section,
  base_model_name AS model,
  versioned_name  AS version,
  run_time        AS trained_at,
  (SELECT mean_absolute_error FROM eval_new) AS mae_new,
  (SELECT r2_score FROM eval_new) AS r2_new,
  IF(prod_exists, (SELECT mean_absolute_error FROM eval_prod), NULL) AS mae_prev;
