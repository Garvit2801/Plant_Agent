-- train_spower.sql
-- Usage:
--   export PROJECT_ID="$(gcloud config get-value project)"
--   BQ_LOCATION="asia-south2"
--   envsubst < train_spower.sql | bq query --use_legacy_sql=false --location="$BQ_LOCATION"

-- =========================
-- Parameters (safe defaults)
-- =========================
DECLARE lookback_days INT64 DEFAULT 30;  -- days of recent data to train on

-- =========================
-- Train (stable model name)
-- =========================
CREATE OR REPLACE MODEL `${PROJECT_ID}.plant_ops.spower_reg`
OPTIONS (
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['specific_power_kwh_per_ton'],
  data_split_method = 'AUTO_SPLIT',
  max_iterations = 200,
  early_stop = TRUE,
  enable_global_explain = TRUE,
  -- You can tune these if desired (all optional and supported):
  -- max_tree_depth = 6,
  -- subsample = 0.8,
  -- l1_reg = 0.0,
  -- l2_reg = 0.0,
  labels = [
    ('trained_at', CAST(CURRENT_TIMESTAMP() AS STRING)),
    ('lookback_days', CAST(lookback_days AS STRING))
  ]
) AS
SELECT
  -- label
  specific_power_kwh_per_ton,
  -- features
  production_tph,
  kiln_feed_tph,
  separator_dp_pa,
  id_fan_flow_Nm3_h,
  cooler_airflow_Nm3_h,
  kiln_speed_rpm,
  o2_percent
FROM `${PROJECT_ID}.plant_ops.snapshots`
WHERE
  ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)
  AND specific_power_kwh_per_ton IS NOT NULL
  AND production_tph IS NOT NULL
  AND o2_percent IS NOT NULL
;

-- =========================
-- Evaluate
-- =========================
SELECT 'EVALUATE' AS section, *
FROM ML.EVALUATE(MODEL `${PROJECT_ID}.plant_ops.spower_reg`);

-- (Optional) Feature importance for explainability
SELECT 'FEATURE_IMPORTANCE' AS section, *
FROM ML.FEATURE_IMPORTANCE(MODEL `${PROJECT_ID}.plant_ops.spower_reg`);
