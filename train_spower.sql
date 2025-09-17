-- train_spower.sql (hardcoded project)
DECLARE lookback_days INT64 DEFAULT 30;

CREATE OR REPLACE MODEL `my-plant-agent-123456.plant_ops.spower_reg`
OPTIONS (
  model_type = 'LINEAR_REG',
  input_label_cols = ['specific_power_kwh_per_ton'],
  data_split_method = 'AUTO_SPLIT'
) AS
SELECT
  specific_power_kwh_per_ton,
  production_tph,
  kiln_feed_tph,
  separator_dp_pa,
  id_fan_flow_Nm3_h,
  cooler_airflow_Nm3_h,
  kiln_speed_rpm,
  o2_percent
FROM `my-plant-agent-123456.plant_ops.snapshots_current`
WHERE
  ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)
  AND specific_power_kwh_per_ton IS NOT NULL
  AND production_tph IS NOT NULL
  AND o2_percent IS NOT NULL
;

SELECT 'EVALUATE' AS section, *
FROM ML.EVALUATE(MODEL `my-plant-agent-123456.plant_ops.spower_reg`);

SELECT * FROM ML.WEIGHTS(MODEL `my-plant-agent-123456.plant_ops.spower_reg`)
ORDER BY ABS(weight) DESC;
