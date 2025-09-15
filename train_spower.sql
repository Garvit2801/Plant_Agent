CREATE OR REPLACE MODEL `${PROJECT_ID}.plant_ops.spower_reg`
OPTIONS(model_type='LINEAR_REG') AS
SELECT
  production_tph,
  kiln_feed_tph,
  separator_dp_pa,
  id_fan_flow_Nm3_h,
  cooler_airflow_Nm3_h,
  kiln_speed_rpm,
  o2_percent,
  specific_power_kwh_per_ton AS label
FROM `${PROJECT_ID}.plant_ops.snapshots`
WHERE ts >= TIMESTAMP_SUB(@run_time, INTERVAL 30 DAY)
  AND ts < @run_time;
