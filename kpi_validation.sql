-- 1) Recent snapshots landed (last 10 minutes)
SELECT
  COUNT(*) AS rows_10min,
  MIN(ts) AS min_ts,
  MAX(ts) AS max_ts
FROM `my-plant-agent-123456.plant_ops.snapshots_current`
WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE);

-- 2) Quick model evaluation
SELECT * FROM ML.EVALUATE(MODEL `my-plant-agent-123456.plant_ops.spower_reg`);

-- 3) Predict specific power using last observation (BEGIN block)
BEGIN
  DECLARE last_row STRUCT<
    production_tph FLOAT64,
    kiln_feed_tph FLOAT64,
    separator_dp_pa FLOAT64,
    id_fan_flow_Nm3_h FLOAT64,
    cooler_airflow_Nm3_h FLOAT64,
    kiln_speed_rpm FLOAT64,
    o2_percent FLOAT64
  >;

  SET last_row = (
    SELECT AS STRUCT
      production_tph,
      kiln_feed_tph,
      separator_dp_pa,
      id_fan_flow_Nm3_h,
      cooler_airflow_Nm3_h,
      kiln_speed_rpm,
      o2_percent
    FROM `my-plant-agent-123456.plant_ops.snapshots_current`
    ORDER BY ts DESC
    LIMIT 1
  );

  SELECT *
  FROM ML.PREDICT(
    MODEL `my-plant-agent-123456.plant_ops.spower_reg`,
    (SELECT last_row.*)
  );
END;
