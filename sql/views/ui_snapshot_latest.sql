CREATE OR REPLACE VIEW `my-plant-agent-123456.plant_ops.ui_snapshot_latest` AS
SELECT
  CAST(production_tph             AS FLOAT64) AS production_tph,
  CAST(kiln_feed_tph              AS FLOAT64) AS kiln_feed_tph,
  CAST(separator_dp_pa            AS FLOAT64) AS separator_dp_pa,
  CAST(id_fan_flow_Nm3_h          AS FLOAT64) AS id_fan_flow_Nm3_h,
  CAST(cooler_airflow_Nm3_h       AS FLOAT64) AS cooler_airflow_Nm3_h,
  CAST(kiln_speed_rpm             AS FLOAT64) AS kiln_speed_rpm,
  CAST(o2_percent                 AS FLOAT64) AS o2_percent,
  CAST(specific_power_kwh_per_ton AS FLOAT64) AS specific_power_kwh_per_ton,
  ts
FROM `my-plant-agent-123456.plant_ops.snapshots`
WHERE production_tph IS NOT NULL
QUALIFY ROW_NUMBER() OVER (ORDER BY ts DESC) = 1;
