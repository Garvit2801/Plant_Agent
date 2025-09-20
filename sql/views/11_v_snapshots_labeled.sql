CREATE OR REPLACE VIEW `my-plant-agent-123456.plant_ops.v_snapshots_labeled` AS
WITH s AS (
  SELECT
    ts, production_tph, kiln_feed_tph, separator_dp_pa,
    id_fan_flow_Nm3_h, cooler_airflow_Nm3_h, kiln_speed_rpm,
    o2_percent, specific_power_kwh_per_ton
  FROM `my-plant-agent-123456.plant_ops.snapshots_current`
),
a AS (
  SELECT applied_at, plan_id, stage_index, stage_name, mode
  FROM `my-plant-agent-123456.plant_ops.actuations_v2`
  WHERE success IS TRUE
),
labeled AS (
  SELECT
    s.*,
    ANY_VALUE(a.plan_id)      AS plan_id,
    ANY_VALUE(a.stage_index)  AS stage_index,
    ANY_VALUE(a.stage_name)   AS stage_name,
    ANY_VALUE(a.mode)         AS mode,
    MAX(a.applied_at)         AS last_applied_at
  FROM s
  LEFT JOIN a
    ON a.applied_at <= s.ts
   AND s.ts <= TIMESTAMP_ADD(a.applied_at, INTERVAL 2 HOUR)
  GROUP BY s.ts, s.production_tph, s.kiln_feed_tph, s.separator_dp_pa,
           s.id_fan_flow_Nm3_h, s.cooler_airflow_Nm3_h, s.kiln_speed_rpm,
           s.o2_percent, s.specific_power_kwh_per_ton
)
SELECT * FROM labeled;
