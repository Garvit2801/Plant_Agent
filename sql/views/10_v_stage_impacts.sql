CREATE OR REPLACE VIEW `my-plant-agent-123456.plant_ops.v_stage_impacts` AS
WITH exploded AS (
  SELECT
    p.plan_id,
    p.mode,
    p.created_at,
    p.source,
    p.snapshot,
    p.target,
    p.predicted_after,
    s.name AS stage_name,
    s.setpoints AS stage_setpoints_json,
    s.checks AS stage_checks,
    OFFSET AS stage_index
  FROM `my-plant-agent-123456.plant_ops.plans_v2` AS p,
  UNNEST(p.stages) AS s WITH OFFSET
),
calc AS (
  SELECT
    plan_id,
    mode,
    created_at,
    source,
    stage_index,
    stage_name,
    stage_setpoints_json,
    stage_checks,

    -- Example “impacts” derived from existing structs
    snapshot.production_tph AS snapshot_production_tph,
    target.production_tph   AS target_production_tph,
    (target.production_tph - snapshot.production_tph) AS delta_production_tph,

    snapshot.specific_power_kwh_per_ton AS snapshot_sp_kwhpt,
    predicted_after.specific_power_kwh_per_ton AS predicted_sp_kwhpt,
    (predicted_after.specific_power_kwh_per_ton - snapshot.specific_power_kwh_per_ton) AS delta_sp_kwhpt,

    snapshot.kiln_feed_tph AS snapshot_kiln_feed_tph,
    snapshot.separator_dp_pa AS snapshot_separator_dp_pa,
    snapshot.id_fan_flow_Nm3_h AS snapshot_id_fan_flow_Nm3_h,
    snapshot.cooler_airflow_Nm3_h AS snapshot_cooler_airflow_Nm3_h,
    snapshot.kiln_speed_rpm AS snapshot_kiln_speed_rpm,
    snapshot.o2_percent AS snapshot_o2_percent
  FROM exploded
)
SELECT * FROM calc;
