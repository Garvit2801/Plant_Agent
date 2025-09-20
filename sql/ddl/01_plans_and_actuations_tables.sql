-- Dataset
CREATE SCHEMA IF NOT EXISTS `my-plant-agent-123456.plant_ops`;

ALTER SCHEMA `my-plant-agent-123456.plant_ops`
SET OPTIONS (default_table_expiration_days=180);

-- plans_v2: staged plans and routine proposals
CREATE TABLE IF NOT EXISTS `my-plant-agent-123456.plant_ops.plans_v2` (
  plan_id        STRING,
  mode           STRING,   -- 'load_up' | 'load_down' | 'routine'
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  source         STRING,

  snapshot       STRUCT<
    production_tph FLOAT64,
    kiln_feed_tph  FLOAT64,
    separator_dp_pa FLOAT64,
    id_fan_flow_Nm3_h FLOAT64,
    cooler_airflow_Nm3_h FLOAT64,
    kiln_speed_rpm FLOAT64,
    o2_percent FLOAT64,
    specific_power_kwh_per_ton FLOAT64,
    ts TIMESTAMP
  >,

  target         STRUCT<
    production_tph FLOAT64,
    delta_pct FLOAT64
  >,

  predicted_after STRUCT<
    specific_power_kwh_per_ton FLOAT64
  >,

  stages         ARRAY<STRUCT<
    name STRING,
    setpoints JSON,
    checks ARRAY<STRING>
  >>,

  proposed_setpoints JSON,
  note           STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY mode, plan_id;

-- actuations_v2: what was applied + before/after snapshots
CREATE TABLE IF NOT EXISTS `my-plant-agent-123456.plant_ops.actuations_v2` (
  actuation_id   STRING,
  plan_id        STRING,
  stage_index    INT64,
  stage_name     STRING,

  mode           STRING,   -- 'load_up' | 'load_down' | 'routine'
  requested_by   STRING,
  requested_at   TIMESTAMP,
  applied_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  success        BOOL,
  error          STRING,

  setpoints      JSON,

  before         STRUCT<
    production_tph FLOAT64,
    kiln_feed_tph  FLOAT64,
    separator_dp_pa FLOAT64,
    id_fan_flow_Nm3_h FLOAT64,
    cooler_airflow_Nm3_h FLOAT64,
    kiln_speed_rpm FLOAT64,
    o2_percent FLOAT64,
    specific_power_kwh_per_ton FLOAT64,
    ts TIMESTAMP
  >,
  after          STRUCT<
    production_tph FLOAT64,
    kiln_feed_tph  FLOAT64,
    separator_dp_pa FLOAT64,
    id_fan_flow_Nm3_h FLOAT64,
    cooler_airflow_Nm3_h FLOAT64,
    kiln_speed_rpm FLOAT64,
    o2_percent FLOAT64,
    specific_power_kwh_per_ton FLOAT64,
    ts TIMESTAMP
  >
)
PARTITION BY DATE(applied_at)
CLUSTER BY plan_id, mode;
