-- Create the new improved table (snapshots_v2)
CREATE TABLE `my-plant-agent-123456.plant_ops.snapshots_v2`
(
  ts TIMESTAMP NOT NULL,                     -- timestamp of the reading
  source STRING,                             -- where the data came from
  production_tph FLOAT64,                    -- tons/hour
  kiln_feed_tph FLOAT64,                     -- tons/hour
  separator_dp_pa FLOAT64,                   -- pressure
  id_fan_flow_Nm3_h FLOAT64,                 -- airflow
  cooler_airflow_Nm3_h FLOAT64,              -- airflow
  kiln_speed_rpm FLOAT64,                    -- rotation speed
  o2_percent FLOAT64,                        -- oxygen %
  specific_power_kwh_per_ton FLOAT64,        -- power usage
  raw JSON                                   -- raw JSON copy of snapshot
)
-- Extra improvements:
PARTITION BY DATE(ts)                        -- store data by day
CLUSTER BY source                            -- group rows by source
OPTIONS (partition_expiration_days = 180);   -- auto-delete after 180 days

-- Copy (backfill) all historical rows into the new table
INSERT INTO `my-plant-agent-123456.plant_ops.snapshots_v2`
SELECT * FROM `my-plant-agent-123456.plant_ops.snapshots`;
