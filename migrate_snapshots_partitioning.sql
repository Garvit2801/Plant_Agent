-- migrate_snapshots_partitioning.sql
-- Goal: make plant_ops.snapshots partitioned by DATE(ts), clustered by source, 180-day retention
-- Region: asia-south2

-- ========= Parameters (edit if needed) =========
DECLARE project_id   STRING DEFAULT "my-plant-agent-123456";
DECLARE dataset_name STRING DEFAULT "plant_ops";
DECLARE base_table   STRING DEFAULT "snapshots";
DECLARE part_table   STRING DEFAULT "snapshots_part";
DECLARE old_table    STRING DEFAULT "snapshots_old";

-- We'll need this in multiple places; declare it up front.
DECLARE base_exists BOOL DEFAULT FALSE;

-- ========= Detect if the current base table exists =========
EXECUTE IMMEDIATE FORMAT("""
  SELECT COUNT(1) > 0
  FROM `%s.%s`.INFORMATION_SCHEMA.TABLES
  WHERE table_name = @t
""", project_id, dataset_name)
INTO base_exists
USING base_table AS t;

-- ========= 1) Create partitioned+clustered table (IF NOT EXISTS) =========
EXECUTE IMMEDIATE FORMAT("""
  CREATE TABLE IF NOT EXISTS `%s.%s.%s`
  (
    ts TIMESTAMP NOT NULL,
    source STRING,
    production_tph FLOAT64,
    kiln_feed_tph FLOAT64,
    separator_dp_pa FLOAT64,
    id_fan_flow_Nm3_h FLOAT64,
    cooler_airflow_Nm3_h FLOAT64,
    kiln_speed_rpm FLOAT64,
    o2_percent FLOAT64,
    specific_power_kwh_per_ton FLOAT64,
    raw JSON
  )
  PARTITION BY DATE(ts)
  CLUSTER BY source
  OPTIONS (partition_expiration_days = 180)
""", project_id, dataset_name, part_table);

-- ========= 2) Backfill existing data into snapshots_part (if base exists) =========
IF base_exists THEN
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s.%s.%s` (
      ts, source, production_tph, kiln_feed_tph, separator_dp_pa,
      id_fan_flow_Nm3_h, cooler_airflow_Nm3_h, kiln_speed_rpm,
      o2_percent, specific_power_kwh_per_ton, raw
    )
    SELECT
      ts, source, production_tph, kiln_feed_tph, separator_dp_pa,
      id_fan_flow_Nm3_h, cooler_airflow_Nm3_h, kiln_speed_rpm,
      o2_percent, specific_power_kwh_per_ton, raw
    FROM `%s.%s.%s`
  """, project_id, dataset_name, part_table,
       project_id, dataset_name, base_table);
END IF;

-- ========= 3) Swap names (do during low traffic) =========
BEGIN
  -- Drop any previous snapshots_old to free the name
  EXECUTE IMMEDIATE FORMAT("""
    DROP TABLE IF EXISTS `%s.%s.%s`
  """, project_id, dataset_name, old_table);

  -- Re-check existence (if someone dropped it meanwhile)
  SET base_exists = FALSE;
  EXECUTE IMMEDIATE FORMAT("""
    SELECT COUNT(1) > 0
    FROM `%s.%s`.INFORMATION_SCHEMA.TABLES
    WHERE table_name = @t
  """, project_id, dataset_name)
  INTO base_exists
  USING base_table AS t;

  -- If base exists, rename base -> old (RENAME uses unqualified target name)
  IF base_exists THEN
    EXECUTE IMMEDIATE FORMAT("""
      ALTER TABLE `%s.%s.%s` RENAME TO %s
    """, project_id, dataset_name, base_table, old_table);
  END IF;

  -- Now rename part -> base
  EXECUTE IMMEDIATE FORMAT("""
    ALTER TABLE `%s.%s.%s` RENAME TO %s
  """, project_id, dataset_name, part_table, base_table);
END;

-- ========= 4) (Optional) Drop the old table after verification =========
-- Uncomment after you’ve verified:
-- EXECUTE IMMEDIATE FORMAT("DROP TABLE `%s.%s.%s`", project_id, dataset_name, old_table);
