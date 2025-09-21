-- Backfill per-lever suggestions from routine_suggestions_v2 → suggestions_v1
-- Run with:
--   bq --location="asia-south2" --project_id="$PROJECT_ID" query --nouse_legacy_sql \
--     < sql/backfill/backfill_suggestions_from_routine.sql

MERGE `plant_ops.suggestions_v1` T
USING (
  WITH src AS (
    SELECT
      suggestion_id,
      created_at,
      snapshot,            -- JSON
      proposed_setpoints,  -- JSON
      constraints          -- JSON
    FROM `plant_ops.routine_suggestions_v2`
  ),

  -- Expand to one row per lever (add more levers here if needed)
  pairs AS (
    SELECT suggestion_id, created_at, snapshot, proposed_setpoints, constraints,
           'separator_dp_pa' AS lever,
           JSON_VALUE(proposed_setpoints, '$.separator_dp_pa') AS proposed_val_str
    FROM src
    UNION ALL
    SELECT suggestion_id, created_at, snapshot, proposed_setpoints, constraints,
           'id_fan_flow_Nm3_h' AS lever,
           JSON_VALUE(proposed_setpoints, '$.id_fan_flow_Nm3_h') AS proposed_val_str
    FROM src
    UNION ALL
    SELECT suggestion_id, created_at, snapshot, proposed_setpoints, constraints,
           'cooler_airflow_Nm3_h' AS lever,
           JSON_VALUE(proposed_setpoints, '$.cooler_airflow_Nm3_h') AS proposed_val_str
    FROM src
  ),

  exploded AS (
    SELECT
      suggestion_id,
      created_at,
      'routine' AS source,
      lever,

      -- current_value (constant path per lever)
      CASE lever
        WHEN 'separator_dp_pa'
          THEN SAFE_CAST(JSON_VALUE(snapshot, '$.separator_dp_pa') AS FLOAT64)
        WHEN 'id_fan_flow_Nm3_h'
          THEN SAFE_CAST(JSON_VALUE(snapshot, '$.id_fan_flow_Nm3_h') AS FLOAT64)
        WHEN 'cooler_airflow_Nm3_h'
          THEN SAFE_CAST(JSON_VALUE(snapshot, '$.cooler_airflow_Nm3_h') AS FLOAT64)
        ELSE NULL
      END AS current_value,

      -- proposed_value
      SAFE_CAST(proposed_val_str AS FLOAT64) AS proposed_value,

      -- delta_abs
      SAFE_CAST(proposed_val_str AS FLOAT64)
        - CASE lever
            WHEN 'separator_dp_pa'      THEN SAFE_CAST(JSON_VALUE(snapshot, '$.separator_dp_pa') AS FLOAT64)
            WHEN 'id_fan_flow_Nm3_h'    THEN SAFE_CAST(JSON_VALUE(snapshot, '$.id_fan_flow_Nm3_h') AS FLOAT64)
            WHEN 'cooler_airflow_Nm3_h' THEN SAFE_CAST(JSON_VALUE(snapshot, '$.cooler_airflow_Nm3_h') AS FLOAT64)
            ELSE NULL
          END AS delta_abs,

      -- delta_pct
      SAFE_DIVIDE(
        SAFE_CAST(proposed_val_str AS FLOAT64)
          - CASE lever
              WHEN 'separator_dp_pa'      THEN SAFE_CAST(JSON_VALUE(snapshot, '$.separator_dp_pa') AS FLOAT64)
              WHEN 'id_fan_flow_Nm3_h'    THEN SAFE_CAST(JSON_VALUE(snapshot, '$.id_fan_flow_Nm3_h') AS FLOAT64)
              WHEN 'cooler_airflow_Nm3_h' THEN SAFE_CAST(JSON_VALUE(snapshot, '$.cooler_airflow_Nm3_h') AS FLOAT64)
              ELSE NULL
            END,
        NULLIF(
          CASE lever
            WHEN 'separator_dp_pa'      THEN SAFE_CAST(JSON_VALUE(snapshot, '$.separator_dp_pa') AS FLOAT64)
            WHEN 'id_fan_flow_Nm3_h'    THEN SAFE_CAST(JSON_VALUE(snapshot, '$.id_fan_flow_Nm3_h') AS FLOAT64)
            WHEN 'cooler_airflow_Nm3_h' THEN SAFE_CAST(JSON_VALUE(snapshot, '$.cooler_airflow_Nm3_h') AS FLOAT64)
            ELSE NULL
          END, 0)
      ) * 100.0 AS delta_pct,

      -- constraints
      CASE lever
        WHEN 'separator_dp_pa'
          THEN SAFE_CAST(JSON_VALUE(constraints, '$.separator_dp_pa.min') AS FLOAT64)
        WHEN 'id_fan_flow_Nm3_h'
          THEN SAFE_CAST(JSON_VALUE(constraints, '$.id_fan_flow_Nm3_h.min') AS FLOAT64)
        WHEN 'cooler_airflow_Nm3_h'
          THEN SAFE_CAST(JSON_VALUE(constraints, '$.cooler_airflow_Nm3_h.min') AS FLOAT64)
        ELSE NULL
      END AS constraint_min,

      CASE lever
        WHEN 'separator_dp_pa'
          THEN SAFE_CAST(JSON_VALUE(constraints, '$.separator_dp_pa.max') AS FLOAT64)
        WHEN 'id_fan_flow_Nm3_h'
          THEN SAFE_CAST(JSON_VALUE(constraints, '$.id_fan_flow_Nm3_h.max') AS FLOAT64)
        WHEN 'cooler_airflow_Nm3_h'
          THEN SAFE_CAST(JSON_VALUE(constraints, '$.cooler_airflow_Nm3_h.max') AS FLOAT64)
        ELSE NULL
      END AS constraint_max,

      proposed_setpoints,   -- JSON column
      snapshot              -- JSON column
    FROM pairs
    WHERE proposed_val_str IS NOT NULL
  )

  SELECT
    GENERATE_UUID() AS suggestion_row_id,
    suggestion_id,
    created_at,
    source,
    lever,
    current_value,
    proposed_value,
    delta_abs,
    delta_pct,
    constraint_min,
    constraint_max,
    CAST(NULL AS FLOAT64) AS confidence,         -- explicit type
    CAST(NULL AS STRING)  AS suggestion_text,    -- explicit type
    proposed_setpoints,                           -- JSON type preserved
    snapshot AS snapshot_before,                  -- JSON type preserved
    CAST(NULL AS JSON) AS prediction_after        -- ✅ explicit JSON NULL (FIX)
  FROM exploded
) S
ON  T.suggestion_id = S.suggestion_id
AND T.lever         = S.lever
AND T.created_at    = S.created_at
WHEN NOT MATCHED THEN
  INSERT (
    suggestion_row_id, suggestion_id, created_at, source, lever,
    current_value, proposed_value, delta_abs, delta_pct,
    constraint_min, constraint_max, confidence,
    suggestion_text, proposed_setpoints, snapshot_before, prediction_after
  )
  VALUES (
    S.suggestion_row_id, S.suggestion_id, S.created_at, S.source, S.lever,
    S.current_value, S.proposed_value, S.delta_abs, S.delta_pct,
    S.constraint_min, S.constraint_max, S.confidence,
    S.suggestion_text, S.proposed_setpoints, S.snapshot_before, S.prediction_after
  );

-- 2) Fill human-readable suggestion_text (no external UDF needed)
CREATE TEMP FUNCTION fmt_suggestion_text(
  lever STRING, current_value FLOAT64, proposed_value FLOAT64, delta_pct FLOAT64,
  cmin FLOAT64, cmax FLOAT64
) AS ((
  CASE
    WHEN delta_pct IS NULL THEN CONCAT(lever, ': set to ', CAST(proposed_value AS STRING))
    WHEN delta_pct > 0 THEN CONCAT('Increase ', lever, ' by ~', CAST(ROUND(delta_pct, 1) AS STRING), '% to ', CAST(proposed_value AS STRING))
    WHEN delta_pct < 0 THEN CONCAT('Reduce ', lever, ' by ~', CAST(ABS(ROUND(delta_pct, 1)) AS STRING), '% to ', CAST(proposed_value AS STRING))
    ELSE CONCAT('Hold ', lever, ' at ', CAST(proposed_value AS STRING))
  END
  || CASE
       WHEN cmin IS NOT NULL OR cmax IS NOT NULL THEN
         CONCAT(' (bounds ', IFNULL(CAST(cmin AS STRING), '−∞'), '…', IFNULL(CAST(cmax AS STRING), '+∞'), ')')
       ELSE ''
     END
));

UPDATE `plant_ops.suggestions_v1`
SET suggestion_text = fmt_suggestion_text(lever, current_value, proposed_value, delta_pct, constraint_min, constraint_max)
WHERE suggestion_text IS NULL;
