-- === 1) Recent stage events (applied or just proposed) ===
WITH plan_stages AS (
  SELECT
    p.plan_id, p.mode, p.created_at,
    s.name AS stage_name, OFFSET AS stage_index
  FROM `my-plant-agent-123456.plant_ops.plans_v2` p,
  UNNEST(p.stages) s WITH OFFSET
)
SELECT
  ps.plan_id,
  ps.stage_index,
  ps.stage_name,
  ps.mode,
  COALESCE(a.applied_at, ps.created_at) AS event_ts,
  a.success,
  a.error
FROM plan_stages ps
LEFT JOIN `my-plant-agent-123456.plant_ops.actuations_v2` a
  ON a.plan_id = ps.plan_id
 AND a.stage_index = ps.stage_index
ORDER BY event_ts DESC
LIMIT 50;

-- === 2) Pending stages (defined but not yet applied) ===
WITH plan_stages AS (
  SELECT
    p.plan_id, p.mode, p.created_at,
    s.name AS stage_name, OFFSET AS stage_index
  FROM `my-plant-agent-123456.plant_ops.plans_v2` p,
  UNNEST(p.stages) s WITH OFFSET
)
SELECT
  ps.plan_id,
  ps.stage_index,
  ps.stage_name,
  ps.mode,
  ps.created_at AS defined_at
FROM plan_stages ps
LEFT JOIN `my-plant-agent-123456.plant_ops.actuations_v2` a
  ON a.plan_id = ps.plan_id
 AND a.stage_index = ps.stage_index
WHERE a.applied_at IS NULL
ORDER BY defined_at DESC
LIMIT 50;

-- === 3) Actuation success rate (only applied stages) ===
WITH plan_stages AS (
  SELECT
    p.plan_id, p.mode, p.created_at,
    s.name AS stage_name, OFFSET AS stage_index
  FROM `my-plant-agent-123456.plant_ops.plans_v2` p,
  UNNEST(p.stages) s WITH OFFSET
)
SELECT
  ps.mode,
  COUNTIF(a.success) AS success_count,
  COUNTIF(a.success = FALSE) AS failure_count,
  COUNT(*) AS total,
  SAFE_DIVIDE(COUNTIF(a.success), COUNT(*)) AS success_rate
FROM plan_stages ps
JOIN `my-plant-agent-123456.plant_ops.actuations_v2` a
  ON a.plan_id = ps.plan_id
 AND a.stage_index = ps.stage_index
WHERE a.applied_at IS NOT NULL
GROUP BY ps.mode
ORDER BY total DESC;
