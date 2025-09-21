-- One row: the most recent suggestion bundle rendered as sentences
CREATE OR REPLACE VIEW `plant_ops.suggestions_last_run` AS
SELECT
  ARRAY_AGG(v.suggestion_text ORDER BY v.lever)  AS lines,
  ANY_VALUE(v.created_at)                         AS created_at,
  ANY_VALUE(v.suggestion_id)                      AS suggestion_id
FROM `plant_ops.suggestions_v1` AS v
WHERE v.suggestion_id = (
  SELECT s.suggestion_id
  FROM `plant_ops.suggestions_v1` AS s
  ORDER BY s.created_at DESC
  LIMIT 1
);
