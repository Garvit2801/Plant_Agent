-- Last 7 days of per-lever suggestions (handy for dashboards)
CREATE OR REPLACE VIEW `plant_ops.suggestions_latest` AS
SELECT *
FROM `plant_ops.suggestions_v1`
WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY created_at DESC;
