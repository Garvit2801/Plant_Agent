-- Creates the per-lever suggestions table
CREATE TABLE IF NOT EXISTS `plant_ops.suggestions_v1` (
  suggestion_row_id  STRING    NOT NULL,   -- UUID per lever row
  suggestion_id      STRING    NOT NULL,   -- ties back to routine_suggestions_v2
  created_at         TIMESTAMP NOT NULL,
  source             STRING    NOT NULL,   -- 'routine'
  lever              STRING    NOT NULL,   -- e.g. 'separator_dp_pa'
  current_value      FLOAT64,
  proposed_value     FLOAT64,
  delta_abs          FLOAT64,
  delta_pct          FLOAT64,
  constraint_min     FLOAT64,
  constraint_max     FLOAT64,
  confidence         FLOAT64,              -- optional
  suggestion_text    STRING,               -- human-readable sentence
  proposed_setpoints JSON,                 -- full proposal for traceability
  snapshot_before    JSON,                 -- optional
  prediction_after   JSON                  -- optional
);
