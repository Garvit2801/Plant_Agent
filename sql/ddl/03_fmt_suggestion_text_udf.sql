-- Human-readable sentence generator
CREATE OR REPLACE FUNCTION `plant_ops.fmt_suggestion_text`(
  lever STRING, current_value FLOAT64, proposed_value FLOAT64,
  delta_pct FLOAT64, cmin FLOAT64, cmax FLOAT64
) AS ((
  CASE
    WHEN delta_pct IS NULL THEN CONCAT(lever, ': set to ', CAST(proposed_value AS STRING))
    WHEN delta_pct > 0    THEN CONCAT('Increase ', lever, ' by ~', CAST(ROUND(delta_pct,1) AS STRING), '% to ', CAST(proposed_value AS STRING))
    WHEN delta_pct < 0    THEN CONCAT('Reduce ',   lever, ' by ~', CAST(ABS(ROUND(delta_pct,1)) AS STRING), '% to ', CAST(proposed_value AS STRING))
    ELSE CONCAT('Hold ', lever, ' at ', CAST(proposed_value AS STRING))
  END
  ||
  CASE
    WHEN cmin IS NOT NULL OR cmax IS NOT NULL
      THEN CONCAT(' (bounds ', IFNULL(CAST(cmin AS STRING),'−∞'), '…', IFNULL(CAST(cmax AS STRING),'+∞'), ')')
    ELSE ''
  END
));
