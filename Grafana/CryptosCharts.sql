SELECT 
  CAST(timestamp AS timestamp) AS time,
  symbol,
  current_price AS value
FROM market_ranking_gold 
WHERE $__timeFilter(CAST(timestamp AS timestamp))
ORDER BY time ASC