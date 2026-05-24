SELECT 
  CAST(timestamp AS timestamp) AS time,
  crypto_mentioned,
  AVG(positive) AS positive,
  AVG(negative) AS negative,
  AVG(neutral) AS neutral
FROM post_content_gold
WHERE $__timeFilter(CAST(timestamp AS timestamp))
GROUP BY CAST(timestamp AS timestamp), crypto_mentioned
ORDER BY time ASC