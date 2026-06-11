SELECT 
  CAST(m.timestamp AS timestamp) AS time,
  m.symbol,
  m.current_price AS price,
  COALESCE(s.sentiment_index, LAG(s.sentiment_index) IGNORE NULLS OVER (ORDER BY CAST(m.timestamp AS timestamp))) AS sentiment_avg

FROM market_ranking_gold m
LEFT JOIN score_by_crypto_gold s 
  ON lower(m.name) = lower(s.coin) 
  AND m.year = s.year 
  AND m.month = s.month 
  AND m.day = s.day
WHERE $__timeFilter(CAST(m.timestamp AS timestamp))
  AND m.symbol = 'btc' or m.symbol= 'eth'
ORDER BY time ASC
