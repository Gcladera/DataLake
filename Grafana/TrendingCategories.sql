WITH datos_base AS (
  SELECT 
    name,
    CAST(market_cap_usd AS DOUBLE) AS market_cap,
    CAST(coins_count AS DOUBLE) AS coins_count,
    CAST(timestamp AS TIMESTAMP) AS ts
  FROM trending_gold
),
actual AS (
  SELECT name, market_cap, coins_count
  FROM datos_base
  WHERE ts = (SELECT MAX(ts) FROM datos_base)
),
pasado AS (
  SELECT name, market_cap, coins_count
  FROM (
    SELECT 
      name, 
      market_cap, 
      coins_count,
      ROW_NUMBER() OVER(PARTITION BY name ORDER BY ts DESC) as rn
    FROM datos_base
    WHERE ts <= (SELECT date_add('hour', -1, MAX(ts)) FROM datos_base)
  ) sub
  WHERE rn = 1
)
SELECT 
  a.name,
  a.market_cap AS market_cap_actual,
  CASE 
    WHEN p.market_cap IS NULL OR p.market_cap = 0 THEN NULL
    ELSE ((a.market_cap / p.market_cap) - 1) * 100 
  END AS variacion_market_cap,
  a.coins_count AS coins_count_actual,
  CASE 
    WHEN p.coins_count IS NULL OR p.coins_count = 0 THEN NULL
    ELSE ((a.coins_count / p.coins_count) - 1) * 100 
  END AS variacion_coins_count
FROM actual a
LEFT JOIN pasado p 
  ON a.name = p.name
ORDER BY market_cap_actual DESC;