WITH datos_base AS (
  SELECT 
    name,
    CAST(market_cap_usd AS DOUBLE) AS market_cap,
    CAST(coins_count AS DOUBLE) AS coins_count,
    CAST(timestamp AS TIMESTAMP) AS ts
  FROM trending_gold
),
tiempo_maximo AS (
  SELECT MAX(ts) AS tiempo_actual 
  FROM datos_base
),
tiempo_pasado AS (
  SELECT MAX(ts) AS tiempo_pasado
  FROM datos_base
  WHERE ts <= (SELECT tiempo_actual - INTERVAL '1' HOUR FROM tiempo_maximo)
)
SELECT 
  actual.name,
  actual.market_cap AS market_cap_actual,
  CASE 
    WHEN pasado.market_cap IS NULL OR pasado.market_cap = 0 THEN NULL
    ELSE ((actual.market_cap / pasado.market_cap) - 1) * 100 
  END AS variacion_market_cap,
  actual.coins_count AS coins_count_actual,
  CASE 
    WHEN pasado.coins_count IS NULL OR pasado.coins_count = 0 THEN NULL
    ELSE ((actual.coins_count / pasado.coins_count) - 1) * 100 
  END AS variacion_coins_count
FROM tiempo_maximo tm
CROSS JOIN tiempo_pasado tp
JOIN datos_base AS actual 
  ON actual.ts = tm.tiempo_actual
LEFT JOIN datos_base AS pasado 
  ON pasado.ts = tp.tiempo_pasado 
  AND actual.name = pasado.name
ORDER BY market_cap_actual DESC;