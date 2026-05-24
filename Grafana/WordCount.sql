WITH lista_palabras AS (
  SELECT palabra
  FROM post_content_gold,
  UNNEST(SPLIT(LOWER(text_cleaned), ' ')) AS t(palabra)
  WHERE 
    palabra != '' 
    AND palabra != '.' 
    AND palabra != ',' 
    AND palabra != '#'
    AND CAST(timestamp AS TIMESTAMP) >= CAST($__timeFrom() AS TIMESTAMP)
    AND CAST(timestamp AS TIMESTAMP) <= CAST($__timeTo() AS TIMESTAMP)
)
SELECT
  palabra,
  COUNT(*) AS repeticiones
FROM lista_palabras
GROUP BY palabra
ORDER BY repeticiones DESC
LIMIT 20;