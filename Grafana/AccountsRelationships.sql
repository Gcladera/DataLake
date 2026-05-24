--Nodes
WITH top_nodes AS (
  SELECT node_id FROM (
    SELECT start_id AS node_id FROM relationships
    UNION ALL
    SELECT end_id AS node_id FROM relationships
  ) AS all_connections
  GROUP BY node_id
  ORDER BY COUNT(*) DESC
  LIMIT 20
)
SELECT 
  user_id AS id,
  user_id AS title,
  label AS subTitle
FROM nodes
WHERE user_id IN (SELECT node_id FROM top_nodes);

--Relationships
WITH top_nodes AS (
  SELECT node_id FROM (
    SELECT start_id AS node_id FROM relationships
    UNION ALL
    SELECT end_id AS node_id FROM relationships
  ) AS all_connections
  GROUP BY node_id
  ORDER BY COUNT(*) DESC
  LIMIT 20
)
SELECT 
  start_id AS source,
  end_id AS target,
  COUNT(*) AS mainStat, -- Agrupa interacciones y define el peso de la arista
  CONCAT(start_id, '-', end_id) AS id
FROM relationships
WHERE start_id IN (SELECT node_id FROM top_nodes)
  AND end_id IN (SELECT node_id FROM top_nodes)
GROUP BY start_id, end_id;