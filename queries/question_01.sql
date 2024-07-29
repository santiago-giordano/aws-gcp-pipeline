--Actor que aparece más veces en Netflix
SELECT 
  actor,
  COUNT(*) AS appearances
FROM (
  SELECT
    actor
  FROM
    `project-rockingdata.dataset_rd.netflix_titles`,
    UNNEST(SPLIT(`cast`, ', ')) AS actor
)
GROUP BY actor
ORDER BY appearances DESC
LIMIT 1;