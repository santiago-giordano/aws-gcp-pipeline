--Top 10 de actores participantes considerando ambas plataformas en el año actual. Se aprecia flexibilidad.
DECLARE year INT64 DEFAULT CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INT64);

SET year = CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INT64);

WITH
ACTORS AS (
  SELECT 
    actor
  FROM `project-rockingdata.dataset_rd.all_titles`,
    UNNEST(SPLIT(`cast`, ', ')) AS actor 
  WHERE 
    release_year = year
)
SELECT 
  actor,
  COUNT(*) AS appearances
FROM ACTORS
GROUP BY ALL
ORDER BY appearances DESC
LIMIT 10;