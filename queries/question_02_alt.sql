--Top 10 de actores participantes considerando ultimo año existente
DECLARE year INT64 DEFAULT CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INT64);

SET year = (SELECT MAX(release_year) FROM `project-rockingdata.dataset_rd.all_titles`);

WITH
ACTORS AS (
  SELECT 
    actor,
    release_year
  FROM `project-rockingdata.dataset_rd.all_titles`,
    UNNEST(SPLIT(`cast`, ', ')) AS actor 
  WHERE 
    release_year = year
)
SELECT 
  actor,
  release_year,
  COUNT(*) AS appearances
FROM ACTORS
GROUP BY ALL
ORDER BY appearances DESC
LIMIT 10;