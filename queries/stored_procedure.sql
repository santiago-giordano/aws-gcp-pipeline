CREATE OR REPLACE PROCEDURE `project-rockingdata.dataset_rd.get_top_5_longest_movies`(year INT64)
BEGIN
  CREATE OR REPLACE TABLE `project-rockingdata.dataset_rd.top_5_longest_movies` AS
  WITH MOVIES AS (
    SELECT
      title,
      CAST(REGEXP_EXTRACT(duration, r'(\d+)') AS INT64) AS duration_minutes,
      release_year
    FROM
      `project-rockingdata.dataset_rd.all_titles`
    WHERE 
      release_year = year AND type = 'Movie'
  )
  SELECT 
    title,
    duration_minutes,
    release_year
  FROM MOVIES
  ORDER BY duration_minutes DESC
  LIMIT 5;
END;