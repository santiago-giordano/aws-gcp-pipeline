CREATE OR REPLACE TABLE `project-rockingdata.dataset_rd.all_titles`
PARTITION BY date_added
CLUSTER BY release_year AS
SELECT * FROM `project-rockingdata.dataset_rd.disney_plus_titles` 
UNION ALL
SELECT * FROM `project-rockingdata.dataset_rd.netflix_titles`;