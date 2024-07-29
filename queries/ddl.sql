CREATE OR REPLACE TABLE `{table_id}`
(
    `show_id` STRING OPTIONS(description="Unique identifier for each show"),
    `type` STRING OPTIONS(description="Type of content"),
    `title` STRING OPTIONS(description="Title of the show or movie"),
    `director` STRING OPTIONS(description="Director of the show or movie"),
    `cast` STRING OPTIONS(description="Main cast members of the show or movie"),
    `country` STRING OPTIONS(description="Country where the show or movie was produced"),
    `date_added` DATE OPTIONS(description="Date when the show or movie was added"),
    `release_year` INT64 OPTIONS(description="Year when the show or movie was originally released"),
    `rating` STRING OPTIONS(description="Content rating of the show or movie"),
    `duration` STRING OPTIONS(description="Duration of the show or movie"),
    `listed_in` STRING OPTIONS(description="Categories or genres the show or movie belongs to"),
    `description` STRING OPTIONS(description="Brief summary or synopsis of the show or movie"),
    `unique_id` STRING OPTIONS(description="Unique identifier combining show_id and service")
)
PARTITION BY date_added
CLUSTER BY release_year;