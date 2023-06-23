with raw_hashtag AS (
  SELECT
    REGEXP_REPLACE(VideoID, r'[^A-Za-z0-9]+', '') AS video_id,
    REGEXP_EXTRACT_ALL(Title, r'#\w+') AS hashtags_title,
    REGEXP_EXTRACT_ALL(Description, r'#\w+') AS hashtags_description
  FROM
    {{ source('raw', 'raw_video') }}
),
hashtags as(
  select
    video_id,
    hashtag
  from
    raw_hashtag
    CROSS JOIN UNNEST(COALESCE(hashtags_title, hashtags_description)) AS hashtag
)
select
  lower(video_id) as video_id,
  hashtag
from
  hashtags