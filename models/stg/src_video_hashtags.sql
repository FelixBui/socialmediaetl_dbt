with raw_hashtag AS (
  SELECT
    CAST(VideoID AS STRING) AS video_id,
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
  video_id hash_tag
from
  hashtags