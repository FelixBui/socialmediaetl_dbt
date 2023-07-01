select
  video_id,
  hashtag
from 
  {{ref("src_video_hashtags")}}