select 
  video_id_key,
  video_published_date,
  title as video_title,
  description as video_description,
  timestamp
from
  {{ref("src_video")}}