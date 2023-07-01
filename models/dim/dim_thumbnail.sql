select 
  thumbnail_id_key,
  size_id as thumbnail_size_id,
  url as thumbnail_url,
  timestamp
from 
  {{ref("src_thumbnail")}}