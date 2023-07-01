select 
  size_id as thumbnail_size_id,
  thumbnail_width,
  thumbnail_height
from 
  {{ref("src_thumbnail_size")}}