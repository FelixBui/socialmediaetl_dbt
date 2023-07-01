select 
  caption_id_key,
  caption_language_id,
  caption_contents,
  timestamp
from 
  {{ref("src_caption")}}