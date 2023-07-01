select 
  caption_language_id,
  language as caption_language
from 
  {{ref("src_caption_language")}}