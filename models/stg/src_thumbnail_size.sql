WITH raw as(
  select
    row_number() over(order by width desc, height desc) size_id,
    CAST (width as INT64) as thumbnail_width,
    CAST (height as INT64) as thumbnail_height
  from
    {{source("raw","raw_thumbnail")}}
  group by 
    thumbnail_width,
    thumbnail_height
)
select 
  size_id,
  thumbnail_width,
  thumbnail_height
from raw