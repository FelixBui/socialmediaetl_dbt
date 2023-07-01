/*
    2023-06-15: LongTP created fact model for videos
*/

with captions as (
  select
    c.video_id,
    c.caption_id_key,
    c.caption_language_id,
    c.language
  from {{ref("src_caption")}} c
  join {{ref("src_caption_language")}} cl
    on c.caption_language_id = cl.caption_language_id
),
thumbnails as (
  select
    tn.thumbnail_id_key,
    tn.thumbnail_id || '_' || tnz.size_id as thumbnail_size_id,
    tn.video_id,
  from {{ref("src_thumbnail")}} tn
  join {{ref("src_thumbnail_size")}} tnz
    on tn.size_id = tnz.size_id
)
select
  vid.video_id_key,
  tn.thumbnail_id_key,
  cn.channel_id_key,
  c.caption_id_key,
  vid.date_id_key,

  vid.video_length,
  vid.video_views,
  count(distinct tn.thumbnail_size_id) as cnt_thumbnail,
  count(distinct ht.hashtag) as cnt_hashtag,
  count(distinct c.caption_language_id) as cnt_language

from {{ref("src_video")}} vid
left join {{ref("src_video_hashtags")}} ht 
  on vid.video_id = ht.video_id
left join thumbnails tn
  on vid.video_id = tn.video_id
left join {{ref("src_channel")}} cn 
  on vid.channel_id = cn.channel_id
left join captions c
  on vid.video_id = c.video_id
group by 
  1,2,3,4,5,6,7