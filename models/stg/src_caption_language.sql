with raw as (
  select
    distinct
    trim(lower(Language)) as language
  from
    {{source("raw","raw_caption")}}
)
SELECT
    dense_rank() over(order by Language) as caption_language_id,
    language
FROM
  raw