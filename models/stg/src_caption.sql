with raw as (
  select
    row_number() over(partition by Contents_caption_id) as rn,
    REGEXP_REPLACE(SPLIT(Contents_caption_id,"_")[SAFE_OFFSET(0)], r'[^A-Za-z0-9]+', '') as caption_id,
    REGEXP_REPLACE(VideoID, r'[^A-Za-z0-9]+', '') as video_id,
    DENSE_RANK() OVER(ORDER BY Language) as caption_language_id,
    EXTRACT(
      DATE
      FROM
        TIMESTAMP_SECONDS(CAST(Timestamp AS INT64)) AT TIME ZONE 'Asia/Ho_Chi_Minh'
    ) as date_ingested,
    trim(lower(Language)) as language,
    TO_JSON_STRING(Contents) as caption_contents,
    TIMESTAMP_SECONDS(CAST(Timestamp AS INT64)) as timestamp
  from
    {{source("raw","raw_caption")}}
)
SELECT
    FORMAT_DATE('%Y%m%d', date_ingested) || '_' || caption_id AS caption_id_key,
    caption_id,
    video_id,
    caption_language_id,
    FORMAT_DATE('%Y%m%d', date_ingested) AS date_id_key,
    language,
    trim(lower(caption_contents)) as caption_contents,
    timestamp
FROM
  raw