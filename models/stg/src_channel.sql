with raw as (
  select
    row_number() over(partition by ChannelID) as rn,
    REGEXP_REPLACE(ChannelID, r'[^A-Za-z0-9]+', '') as channel_id,
    EXTRACT(
      DATE
      FROM
        TIMESTAMP_SECONDS(CAST(Timestamp AS INT64)) AT TIME ZONE 'Asia/Ho_Chi_Minh'
    ) as date_ingested,
    trim(URL) as url,
    lower(trim(Name)) as channel_name,
    lower(trim(Description)) as channel_description,
    CASE
      WHEN upper(Subcribed) LIKE '%K%' THEN CAST(REGEXP_EXTRACT(Subcribed, r'(\d+)') AS INT64) * 1000
      WHEN upper(Subcribed) LIKE '%M%' THEN CAST(REGEXP_EXTRACT(Subcribed, r'(\d+)') AS INT64) * 1000000
    END AS subscriber_count,
    TIMESTAMP_SECONDS(CAST(Timestamp AS INT64)) as timestamp
  from
    {{ source('raw', 'raw_channel') }}
)
select
  format_date('%Y%m%d', date_ingested) || '_' || lower(channel_id) as channel_id_key,
  channel_id,
  format_date('%Y%m%d', date_ingested) date_id_key,
  url,
  channel_name,
  channel_description,
  subscriber_count,
  date_ingested,
  timestamp
from
  raw
where
  rn = 1