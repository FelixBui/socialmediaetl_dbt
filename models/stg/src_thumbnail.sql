with raw as (
    select
        row_number() over(partition by ThumbnailID) as rn,
        REGEXP_REPLACE(SPLIT(ThumbnailID,"_")[SAFE_OFFSET(0)], r'[^A-Za-z0-9]+', '') as thumbnail_id,
        REGEXP_REPLACE(VideoID, r'[^A-Za-z0-9]+', '') as video_id,
        dense_rank () over (order by width desc,height desc) as size_id,
        EXTRACT(
            DATE
            FROM
                TIMESTAMP_SECONDS(CAST(Timestamp AS INT64)) AT TIME ZONE 'Asia/Ho_Chi_Minh'
        ) as date_ingested,
        trim(URL) as url,
        TIMESTAMP_SECONDS(CAST(Timestamp AS INT64)) as timestamp
    from
        {{source("raw","raw_thumbnail")}} TN
)
select
    distinct
    format_date('%Y%m%d', date_ingested) || '_' || lower(thumbnail_id) as thumbnail_id_key,
    lower(thumbnail_id) as thumbnail_id,
    lower(video_id) as video_id,
    size_id,
    format_date('%Y%m%d', date_ingested) date_id_key,
    url,
    date_ingested,
    timestamp
from
    raw
where
    rn = 1