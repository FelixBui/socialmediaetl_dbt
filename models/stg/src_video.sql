WITH 
raw AS (
    SELECT
        ROW_NUMBER() OVER(PARTITION BY VideoID) AS rn,
        REGEXP_REPLACE(VideoID, r'[^A-Za-z0-9]+', '') AS video_id,
        REGEXP_REPLACE(ChannelID, r'[^A-Za-z0-9]+', '') AS channel_id,
        EXTRACT(
            DATE
            FROM
                TIMESTAMP_SECONDS(CAST(Timestamp AS INT64)) AT TIME ZONE 'Asia/Ho_Chi_Minh'
        ) AS date_ingested,
        CAST(PARSE_DATE('%Y/%m/%d', Publish_date) AS DATE) AS video_published_date,
        LOWER(TRIM(Title)) AS title,
        LOWER(TRIM(Description)) AS description,
        CAST(Length AS INT64) AS video_length,
        CAST(Views AS INT64) AS video_views,
        TIMESTAMP_SECONDS(CAST(Timestamp AS INT64)) AS timestamp
    FROM
        {{ source('raw', 'raw_video') }}
)
SELECT
    distinct FORMAT_DATE('%Y%m%d', date_ingested) || '_' || video_id AS video_id_key,
    lower(video_id) as video_id,
    channel_id,
    FORMAT_DATE('%Y%m%d', date_ingested) AS date_id_key,
    video_published_date,
    title,
    description,
    video_length,
    video_views,
    timestamp
FROM
    raw
