select
  channel_id_key,
  channel_name,
  channel_description,
  url as channel_url,
  subscriber_count,
  timestamp
from
  {{ref("src_channel")}}