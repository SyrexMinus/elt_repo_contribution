{{ config(materialized='table', cluster_by=['repo', 'merged_at']) }}

select
    repo,
    pr_num,
    user_login,
    merged_at,
    floor(datediff('second', created_at, merged_at) / 60)::number(38, 0)
        as time_to_ship_minutes,
    updated_at as source_updated_at,
    parse_run_id as source_parse_run_id
from {{ source('silver', 'pull') }}
where state = 'closed'
  and merged_at is not null
