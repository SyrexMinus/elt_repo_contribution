{{ config(materialized='table', cluster_by=['repo', 'merged_at']) }}

select
    p.repo,
    p.pr_num,
    p.user_login as author,
    r.user_login as reviewer,
    r.state as review_state,
    p.merged_at,
    p.updated_at as source_updated_at,
    greatest(p.parse_run_id, r.parse_run_id) as source_parse_run_id
from {{ source('silver', 'pull') }} as p
inner join {{ source('silver', 'review') }} as r
    on p.repo = r.repo
   and p.pr_num = r.pr_num
where p.state = 'closed'
  and p.merged_at is not null
