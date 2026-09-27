{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['repo', 'pr_num'],
        cluster_by=['repo', 'merged_at']
    )
}}

with changed_pulls as (
    select repo, pr_num
    from {{ source('silver', 'pull') }}
    {% if is_incremental() %}
    where parse_run_id > (
        select coalesce(max(source_parse_run_id), '')
        from {{ this }}
    )
    {% endif %}

    union

    select repo, pr_num
    from {{ source('silver', 'file') }}
    {% if is_incremental() %}
    where parse_run_id > (
        select coalesce(max(source_parse_run_id), '')
        from {{ this }}
    )
    {% endif %}
),
scoped_files as (
    select
        p.repo,
        p.pr_num,
        p.user_login,
        p.merged_at,
        p.updated_at as source_updated_at,
        f.filename,
        f.additions + f.deletions as file_diff,
        greatest(p.parse_run_id, f.parse_run_id) as source_parse_run_id
    from changed_pulls as changed
    inner join {{ source('silver', 'pull') }} as p
        on changed.repo = p.repo
       and changed.pr_num = p.pr_num
    inner join {{ source('silver', 'file') }} as f
        on changed.repo = f.repo
       and changed.pr_num = f.pr_num
    where p.state = 'closed'
      and p.merged_at is not null
),
files_with_pr_diff as (
    select
        *,
        sum(file_diff) over (partition by repo, pr_num) as pr_diff
    from scoped_files
),
directory_levels as (
    select row_number() over (order by seq4()) as nest_level
    from table(generator(rowcount => {{ var('max_directory_depth') }}))
)
select
    f.repo,
    f.pr_num,
    levels.nest_level,
    {{ directory_at_level('f.filename', 'levels.nest_level') }} as dir,
    f.user_login,
    f.merged_at,
    sum(f.file_diff) as directory_diff,
    max(f.pr_diff) as pr_diff,
    max(f.source_updated_at) as source_updated_at,
    max(f.source_parse_run_id) as source_parse_run_id
from files_with_pr_diff as f
cross join directory_levels as levels
group by
    f.repo,
    f.pr_num,
    levels.nest_level,
    dir,
    f.user_login,
    f.merged_at
