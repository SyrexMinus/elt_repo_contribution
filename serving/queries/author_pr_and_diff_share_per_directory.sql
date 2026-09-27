with author_totals as (
    select
        dir,
        user_login,
        count(*) as user_pr_count,
        sum(directory_diff) as user_diff
    from GOLD.fact_pr_directory
    where repo = :repo
      and merged_at = :daterange
      and nest_level = :nest_level
      and case
          when pr_diff < 100 then '0-99'
          when pr_diff < 1000 then '100-999'
          when pr_diff < 10000 then '1000-9999'
          else '10000+'
      end = :pr_diff_range
    group by dir, user_login
),
directory_totals as (
    select
        *,
        sum(user_pr_count) over (partition by dir) as total_pr_count,
        sum(user_diff) over (partition by dir) as total_diff
    from author_totals
)
select
    dir,
    user_login,
    user_pr_count / nullif(total_pr_count, 0) as pr_count_share,
    user_pr_count,
    total_pr_count,
    user_diff / nullif(total_diff, 0) as diff_share,
    user_diff,
    total_diff
from directory_totals
order by pr_count_share desc, total_pr_count desc, dir, user_login
