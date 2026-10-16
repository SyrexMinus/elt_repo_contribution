/* chart
{
  "title": "PRs per directory",
  "type": "treemap"
}
*/
with parameters as (
    select
        'ClickHouse/ClickHouse' as repo,
        '2022-01-01'::timestamp_ntz as merged_from,
        '2026-10-09'::timestamp_ntz as merged_until,
        5 as max_nest_level
),
scoped_directories as (
    select facts.pr_num, facts.dir
    from GOLD.fact_pr_directory as facts
    cross join parameters
    where facts.repo = parameters.repo
      and facts.merged_at >= parameters.merged_from
      and facts.merged_at < dateadd(day, 1, parameters.merged_until::date)
      and facts.nest_level <= parameters.max_nest_level
      and facts.nest_level = iff(
          facts.dir = '.', 1, array_size(split(facts.dir, '/'))
      )
),
directory_counts as (
    select
        dir,
        iff(contains(dir, '/'), regexp_replace(dir, '/[^/]+$', ''), '/')
            as parent_dir,
        count(distinct pr_num) as pr_count
    from scoped_directories
    group by dir

    union all

    select '/', '', count(distinct pr_num)
    from scoped_directories
    having count(*) > 0
)
select
    directory.dir,
    directory.parent_dir,
    directory.pr_count,
    coalesce(parent.pr_count, directory.pr_count) as parent_pr_count,
    directory.pr_count / nullif(parent_pr_count, 0) as pr_share
from directory_counts as directory
left join directory_counts as parent
    on directory.parent_dir = parent.dir
order by directory.pr_count desc, directory.dir;
