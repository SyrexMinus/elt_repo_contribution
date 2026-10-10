/* chart
{
  "title": "Author share of PRs and diff",
  "filter": {
    "field": "dir",
    "label": "Directory",
    "default": "/",
    "labels": {"/": "All directories", ".": "Root files (.)"}
  },
  "spec": {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "mark": "bar",
    "height": {"step": 18},
    "transform": [
      {"fold": ["pr_count_share", "diff_share"], "as": ["metric", "share"]},
      {"calculate": "datum.metric === 'pr_count_share' ? 'PRs' : 'Changed lines'", "as": "metric_label"}
    ],
    "encoding": {
      "x": {"field": "share", "type": "quantitative", "title": "Share of directory total", "scale": {"domain": [0, 1]}, "axis": {"format": ".0%"}},
      "y": {"field": "user_login", "type": "nominal", "title": "Author", "sort": {"field": "pr_count_share", "op": "max", "order": "descending"}},
      "yOffset": {"field": "metric_label", "sort": ["PRs", "Changed lines"]},
      "color": {"field": "metric_label", "type": "nominal", "title": "Contribution", "scale": {"domain": ["PRs", "Changed lines"], "range": ["#2563eb", "#0d9488"]}},
      "tooltip": [
        {"field": "dir", "type": "nominal", "title": "Directory"},
        {"field": "user_login", "type": "nominal", "title": "Author"},
        {"field": "pr_count_share", "type": "quantitative", "title": "PR share", "format": ".1%"},
        {"field": "diff_share", "type": "quantitative", "title": "Diff share", "format": ".1%"},
        {"field": "user_pr_count", "type": "quantitative", "title": "Author PRs", "format": ",d"},
        {"field": "total_pr_count", "type": "quantitative", "title": "Directory PRs", "format": ",d"},
        {"field": "user_diff", "type": "quantitative", "title": "Author changed lines", "format": ",d"},
        {"field": "total_diff", "type": "quantitative", "title": "Directory changed lines", "format": ",d"}
      ]
    }
  }
}
*/
with parameters as (
    select
        'salute-developers/smart_app_framework' as repo,
        '2022-01-01'::timestamp_ntz as merged_from,
        '2026-10-09'::timestamp_ntz as merged_until,
        0 as min_diff,
        null::number as max_diff
),
scoped_directories as (
    select
        dir,
        user_login,
        pr_num,
        nest_level,
        directory_diff
    from GOLD.fact_pr_directory as facts
    cross join parameters
    where facts.repo = parameters.repo
      and facts.merged_at >= parameters.merged_from
      and facts.merged_at < dateadd(day, 1, parameters.merged_until::date)
      and facts.nest_level = iff(
          facts.dir = '.', 1, array_size(split(facts.dir, '/'))
      )
      and facts.pr_diff >= parameters.min_diff
      and (parameters.max_diff is null or facts.pr_diff <= parameters.max_diff)
),
author_totals as (
    select
        dir,
        user_login,
        count(distinct pr_num) as user_pr_count,
        sum(directory_diff) as user_diff
    from scoped_directories
    group by dir, user_login

    union all

    select
        '/' as dir,
        user_login,
        count(distinct pr_num) as user_pr_count,
        sum(directory_diff) as user_diff
    from scoped_directories
    where nest_level = 1
    group by user_login
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
from directory_totals as directory
where not exists (
    select 1
    from directory_totals as ancestor
    where ancestor.dir <> '/'
      and ancestor.user_pr_count = ancestor.total_pr_count
      and startswith(directory.dir, ancestor.dir || '/')
)
order by pr_count_share desc, total_pr_count desc, dir, user_login;
