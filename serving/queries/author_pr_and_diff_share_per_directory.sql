/* chart
{
  "title": "Author share of PRs and diff",
  "filter": {"field": "dir", "label": "Directory"},
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
        1 as nest_level,
        array_construct('0-99', '100-999', '1000-9999', '10000+') as pr_diff_ranges
),
author_totals as (
    select
        dir,
        user_login,
        count(*) as user_pr_count,
        sum(directory_diff) as user_diff
    from GOLD.fact_pr_directory as facts
    cross join parameters
    where facts.repo = parameters.repo
      and facts.merged_at >= parameters.merged_from
      and facts.merged_at < parameters.merged_until
      and facts.nest_level = parameters.nest_level
      and array_contains(
          case
              when pr_diff < 100 then '0-99'
              when pr_diff < 1000 then '100-999'
              when pr_diff < 10000 then '1000-9999'
              else '10000+'
          end::variant,
          parameters.pr_diff_ranges
      )
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
order by pr_count_share desc, total_pr_count desc, dir, user_login;
