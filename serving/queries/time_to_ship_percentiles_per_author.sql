/* chart
{
  "title": "Time to ship per author",
  "spec": {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "mark": "bar",
    "height": {"step": 12},
    "transform": [
      {"fold": ["p50_minutes", "p75_minutes", "p90_minutes"], "as": ["percentile", "minutes"]},
      {"calculate": "upper(replace(datum.percentile, '_minutes', ''))", "as": "percentile_label"}
    ],
    "encoding": {
      "x": {"field": "minutes", "type": "quantitative", "title": "Minutes from creation to merge"},
      "y": {"field": "user_login", "type": "nominal", "title": "Author", "sort": {"field": "p75_minutes", "op": "max", "order": "descending"}},
      "yOffset": {"field": "percentile_label", "sort": ["P50", "P75", "P90"]},
      "color": {"field": "percentile_label", "type": "nominal", "title": "Percentile", "scale": {"domain": ["P50", "P75", "P90"], "range": ["#2563eb", "#0d9488", "#ea580c"]}},
      "tooltip": [
        {"field": "user_login", "type": "nominal", "title": "Author"},
        {"field": "percentile_label", "type": "nominal", "title": "Percentile"},
        {"field": "minutes", "type": "quantitative", "title": "Minutes", "format": ",.0f"},
        {"field": "pr_count", "type": "quantitative", "title": "Merged PRs", "format": ",d"}
      ]
    }
  }
}
*/
with parameters as (
    select
        'ClickHouse/ClickHouse' as repo,
        '2022-01-01'::timestamp_ntz as merged_from,
        '2026-10-09'::timestamp_ntz as merged_until,
        20 as max_users
),
scoped_pulls as (
    select user_login, time_to_ship_minutes
    from GOLD.fact_time_to_ship as facts
    cross join parameters
    where facts.repo = parameters.repo
      and facts.merged_at >= parameters.merged_from
      and facts.merged_at < dateadd(day, 1, parameters.merged_until::date)
),
top_authors as (
    select user_login
    from scoped_pulls
    cross join parameters
    group by user_login, parameters.max_users
    qualify row_number() over (
        order by count(*) desc, lower(user_login), user_login
    ) <= parameters.max_users
),
grouped_pulls as (
    select
        coalesce(top_authors.user_login, 'others') as user_login,
        scoped_pulls.time_to_ship_minutes
    from scoped_pulls
    left join top_authors on top_authors.user_login = scoped_pulls.user_login
)
select
    user_login,
    count(*) as pr_count,
    percentile_disc(0.50) within group (order by time_to_ship_minutes)
        as p50_minutes,
    percentile_disc(0.75) within group (order by time_to_ship_minutes)
        as p75_minutes,
    percentile_disc(0.90) within group (order by time_to_ship_minutes)
        as p90_minutes
from grouped_pulls
group by user_login
order by p75_minutes desc, user_login;
