/* chart
{
  "title": "PRs per directory",
  "spec": {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "mark": {"type": "bar", "color": "#2563eb"},
    "height": {"step": 32},
    "encoding": {
      "x": {"field": "pr_count", "type": "quantitative", "title": "Merged PRs", "axis": {"tickMinStep": 1}},
      "y": {"field": "dir", "type": "nominal", "title": "Directory", "sort": "-x"},
      "tooltip": [
        {"field": "dir", "type": "nominal", "title": "Directory"},
        {"field": "pr_count", "type": "quantitative", "title": "Merged PRs", "format": ",d"}
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
        1 as nest_level
)
select
    dir,
    count(*) as pr_count
from GOLD.fact_pr_directory as facts
cross join parameters
where facts.repo = parameters.repo
  and facts.merged_at >= parameters.merged_from
  and facts.merged_at < parameters.merged_until
  and facts.nest_level = parameters.nest_level
group by dir
order by pr_count desc, dir;
