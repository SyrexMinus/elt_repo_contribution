/* chart
{
  "title": "Approved reviews by author and reviewer",
  "spec": {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "mark": "rect",
    "height": {"step": 28},
    "encoding": {
      "x": {"field": "reviewer", "type": "nominal", "title": "Reviewer", "sort": "ascending", "axis": {"labelAngle": -40}},
      "y": {"field": "author", "type": "nominal", "title": "Author", "sort": "ascending"},
      "color": {"field": "approved_pr_count", "type": "quantitative", "title": "Approved PRs", "scale": {"scheme": "blues", "zero": true}},
      "tooltip": [
        {"field": "author", "type": "nominal", "title": "Author"},
        {"field": "reviewer", "type": "nominal", "title": "Reviewer"},
        {"field": "approved_pr_count", "type": "quantitative", "title": "Approved PRs", "format": ",d"}
      ]
    }
  }
}
*/
with parameters as (
    select
        'salute-developers/smart_app_framework' as repo,
        '2022-01-01'::timestamp_ntz as merged_from,
        '2026-10-09'::timestamp_ntz as merged_until
)
select
    author,
    reviewer,
    count(*) as approved_pr_count
from GOLD.fact_review_interactions as facts
cross join parameters
where facts.repo = parameters.repo
  and facts.merged_at >= parameters.merged_from
  and facts.merged_at < parameters.merged_until
  and review_state = 'APPROVED'
group by author, reviewer
order by
    approved_pr_count desc,
    lower(author),
    author,
    lower(reviewer),
    reviewer;
