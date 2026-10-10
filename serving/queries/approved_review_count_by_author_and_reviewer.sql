/* chart
{
  "title": "Review interactions",
  "type": "sankey"
}
*/
with parameters as (
    select
        'salute-developers/smart_app_framework' as repo,
        '2022-01-01'::timestamp_ntz as merged_from,
        '2026-10-09'::timestamp_ntz as merged_until
)
select
    facts.author,
    facts.reviewer,
    count(*) as approved_pr_count
from GOLD.fact_review_interactions as facts
cross join parameters
where facts.repo = parameters.repo
  and facts.merged_at >= parameters.merged_from
  and facts.merged_at < dateadd(day, 1, parameters.merged_until::date)
  and review_state = 'APPROVED'
group by facts.author, facts.reviewer
order by
    approved_pr_count desc,
    lower(facts.author),
    facts.author,
    lower(facts.reviewer),
    facts.reviewer;
