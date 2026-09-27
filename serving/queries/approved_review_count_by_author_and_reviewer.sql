select
    author,
    reviewer,
    count(*) as approved_pr_count
from GOLD.fact_review_interactions
where repo = :repo
  and merged_at = :daterange
  and review_state = 'APPROVED'
group by author, reviewer
order by
    approved_pr_count desc,
    lower(author),
    author,
    lower(reviewer),
    reviewer
