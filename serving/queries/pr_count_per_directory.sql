select
    dir,
    count(*) as pr_count
from GOLD.fact_pr_directory
where repo = :repo
  and merged_at = :daterange
  and nest_level = :nest_level
group by dir
order by pr_count desc, dir
