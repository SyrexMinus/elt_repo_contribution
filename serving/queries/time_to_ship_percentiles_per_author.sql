select
    user_login,
    count(*) as pr_count,
    percentile_disc(0.50) within group (order by time_to_ship_minutes)
        as p50_minutes,
    percentile_disc(0.75) within group (order by time_to_ship_minutes)
        as p75_minutes,
    percentile_disc(0.90) within group (order by time_to_ship_minutes)
        as p90_minutes
from GOLD.fact_time_to_ship
where repo = :repo
  and merged_at = :daterange
group by user_login
order by p75_minutes desc, user_login
