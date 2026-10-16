/* chart
{
  "title": "Review interactions",
  "type": "sankey"
}
*/
with parameters as (
    select
        'ClickHouse/ClickHouse' as repo,
        '2022-01-01'::timestamp_ntz as merged_from,
        '2026-10-09'::timestamp_ntz as merged_until,
        20 as max_users
),
interactions as (
    select
        facts.author,
        facts.reviewer,
        count(*) as approved_pr_count
    from GOLD.fact_review_interactions as facts
    cross join parameters
    where facts.repo = parameters.repo
      and facts.merged_at >= parameters.merged_from
      and facts.merged_at < dateadd(day, 1, parameters.merged_until::date)
      and facts.review_state = 'APPROVED'
    group by facts.author, facts.reviewer
),
user_interactions as (
    select 'author' as user_role, author as user_login,
           sum(approved_pr_count) as interaction_count
    from interactions
    group by author

    union all

    select 'reviewer', reviewer, sum(approved_pr_count)
    from interactions
    group by reviewer
),
top_users as (
    select user_role, user_login
    from user_interactions
    cross join parameters
    qualify row_number() over (
        partition by user_role
        order by interaction_count desc, lower(user_login), user_login
    ) <= parameters.max_users
),
grouped_interactions as (
    select
        coalesce(author_user.user_login, 'others') as author,
        coalesce(reviewer_user.user_login, 'others') as reviewer,
        sum(interactions.approved_pr_count) as approved_pr_count
    from interactions
    left join top_users as author_user
        on author_user.user_role = 'author' and author_user.user_login = interactions.author
    left join top_users as reviewer_user
        on reviewer_user.user_role = 'reviewer' and reviewer_user.user_login = interactions.reviewer
    group by 1, 2
)
select author, reviewer, approved_pr_count
from grouped_interactions
order by
    approved_pr_count desc,
    lower(author),
    author,
    lower(reviewer),
    reviewer;
