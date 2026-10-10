# Batch ELT Pipeline for Repository Contribution Analytics: Design Document

Status: Implemented (see USAGE.md)

Author: Makar Shevchenko

Scope: Batch ELT pipeline with medallion lakehouse, serving analytics of contribution & code-review health for large open-source repositories.

## 1. Context

### Problem

Maintainers of large open-source projects lack visibility into the health and risk profile of their contribution process.
They operate subjectively about questions that are actually measurable:
- Is one person reviewing everything and heading toward burnout?
- Are newcomer PRs rotting in the queue until contributors leave?
- Is critical expertise for a module concentrated in a single person (bus factor = 1)?
- How long does it actually take to ship a change, and where are the bottlenecks?

### Users

Table below shows jobs to be done for user groups: repo managers, and pipeline operators.

| User                       | Primary jobs-to-be-done                                                   |
|----------------------------|---------------------------------------------------------------------------|
| Repo maintainer / lead     | See health & risk signals: review load concentration, time-to-ship, bus factor per area of the codebase, review interaction graph. Weekly-ish read pattern, not real-time. |
| Pipeline operator (us)     | Monitor pipeline health, add new marts, run backfills, respond to alerts. |

> Explicit non-goal: this is not a "rank developers for bonuses" tool.
The product sells risk & health awareness.
Contribution metrics are a means (surfacing concentration/bottlenecks), not the end.

## 2. Requirements

### Functional

- Ingest PRs, PR files, and PR reviews from the GitHub REST API for a configured set of repositories.
- Transform raw API responses into a clean, deduplicated tabular model.
- Serve 4 analytical marts (see Chapter 6) via dashboards, filterable by repo and time window.

### Non-functional

The system is designed for the target scale -- ~2.5 GB/day across 10 000 repositories, 10 000 managers/day at ~10 RPS peak.
The demo runs at MVP scale (2 repos).
Table below shows characteristics of the demo and target scales. 

| Dimension                 | Demo                                        | Target scale                            |
|---------------------------|---------------------------------------------|-----------------------------------------|
| Repositories              | 2 (`apache/spark`, `ClickHouse/ClickHouse`) | 10 000 repos                            |
| Data volume               | single-digit MB/day                         | ~2.5 GB/day raw*                        |
| Freshness                 | 6h ingestion cadence                        | 1h cadence; SLA: 99% of new data in 24h |
| Availability (dashboards) | upon request                                | 99% -- not a transactional system       |
| Consistency               | Eventual; 1 PR = 1 latest-state row         | same                                    |
| Read load                 | ~10 views/day, effectively no concurrency   | 10 000 managers/day, ~10 RPS peak       |
| Cost                      | ~$0                                         | explicit & bounded (see Chapter 9)      |
| Growth                    | -                                           | ~50%/year in repos/volume               |

*Bronze; ~250 KB/repo/day of raw JSON -- Silver is smaller after field selection

Marts use cached aggregates.

## 3. Right-Sizing Decision

At the Demo volume (single-digit MB/day, 6h cadence), the problem is fully solved by a single Python poller writing into ClickHouse, with SQL views as marts.
That MVP costs ~$0 and is operationally trivial.

Stack for the target scale (Chapter 2: ~2.5 GB/day, 10 000 repos, 10 000 managers/day at ~10 RPS) is S3, Spark, Snowflake, dbt, orchestrated by Airflow, provisioned by Terraform.
The stack transition triggers from the scale, where naive stack fails on:
- `too many parts` ClickHouse error appear once ingestion exceeds ~1M inserts/day -- reached around the 10 000-repo mark.
- Backfill of the full history for 10 000 repos would take ~a day on a single poller.
- Heavy cross-entity joins (e.g. review-interaction graph across all 10 000 repos) outgrow a single node.
- ~10 RPS of concurrent maintainer reads against fresh aggregates outgrows a single-node OLAP serving layer.

At that scale the managed stack earns its keep, and it also buys operational simplicity out of the box: dbt gives tests + lineage + docs, Snowflake gives auto-suspend + elastic compute, Terraform gives reproducible teardown.
This document therefore describes the target-scale design; Chapter 9 tracks the cost this implies and Chapter 11 records what I'd simplify for a genuine MVP.

## 4. Architecture

Architecture diagram is as follows:
```
              ┌──────────────┐
              │  GitHub API  │  (/pulls, /files, /reviews)
              └──────┬───────┘
                     │ (1) hourly, incremental (sort=updated)
              ┌──────▼───────┐
Airflow ───▶  │  Poller      │  batches to ~1000 PR / ~10MB objects,
(cron 1h)     │  (Python)    │  preserving raw JSON down to 1 response
              └──────┬───────┘
                     │ writes
              ┌──────▼───────┐
              │  S3 (Bronze) │  raw responses + pipeline artifacts by run_id
              └──────┬───────┘
                     │ (2) Airflow triggers
              ┌──────▼───────┐
              │  Spark       │  flatten JSON → rows, select fields,
              │  (Silver)    │  enforce schema, MERGE INTO (upsert)
              └──────┬───────┘
                     │ writes
              ┌──────▼───────┐
              │  Snowflake   │  pull / file / review tables (1 entity = 1 row)
              └──────┬───────┘
                     │ (3) Airflow triggers
              ┌──────▼───────┐
              │  dbt (Gold)  │  3 facts, tests, docs
              └──────┬───────┘
                     │
              ┌──────▼───────┐
              │  Dashboards  │  4 marts (Streamlit in Snowflake), served 
              │  (Snowflake) |  with Query Result Cache
              └──────────────┘

Cross-cutting: Terraform provisions all AWS/Snowflake resources.
               CloudWatch collects per-stage metrics + alerts.
```

### Medallion layers

Data layers according to medallion framework:
- Bronze (S3): Raw API responses, batched into ~10MB objects to avoid the small-files problem, structure preserved down to 1 response.
               Immutable landing zone.
- Silver (Snowflake): Flattened, typed, deduplicated.
                      Tables `pull`, `file`, `review` -- the single source of truth.
- Gold (Snowflake, dbt): Three fact tables supplying the four analytical marts (see Chapter 6).

### Orchestration & stage handoff

Airflow DAG runs hourly.
Artifacts are passed between stages via XCom in form of S3 paths; the data itself lives in S3, partitioned by `run_id` (`pipeline-artifacts/{run_id}/{stage}/output.parquet`).

### Ingestion modes

The ingestion modes are:
- Incremental (default, hourly): `GET .../pulls?state=all&sort=updated&direction=desc`, page downward until `updated_at < last_successful_watermark`.
                                 Files/reviews for a PR are fetched on change; merged PRs are effectively immutable, so their files/reviews are cached and not re-fetched.
- Backfill (explicit mode): Initial / recovery run that walks full history.
                            The mode is PR-wise.

## 5. Data Model

### Silver

The single source of truth is three tables in silver layer: `pull`, `file`, and `review`.
Their schemas are as follows:
```
pull   (primary key (PK): repo + pr_num)
  repo, pr_num, merged_at, user_login, state, created_at, updated_at,
  title, parse_run_id
  -- sort order (physical): repo, state, merged_at

file   (PK: repo + pr_num + filename)
  repo, pr_num, filename, additions, deletions, parse_run_id
  -- sort order: repo, pr_num, filename

review (PK: repo + pr_num + user_login)
  repo, pr_num, user_login, state, parse_run_id
  -- sort order: repo, pr_num, state
```

All three join on `repo + pr_num`.
Physical sort order chosen to match marts' query access patterns.
`parse_run_id` column is used for data freshness checking and partial backfilling.

### Gold

The three dbt-computed fact tables contain merged PRs and supply the four rendered marts.

#### Shipping time fact

```text
fact_time_to_ship (PK: repo + pr_num)
  repo, pr_num, user_login, merged_at, time_to_ship_minutes,
  source_updated_at, source_parse_run_id
```

#### PR directory fact

```text
fact_pr_directory (PK: repo + pr_num + nest_level + dir)
  repo, pr_num, nest_level, dir, user_login, merged_at, directory_diff,
  pr_diff, source_updated_at, source_parse_run_id
```

#### Review interactions fact

```text
fact_review_interactions (PK: repo + pr_num + reviewer)
  repo, pr_num, author, reviewer, review_state, merged_at,
  source_updated_at, source_parse_run_id
```

### Idempotency & deduplication

The pipeline is idempotent by featuring:
- Polling restart reuses `last_successful_watermark` in `updated_at < last_successful_watermark` filtering ensuring inclusion of the same PRs.
  The difference between re-runs result is fetching PR's most recent state, including `updated_at`.
- Spark writes use upsert: `MERGE INTO ... ON <PK> WHEN MATCHED AND s.parse_run_id > t.parse_run_id THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`, keeping the invariant 1 entity = 1 latest row.
- Stage re-runs overwrite `s3://.../{run_id}/{stage}/` rather than appending.

## 6. Analytical Marts

Each mart answers a maintainer question.
Streamlit renders the marts with repository and merge-time window filters based on dbt-computed fact tables.

### 6.1 Time-to-ship per author

Question of the mart: who experiences slow merges, and how bad is the tail?
Presentation: grouped bar chart.
Source fact: [GOLD.fact_time_to_ship](#shipping-time-fact).

Rendered mart schema, one row per author:

```text
time_to_ship_per_author (PK: user_login)
  user_login, pr_count, p50_minutes, p75_minutes, p90_minutes
```

### 6.2 PRs per directory

Question of the mart: which areas of the codebase attract the most change?
Presentation: treemap.
Source fact: [GOLD.fact_pr_directory](#pr-directory-fact).

Rendered mart schema, one row per directory:

```text
pr_count_per_directory (PK: dir)
  dir, parent_dir, pr_count, parent_pr_count, pr_share
```

A PR touching N directories counts in each -- intended (it genuinely touched N areas).

### 6.3 Author share of PRs & diff per directory

Question of the mart: who owns the churn in each area? Concentration is a bus-factor signal.
Presentation: grouped bar chart with a table.
Source fact: [GOLD.fact_pr_directory](#pr-directory-fact).

Rendered mart schema, one row per directory and author:

```text
author_pr_and_diff_share_per_directory (PK: dir + user_login)
  dir, user_login, pr_count_share, user_pr_count, total_pr_count,
  diff_share, user_diff, total_diff
```

PRs are first filtered by total diff size (guards against giant autogen / mass-delete PRs via `:min_diff` / `:max_diff`).

For better visualization, mart's frontend does not show child rows of 100%-ownership dirs.

### 6.4 Review interactions

Question: who reviews whom? Surfaces review-load concentration / single-reviewer risk.
Presentation: Sankey diagram.
Source fact: [GOLD.fact_review_interactions](#review-interactions-fact).

Rendered mart schema, one row per author and reviewer:

```text
approved_review_count_by_author_and_reviewer (PK: author + reviewer)
  author, reviewer, approved_pr_count
```

## 7. Data Quality & Observability

### Corrupted pools

Table below shows quarantine rules per stage and location for trace data.
Corrupted records do not propagate downstream.

| Stage  | Quarantine rules                                                      | Trace data & location                |
|--------|-----------------------------------------------------------------------|--------------------------------------|
| Poller | Requests with `status != 200` / empty bodies                          | Offending URL query params, `s3://.../corrupted-artifacts/{run_id}/stage1/output.parquet` |
| Spark  | Rows missing required fields* / type violations / out-of-range values | Input row copy, S3, 1 file per table |
| dbt    | Output row count below expected floor (empty-increment guard)         | dbt test failure                     |

*Schema-drift check

### Freshness

Freshness checks per layer:
- Poller: timestamp of the latest `pipeline-artifacts/{run_id}/stage1/output.parquet`.
- Spark/Silver: `MAX(pull.updated_at)` from the source data.
- Gold: materialized marts' `updated_at` in Snowflake.

### CloudWatch metrics & alerts

Metrics are: `job_start_count`, `inflight_job_count`, `job_duration`, `job_finish_status` (success / empty / fail).

Alerts are: error-rate share, stage duration exceeding threshold, `corrupted_count > 0`, data-freshness beyond SLA, empty-increment where a non-empty one is expected.

## 8. Failure Modes & Recovery

Failed stage runs are re-run safely thanks to idempotency (recall Chapter 5: MERGE + `run_id`-scoped overwrite).

Failure effect analysis:
- S3 as the write broker is effectively the pipeline's single point of coordination;
- The pipeline is sequential, so any stage failing halts forward progress -- but reads of already-written data are unaffected by a failing new write run.

Backup the single-source-of-truth tables (`pull`, `file`, `review`).
Marts are not backed up -- they are cheaply recomputable from Silver.

Recovery scenarios:
- Stop + backfill: halt ingestion, run backfill polling;
- Corrupted-record: get `corrupted_count` alert fire (downstream staying clean), analyze the corrupted pool, fix the rootcause and re-run the stage.

## 9. Cost

### MVP

Single Python poller + ClickHouse (single node) + SQL views.
Effectively ~$0 beyond a tiny always-on host.
This is the honest baseline.

### Target-scale

Total is ~$620–700/mo, Airflow (MWAA) + Snowflake dominated.
Decomposition:
- Airflow: ~$350. MWAA, managed, effortless high availability (demo uses self-managed Docker on free-tier EC2 t3.micro).
- Snowflake compute: ~$220–260 / month. Computation: X-Small = ~$2/hr, auto-suspend after 1 min; hourly Silver `MERGE` + dbt Gold = 5–10 min active/run; reads served from Query Result Cache.
- Spark: ~$30–60 amortized. EMR transient / Serverless, not kept hot; backfill + occasional heavy compaction only; hourly increment is light.
- Snowflake storage: ~$10–25. Silver + Gold compressed, ~1 yr accumulation ~100–300 GB @ ~$23–40/TB-mo.
- CloudWatch: ~$5. Per-stage metrics + a handful of alarms.
- S3: ~$1–2. Bronze + artifacts. 2.5 GB/day raw × 7-day lifecycle = 18 GB resident + parquet artifacts; ~250 PUT/day of ~10 MB objects; intra-region reads free.

Cost risks:
- Cost scales roughly with data volume / repo count; at ~50%/yr growth Snowflake compute grows fastest, so it is the lever to watch — keep the warehouse small and suspended.
- Spark is the one component that can silently drain the budget if a cluster is left hot; it runs transiently for backfill + compaction only, never as a standing cluster.
- The demo runs at MVP scale, so actual spend stays ~$0, safely under the $10/month AWS budget alert; that guardrail exists to catch runaway Spark, not to cap the target-scale figure.
`terraform destroy` tears everything down in one command.

## 10. Scaling

Scaling approaches per characteristic:
- Compute scales by adding Airflow workers, Spark executors, and Snowflake warehouse size/multi-cluster.
Snowflake and S3 storage is elastic and managed, minimum of our management required.
- Durability is provided by the platform: S3 gives 11-nines durability with cross-AZ replication built in.
- Sharding of processing by `pr_num` -- a naturally small, evenly distributed key that aggregations group on.
- Read caching via Snowflake Query Result Cache for the maintainer read pattern.

## 11. Trade-offs

- Batch (1h) over streaming: A maintainer read pattern that is weekly-ish (Chapter 2) does not justify Kafka/Flink. Batching buys operational and cost simplicity at the price of freshness.
- Latest-state snapshot over event history: The "1 entity = 1 latest row" invariant (Chapter 5) makes deduplication and storage trivial, but it discards state transitions: we cannot reconstruct how long a PR sat in each review state, reopen/close churn, or review-latency evolution over time.
- GitHub REST + per-PR fan-out under a rate-limit ceiling: Fetching `/pulls` then `/files` and `/reviews` per PR is bounded by the 5000 req/hr limit; at 10 000 repos this is an ingestion constraint.
- Serving analytics directly from Snowflake via Query Result Cache: (Chapter 10) avoids a dedicated low-latency serving tier, but leans on high cache-hit rates. A materialized serving store would give predictable read latency at the cost of another moving part
- Sequential DAG with S3 as the sole coordinator: No cross-repo failure isolation or partial parallelism.
- Path+diff as an ownership / bus-factor proxy: Directory path plus diff size is universal and cheap but imperfect: a large diff is not expertise, and reviewers aren't captured as owners.
