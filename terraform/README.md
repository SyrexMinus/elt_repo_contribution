# Terraform — ELT Repo-Contribution Pipeline

Provisions the target-scale infrastructure from the design doc (`../README.md`):
S3 (Bronze) → Spark/EMR Serverless (Silver) → Snowflake + dbt (Gold), orchestrated
by Airflow, observed with CloudWatch.

> **Stubs, not implementations.** The Terraform here reflects the *target
> structure* of resources. The stage *code* it references (poller, Spark job,
> dbt models, Airflow DAG) lives under `../stages/` and is intentionally left as
> `# TODO` placeholders. Wiring is real; business logic is not.

## Layout

```
terraform/
  versions.tf              # terraform + provider version constraints
  providers.tf             # aws + snowflake providers
  backend.tf               # remote S3 state (commented; local by default)
  variables.tf             # global inputs
  main.tf                  # module composition
  outputs.tf               # bucket names, airflow url, snowflake db, ...
  terraform.tfvars.example # copy -> terraform.tfvars
  modules/
    s3/          # Bronze bucket + artifacts bucket, 7-day lifecycle, encryption
    iam/         # roles/policies for the Airflow host + EMR Serverless
    spark/       # EMR Serverless application (transient) + stub job upload
    airflow/     # self-managed Docker on free-tier EC2 t3.micro + DAG env
    snowflake/   # database, SILVER/GOLD schemas, X-Small warehouse, source tables
    cloudwatch/  # SNS alerts, alarms (corrupted/duration/failure), $10 budget
../stages/       # STUB stage implementations referenced by Terraform
  poller/handler.py
  spark/silver_job.py
  dbt/ (dbt_project.yml + models/gold/*.sql + sources.yml)
  airflow_dags/elt_dag.py
```

## Notes on choices

- **Airflow is self-managed Docker on a free-tier EC2 `t3.micro`** (per the
  design doc's demo note), not MWAA. The `airflow` module boots Docker via
  `user_data` and injects the pipeline config as an `.env` for the DAG.
- **Spark = EMR Serverless**, auto-start / auto-stop after 1 min idle so only
  backfill/compaction incur meaningful cost.
- **Snowflake** warehouse is X-Small with 1-min auto-suspend; the SILVER
  `pull`/`file`/`review` tables match the data model in ch.5.
- **Budget** alarm (~$10/mo) is the guardrail against runaway Spark.

## Usage

```bash
cp terraform.tfvars.example terraform.tfvars   # then edit
terraform init
terraform plan
terraform apply
# tear everything down in one command:
terraform destroy
```

Secrets (Snowflake auth, etc.) are supplied via environment variables, never
committed. Remote state (`backend.tf`) is commented out so `init` works locally;
uncomment once a state bucket + lock table exist.
