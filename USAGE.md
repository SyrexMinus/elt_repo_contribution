# End-to-end setup: Ubuntu 24.04 LTS x86_64

## 1. Prepare access

Use a machine with sudo, 4 CPUs, 8 GB RAM, 30 GB free disk space, and access to mirror.yandex.ru, terraform-mirror.yandexcloud.net, Docker Hub, GitHub, AWS, and Snowflake.
Prepare AWS IAM access keys with AdministratorAccess, a Snowflake ACCOUNTADMIN login, a GitHub token with repository read access, and an alert email.
For accounts created with Sign up for AWS (new), open AWS Settings → Billing → Upgrade and activate Paid Plan.
If your organization uses custom SCPs, have its administrator permit these EMR Serverless actions.

```text
emr-serverless:CreateApplication
emr-serverless:GetApplication
emr-serverless:UpdateApplication
emr-serverless:DeleteApplication
emr-serverless:TagResource
emr-serverless:UntagResource
emr-serverless:ListTagsForResource
emr-serverless:StartApplication
emr-serverless:StartJobRun
emr-serverless:GetJobRun
emr-serverless:CancelJobRun
```

Run steps 3–12 in one Bash session.
Execute each command block separately and enter requested values before continuing to the next block.

## 2. Install tools

```bash
bash
```

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git jq openssh-client openssl python3 python3-venv unzip
```

```bash
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
printf '%s\n' 'Types: deb' 'URIs: https://download.docker.com/linux/ubuntu' \
  'Suites: noble' 'Components: stable' 'Architectures: amd64' \
  'Signed-By: /etc/apt/keyrings/docker.asc' \
  | sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
```

```bash
ELT_TERRAFORM_INSTALL_DIR="$(mktemp -d)"
curl -fsSL https://mirror.yandex.ru/mirrors/releases.hashicorp.com/terraform/1.9.8/terraform_1.9.8_linux_amd64.zip \
  -o "$ELT_TERRAFORM_INSTALL_DIR/terraform.zip"
printf '186e0145f5e5f2eb97cbd785bc78f21bae4ef15119349f6ad4fa535b83b10df8  %s\n' \
  "$ELT_TERRAFORM_INSTALL_DIR/terraform.zip" | sha256sum -c -
unzip -q "$ELT_TERRAFORM_INSTALL_DIR/terraform.zip" -d "$ELT_TERRAFORM_INSTALL_DIR"
sudo install -m 0755 "$ELT_TERRAFORM_INSTALL_DIR/terraform" /usr/local/bin/terraform
rm -rf "$ELT_TERRAFORM_INSTALL_DIR"
```

```bash
ELT_INSTALL_DIR="$(mktemp -d)"
curl -fsSL https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip \
  -o "$ELT_INSTALL_DIR/awscliv2.zip"
unzip -q "$ELT_INSTALL_DIR/awscliv2.zip" -d "$ELT_INSTALL_DIR"
sudo "$ELT_INSTALL_DIR/aws/install"
rm -rf "$ELT_INSTALL_DIR"
```

Log out and log back in.

## 3. Configure AWS and clone the project

Start Bash and keep this session open through step 12.

```bash
bash
```

Enter the AWS access keys, region us-east-2, and output format json.

```bash
aws configure --profile elt-demo
```

```bash
export AWS_PROFILE=elt-demo
export AWS_REGION=us-east-2
export AWS_DEFAULT_REGION="$AWS_REGION"
export AWS_PAGER=""
cd "$HOME"
git clone https://github.com/SyrexMinus/p_elt_repo_contribution.git
cd p_elt_repo_contribution
export PROJECT_ROOT="$PWD"
```

For a private checkout, enter your GitHub username and repository-read token at the Git prompt.

```bash
printf '%s\n' '/terraform/terraform.tfvars' '/terraform/terraform.tfstate' \
  '/terraform/terraform.tfstate.*' '/terraform/*.tfplan' >> .git/info/exclude
```

## 4. Set deployment inputs

In app.snowflake.com, open account details and copy the organization-account identifier without https:// or .snowflakecomputing.com.
Enter that identifier and your alert email at the prompts.

```bash
export AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export NAME_PREFIX="elt-$AWS_ACCOUNT_ID"
export ENVIRONMENT=demo
export SNOWFLAKE_PREFIX="$(printf '%s_%s' "$NAME_PREFIX" "$ENVIRONMENT" | tr '[:lower:]-' '[:upper:]_')"
export SNOWFLAKE_TERRAFORM_USER="${SNOWFLAKE_PREFIX}_TERRAFORM"
export SNOWFLAKE_RUNTIME_USER="${SNOWFLAKE_PREFIX}_PIPELINE_USER"
export SNOWFLAKE_RUNTIME_ROLE="${SNOWFLAKE_PREFIX}_PIPELINE"
export GITHUB_PARAMETER="/$NAME_PREFIX/$ENVIRONMENT/github-token"
export SNOWFLAKE_PARAMETER="/$NAME_PREFIX/$ENVIRONMENT/snowflake-token"
export AIRFLOW_ECR_REPOSITORY="$NAME_PREFIX-airflow"
export SILVER_ECR_REPOSITORY="$NAME_PREFIX-silver"
export EC2_KEY_NAME="$NAME_PREFIX-$ENVIRONMENT"
export SSH_PRIVATE_KEY="$HOME/.ssh/$EC2_KEY_NAME.pem"
export REPOSITORIES_JSON='["salute-developers/smart_app_framework"]'
export OPERATOR_PUBLIC_IP="$(curl -4 -fsS https://checkip.amazonaws.com | tr -d '\n')"
```

```bash
read -r -p 'Alert email: ' ALERT_EMAIL
```

```bash
read -r -p 'Snowflake account identifier (organization-account): ' SNOWFLAKE_ACCOUNT
```

```bash
export ALERT_EMAIL SNOWFLAKE_ACCOUNT

mkdir -p "$HOME/.config/elt-demo"
declare -p AWS_PROFILE AWS_REGION AWS_DEFAULT_REGION AWS_PAGER PROJECT_ROOT \
  AWS_ACCOUNT_ID NAME_PREFIX ENVIRONMENT SNOWFLAKE_PREFIX SNOWFLAKE_TERRAFORM_USER \
  SNOWFLAKE_RUNTIME_USER SNOWFLAKE_RUNTIME_ROLE GITHUB_PARAMETER SNOWFLAKE_PARAMETER \
  AIRFLOW_ECR_REPOSITORY SILVER_ECR_REPOSITORY EC2_KEY_NAME SSH_PRIVATE_KEY \
  REPOSITORIES_JSON OPERATOR_PUBLIC_IP ALERT_EMAIL SNOWFLAKE_ACCOUNT \
  > "$HOME/.config/elt-demo/environment.sh"
```

## 5. Create Snowflake users and credentials

```bash
umask 077
mkdir -p "$HOME/.snowflake" "$HOME/.github" "$HOME/.ssh"
chmod 700 "$HOME/.snowflake" "$HOME/.github" "$HOME/.ssh"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -out "$HOME/.snowflake/elt_terraform.p8"
export TF_RSA_PUBLIC_KEY="$(openssl pkey -in "$HOME/.snowflake/elt_terraform.p8" -pubout \
  | sed '1d;$d' | tr -d '\n')"

python3 "$PROJECT_ROOT/tools/render_usage_template.py" snowflake-bootstrap.sql > /tmp/elt-bootstrap.sql
cat /tmp/elt-bootstrap.sql
```

Open app.snowflake.com → create a SQL worksheet → paste /tmp/elt-bootstrap.sql → run each statement one by one as ACCOUNTADMIN.
When a token_secret is returned, copy it and complete step 6.
For an RSA key update, use the new elt_terraform.p8 file, repeat public-key export and SQL generation, then run the generated SQL.
When both tokens are already stored in SSM, skip step 6.

## 6. Save tokens in SSM

```bash
read -r -s -p 'Snowflake runtime token_secret: ' SNOWFLAKE_RUNTIME_TOKEN
```

```bash
printf '%s' "$SNOWFLAKE_RUNTIME_TOKEN" > "$HOME/.snowflake/elt_runtime_token"
unset SNOWFLAKE_RUNTIME_TOKEN
```

Create a GitHub token at github.com/settings/personal-access-tokens with Pull requests read access to the selected repositories.
Enter that token and the Snowflake token_secret.

```bash
read -r -s -p 'GitHub pipeline token: ' GITHUB_TOKEN
```

```bash
printf '%s' "$GITHUB_TOKEN" > "$HOME/.github/token"
unset GITHUB_TOKEN

chmod 600 "$HOME/.github/token" "$HOME/.snowflake/elt_runtime_token"

aws ssm put-parameter --name "$GITHUB_PARAMETER" \
  --type SecureString --value "file://$HOME/.github/token" --overwrite
aws ssm put-parameter --name "$SNOWFLAKE_PARAMETER" \
  --type SecureString --value "file://$HOME/.snowflake/elt_runtime_token" --overwrite
```

## 7. Build and publish images

```bash
cd "$PROJECT_ROOT"
bash tools/prepare_aws_resources.sh
AIRFLOW_IMAGE="$(build/airflow/build_and_push.sh \
  "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$AIRFLOW_ECR_REPOSITORY")"
SILVER_IMAGE="$(build/spark/build_and_push.sh \
  "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$SILVER_ECR_REPOSITORY")"
export AIRFLOW_IMAGE SILVER_IMAGE
printf 'Airflow: %s\nSilver: %s\n' "$AIRFLOW_IMAGE" "$SILVER_IMAGE"
```

## 8. Deploy infrastructure

```bash
cd "$PROJECT_ROOT/terraform"
if [ ! -s terraform.tfvars ]; then
  python3 "$PROJECT_ROOT/tools/render_usage_template.py" terraform.tfvars > terraform.tfvars
fi
cp "$PROJECT_ROOT/tools/usage_templates/snowflake-import.tf" snowflake-import.tf
chmod 600 terraform.tfvars
```

Refresh the allowed IP from the network/VPN used to access Airflow.

```bash
OPERATOR_PUBLIC_IP="$(curl -4 -fsS --connect-timeout 10 --max-time 30 https://checkip.amazonaws.com)" &&
  python3 "$PROJECT_ROOT/tools/update_airflow_ip.py" terraform.tfvars "$OPERATOR_PUBLIC_IP"
```

```bash
unset SNOWFLAKE_PASSWORD SNOWFLAKE_TOKEN
export SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
export SNOWFLAKE_PRIVATE_KEY="$(cat "$HOME/.snowflake/elt_terraform.p8")"
export TF_CLI_CONFIG_FILE="$PROJECT_ROOT/terraform/provider-mirror.tfrc"
terraform init
terraform fmt terraform.tfvars snowflake-import.tf
terraform plan -out=deploy.tfplan
terraform apply deploy.tfplan
terraform output
```

Repeat step 8 after changing your network or reconnecting your VPN.
Confirm the SNS subscription in the AWS email.

## 9. Run Airflow

```bash
export AIRFLOW_IP="$(terraform output -raw airflow_public_ip)"
export AIRFLOW_URL="$(terraform output -raw airflow_url)"
export BRONZE_BUCKET="$(terraform output -raw bronze_bucket)"
export ARTIFACTS_BUCKET="$(terraform output -raw artifacts_bucket)"
curl -fsS "$AIRFLOW_URL/api/v2/monitor/health" | jq .
ssh -i "$SSH_PRIVATE_KEY" "ec2-user@$AIRFLOW_IP" \
  'sudo docker exec airflow airflow dags list-import-errors --output json'
```

Repeat the health request until all four components report healthy and list-import-errors returns `[]`.

```bash
ssh -i "$SSH_PRIVATE_KEY" "ec2-user@$AIRFLOW_IP" \
  'sudo docker exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated'
```

Open AIRFLOW_URL from an IP allowed by airflow_allowed_cidr and sign in as admin with the password from the JSON.
Enable elt_repo_contribution → Trigger DAG → set mode to backfill → Run.
Wait for poller, spark_silver, and dbt_gold to turn green.
Open poller → XCom → return_value → copy run_id.

## 10. Check S3 and Snowflake

```bash
read -r -p 'Pipeline run_id from Poller XCom: ' RUN_ID
```

```bash
aws s3 ls "s3://$BRONZE_BUCKET/" --recursive --summarize
aws s3 ls "s3://$ARTIFACTS_BUCKET/pipeline-artifacts/$RUN_ID/stage1/output.parquet"
aws s3 ls "s3://$ARTIFACTS_BUCKET/pipeline-artifacts/$RUN_ID/silver/output.parquet"
aws s3 ls "s3://$ARTIFACTS_BUCKET/poller-state/" --recursive
```

Check that Bronze objects, both Parquet manifests, and repository state exist.

```bash
python3 "$PROJECT_ROOT/tools/render_usage_template.py" snowflake-check.sql > /tmp/elt-check.sql
cat /tmp/elt-check.sql
```

Run /tmp/elt-check.sql in a Snowsight Workspaces SQL file as ACCOUNTADMIN.
Check PULL ≥ 230, FILE ≥ 230, and nonzero row counts for REVIEW and all three Gold tables.
Check that PR 210 is returned for salute-developers/smart_app_framework.

## 11. Deploy the four charts

Edit the parameters CTE and chart JSON in serving/queries/*.sql if needed.

```bash
cd "$PROJECT_ROOT/terraform"
export SNOWFLAKE_USER="$SNOWFLAKE_TERRAFORM_USER"
export SNOWFLAKE_ROLE=ACCOUNTADMIN
export SNOWFLAKE_DATABASE="$(terraform output -raw snowflake_database)"
export SNOWFLAKE_WAREHOUSE="$(terraform output -raw snowflake_warehouse)"
export SNOWFLAKE_PRIVATE_KEY_FILE="$HOME/.snowflake/elt_terraform.p8"
if [ ! -x "$PROJECT_ROOT/serving/.venv/bin/python" ]; then
  python3 -m venv "$PROJECT_ROOT/serving/.venv"
fi
"$PROJECT_ROOT/serving/.venv/bin/python" -m pip install -r "$PROJECT_ROOT/serving/requirements.txt"
"$PROJECT_ROOT/serving/.venv/bin/python" "$PROJECT_ROOT/serving/deploy.py"
```

In Snowsight, open Projects → Streamlit → REPO_CONTRIBUTION in SNOWFLAKE_DATABASE.PUBLIC.
Reload an already open app tab and select Run.
Check that all four charts display data and select a directory for the author-share chart.
Repeat this step after editing SQL parameters or chart settings.

## 12. Run an increment

Trigger elt_repo_contribution with mode set to incremental.
Wait for all three tasks to turn green.

## 13. Delete the deployment

Pause the DAG and wait for active tasks and EMR jobs to finish.
Start Bash and restore the deployment inputs.

```bash
bash
```

```bash
source "$HOME/.config/elt-demo/environment.sh"
export TF_CLI_CONFIG_FILE="$PROJECT_ROOT/terraform/provider-mirror.tfrc"
unset SNOWFLAKE_PASSWORD SNOWFLAKE_TOKEN
export SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
export SNOWFLAKE_PRIVATE_KEY="$(cat "$HOME/.snowflake/elt_terraform.p8")"
cd "$PROJECT_ROOT/terraform"
export AIRFLOW_IP="$(terraform output -raw airflow_public_ip)"
export BRONZE_BUCKET="$(terraform output -raw bronze_bucket)"
export ARTIFACTS_BUCKET="$(terraform output -raw artifacts_bucket)"
```

```bash
ssh -i "$SSH_PRIVATE_KEY" "ec2-user@$AIRFLOW_IP" 'sudo docker stop airflow'
```

```bash
for ELT_BUCKET in "$BRONZE_BUCKET" "$ARTIFACTS_BUCKET"; do
  aws s3 rm "s3://$ELT_BUCKET/" --recursive
  while true; do
    ELT_VERSIONS="$(aws s3api list-object-versions --bucket "$ELT_BUCKET" \
      --max-keys 1000 --no-paginate --output json \
      | jq -c '{Objects: ([.Versions[]?, .DeleteMarkers[]?] | map({Key, VersionId})), Quiet: true}')"
    if [ "$(jq '.Objects | length' <<< "$ELT_VERSIONS")" -eq 0 ]; then
      break
    fi
    aws s3api delete-objects --bucket "$ELT_BUCKET" --delete "$ELT_VERSIONS" \
      > /tmp/elt-delete-objects.json
    jq -e '(.Errors // []) | length == 0' /tmp/elt-delete-objects.json >/dev/null || break
  done
done

terraform plan -destroy -out=destroy.tfplan
terraform apply destroy.tfplan
```

If /tmp/elt-delete-objects.json contains Errors, fix them and repeat bucket cleanup before retrying destroy.

```bash
aws ecr delete-repository --repository-name "$AIRFLOW_ECR_REPOSITORY" --force
aws ecr delete-repository --repository-name "$SILVER_ECR_REPOSITORY" --force
aws ssm delete-parameters --names "$GITHUB_PARAMETER" "$SNOWFLAKE_PARAMETER"
aws ec2 delete-key-pair --key-name "$EC2_KEY_NAME"
```

```bash
python3 "$PROJECT_ROOT/tools/render_usage_template.py" snowflake-cleanup.sql > /tmp/elt-cleanup.sql
cat /tmp/elt-cleanup.sql
```

Run /tmp/elt-cleanup.sql in Snowsight as your original administrator.
Delete the demo GitHub token at github.com/settings/personal-access-tokens.
