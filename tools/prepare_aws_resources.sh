#!/usr/bin/env bash
set -euo pipefail
umask 077

for repository_name in "$AIRFLOW_ECR_REPOSITORY" "$SILVER_ECR_REPOSITORY"; do
  repository_count="$(aws ecr describe-repositories \
    --query "length(repositories[?repositoryName=='$repository_name'])" --output json)"
  if [ "$repository_count" -eq 0 ]; then
    aws ecr create-repository --repository-name "$repository_name" >/dev/null
  fi
done

mkdir -p "$(dirname "$SSH_PRIVATE_KEY")"
if [ ! -s "$SSH_PRIVATE_KEY" ]; then
  ssh_key_dir="$(mktemp -d)"
  ssh-keygen -q -t rsa -b 2048 -m PEM -N '' -f "$ssh_key_dir/key"
  mv "$ssh_key_dir/key" "$SSH_PRIVATE_KEY"
  rm -rf "$ssh_key_dir"
fi
chmod 600 "$SSH_PRIVATE_KEY"
ssh-keygen -y -f "$SSH_PRIVATE_KEY" > "$SSH_PRIVATE_KEY.pub"

key_pair_count="$(aws ec2 describe-key-pairs \
  --filters "Name=key-name,Values=$EC2_KEY_NAME" --query 'length(KeyPairs)' --output json)"
if [ "$key_pair_count" -eq 0 ]; then
  aws ec2 import-key-pair --key-name "$EC2_KEY_NAME" \
    --public-key-material "fileb://$SSH_PRIVATE_KEY.pub" >/dev/null
fi
