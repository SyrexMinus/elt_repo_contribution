#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <ECR repository URI>" >&2
  exit 2
fi

repository_uri="${1%/}"
if [[ ! "$repository_uri" =~ ^[0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/[a-z0-9._/-]+$ ]]; then
  echo "Expected a private ECR repository URI" >&2
  exit 2
fi

registry="${repository_uri%%/*}"
region="$(printf '%s' "$registry" | cut -d. -f4)"
repository_name="${repository_uri#*/}"
image_tag="${IMAGE_TAG:-$(git rev-parse --short=12 HEAD)}"
tagged_image="${repository_uri}:${image_tag}"

aws ecr get-login-password --region "$region" \
  | docker login --username AWS --password-stdin "$registry" >&2
docker build --platform linux/amd64 --pull --file build/airflow/Dockerfile --tag "$tagged_image" . >&2
docker push "$tagged_image" >&2

digest="$(aws ecr describe-images \
  --region "$region" \
  --repository-name "$repository_name" \
  --image-ids "imageTag=$image_tag" \
  --query 'imageDetails[0].imageDigest' \
  --output text)"

printf '%s@%s\n' "$repository_uri" "$digest"
