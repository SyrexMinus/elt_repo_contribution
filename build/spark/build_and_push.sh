#!/usr/bin/env bash
set -euo pipefail

repository_uri="${1%/}"
registry="${repository_uri%%/*}"
region="$(printf '%s' "$registry" | cut -d. -f4)"
repository_name="${repository_uri#*/}"
image_tag="${IMAGE_TAG:-$(git rev-parse --short=12 HEAD)}"
tagged_image="${repository_uri}:${image_tag}"

aws ecr get-login-password --region "$region" \
  | docker login --username AWS --password-stdin "$registry" >&2
docker build --platform linux/amd64 --pull --file build/spark/Dockerfile --tag "$tagged_image" . >&2
docker push "$tagged_image" >&2
digest="$(aws ecr describe-images \
  --region "$region" \
  --repository-name "$repository_name" \
  --image-ids "imageTag=$image_tag" \
  --query 'imageDetails[0].imageDigest' \
  --output text)"
printf '%s@%s\n' "$repository_uri" "$digest"
