#!/usr/bin/env bash
#
# bucket_setup.sh — provision the S3 data lake bucket and its three zones.
#
# The data lake uses ONE bucket with prefix-based zones (raw/clean/analytics)
# rather than three separate buckets. One bucket is cheaper to manage, keeps
# IAM/lifecycle policy in a single place, and the zone boundary is enforced by
# prefix conventions — which is all Glue/Athena need.
#
# Idempotent-ish: safe to re-run. Bucket creation will report an error if the
# bucket already exists (which is fine — it means setup already happened).
#
# Cost: an empty bucket and zero-byte prefix markers cost effectively $0.
set -euo pipefail

REGION="eu-west-1"
BUCKET="marco-data-darya"

echo ">> Creating bucket s3://${BUCKET} in ${REGION}"
# Regions other than us-east-1 REQUIRE an explicit LocationConstraint.
aws s3api create-bucket \
  --bucket "${BUCKET}" \
  --region "${REGION}" \
  --create-bucket-configuration LocationConstraint="${REGION}" \
  || echo "   (bucket already exists — skipping)"

echo ">> Blocking all public access (defense in depth)"
aws s3api put-public-access-block \
  --bucket "${BUCKET}" \
  --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicAccess=true

echo ">> Enabling default server-side encryption (SSE-S3)"
aws s3api put-bucket-encryption \
  --bucket "${BUCKET}" \
  --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

echo ">> Creating zone markers: raw/ clean/ analytics/"
# S3 has no real folders; these zero-byte keys just make the layout visible
# in the console. The crawler and ETL jobs create the real keys under them.
for zone in raw clean analytics; do
  aws s3api put-object --bucket "${BUCKET}" --key "${zone}/" >/dev/null
  echo "   s3://${BUCKET}/${zone}/"
done

echo ">> Done. Data lake bucket is ready."
