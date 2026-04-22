#!/usr/bin/env bash
# Deploy the SAP MDM migration platform to an AWS environment.
# Usage: bash scripts/deploy.sh <env>
#   env = dev | staging | prod

set -euo pipefail

ENV="${1:-}"
if [[ -z "$ENV" ]]; then
    echo "Usage: $0 <dev|staging|prod>"
    exit 1
fi

if [[ ! "$ENV" =~ ^(dev|staging|prod)$ ]]; then
    echo "ERROR: env must be dev, staging, or prod"
    exit 1
fi

if [[ "$ENV" == "prod" ]]; then
    read -p "⚠️  Deploying to PRODUCTION. Continue? (yes/NO): " CONFIRM
    if [[ "$CONFIRM" != "yes" ]]; then
        echo "Aborted."
        exit 1
    fi
fi

echo "=== 1. Building Python package ==="
make build-package

echo "=== 2. Uploading Glue scripts and package to S3 ==="
SCRIPTS_BUCKET="sap-mdm-${ENV}-glue-scripts-$(cd infra/terraform && terraform output -raw scripts_bucket_suffix 2>/dev/null || echo 'unknown')"
aws s3 cp dist/migration.zip "s3://${SCRIPTS_BUCKET}/packages/migration.zip"
aws s3 cp src/migration/orchestration/glue_extract_job.py "s3://${SCRIPTS_BUCKET}/jobs/"
aws s3 cp src/migration/orchestration/glue_load_job.py "s3://${SCRIPTS_BUCKET}/jobs/"

echo "=== 3. Emitting state machine definitions ==="
sap-migrate emit-sfn --output-dir src/migration/orchestration/

echo "=== 4. Running Terraform ==="
cd infra/terraform
terraform init -backend-config=envs/${ENV}.backend.hcl
terraform plan -var-file=envs/${ENV}.tfvars -out=${ENV}.tfplan

read -p "Review plan above. Apply? (yes/NO): " CONFIRM
if [[ "$CONFIRM" == "yes" ]]; then
    terraform apply ${ENV}.tfplan
    rm ${ENV}.tfplan
    echo "=== Deployment complete ==="
else
    echo "Aborted by user."
    rm ${ENV}.tfplan
    exit 1
fi

echo "=== 5. Applying target DDL ==="
cd ../..
sap-migrate generate-ddl --output-dir ./sql/target
echo "NOTE: DDL files generated in sql/target/ — apply to Postgres + Redshift via your preferred tool"
echo "      (Liquibase, sqitch, Flyway, or manually via psql / Redshift query editor)"
