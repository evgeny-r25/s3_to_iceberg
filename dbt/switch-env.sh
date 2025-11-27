#!/bin/bash

# dbt Environment Switcher
# Usage: source switch-env.sh [environment]
# Example: source switch-env.sh production

set_env_vars() {
    local ENV=$1

    case $ENV in
        development|dev)
            echo "Switching to Development environment..."
            export DBT_ENV=development
            export S3_SOURCE_BUCKET=s3://dev-raw-data/
            export DEST_S3_BUCKET=s3://dev-iceberg-warehouse/
            export DLQ_S3_BUCKET=s3://dev-iceberg-dlq/
            export DBT_THREADS=2
            echo "✓ Development environment loaded"
            ;;
        staging|stage)
            echo "Switching to Staging environment..."
            export DBT_ENV=staging
            export S3_SOURCE_BUCKET=s3://staging-raw-data/
            export DEST_S3_BUCKET=s3://staging-iceberg-warehouse/
            export DLQ_S3_BUCKET=s3://staging-iceberg-dlq/
            export DBT_THREADS=3
            echo "✓ Staging environment loaded"
            ;;
        production|prod)
            echo "Switching to Production environment..."
            export DBT_ENV=production
            export S3_SOURCE_BUCKET=s3://prod-raw-data/
            export DEST_S3_BUCKET=s3://prod-iceberg-warehouse/
            export DLQ_S3_BUCKET=s3://prod-iceberg-dlq/
            export DBT_THREADS=4
            echo "✓ Production environment loaded"
            ;;
        *)
            echo "Usage: source switch-env.sh [environment]"
            echo "Available environments:"
            echo "  - development (dev)"
            echo "  - staging (stage)"
            echo "  - production (prod)"
            return 1
            ;;
    esac

    echo "Environment variables set:"
    echo "  DBT_ENV=$DBT_ENV"
    echo "  S3_SOURCE_BUCKET=$S3_SOURCE_BUCKET"
    echo "  DEST_S3_BUCKET=$DEST_S3_BUCKET"
    echo "  DLQ_S3_BUCKET=$DLQ_S3_BUCKET"
    echo "  DBT_THREADS=$DBT_THREADS"
}

# Call the function with the provided argument
set_env_vars "$@"
