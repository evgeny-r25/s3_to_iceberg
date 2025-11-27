# S3 to Iceberg dbt Project

This dbt project manages the transformation and validation of CSV data from S3 to Apache Iceberg tables. It integrates with the `gcs_to_s3_to_iceberg.py` script to provide data quality, validation, and error tracking.

## Project Overview

- **Purpose**: Transform and validate CSV files from S3 into Iceberg tables
- **Schedule**: Hourly ingestion of CSV files
- **Environments**: Production, Development, Staging
- **Data Warehouse**: DuckDB (local) / AWS Athena or Spark (production)

## Directory Structure

```
dbt/
├── models/
│   ├── sources/         # Source definitions for S3 raw data
│   ├── staging/         # Staging models for data validation
│   └── marts/           # Final Iceberg tables
│       └── dlq/         # Dead Letter Queue for failed files
├── macros/              # Custom macros for validation and S3 operations
├── tests/               # Data quality tests
│   ├── generic/         # Reusable test templates
│   └── singular/        # One-off tests
├── analyses/            # Ad-hoc analysis queries
├── seeds/               # Static data loading
├── data/                # Data files
├── dbt_project.yml      # dbt configuration
├── profiles.yml         # Database connection profiles
└── README.md
```

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
dbt deps
```

### 2. Configure Profiles

Copy `dbt/profiles.yml` to `~/.dbt/profiles.yml` and update with your credentials:

```bash
cp dbt/profiles.yml ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your S3 and database credentials
```

### 3. Set Environment Variables

```bash
# For development
export DBT_ENV=development
export S3_SOURCE_BUCKET=s3://dev-raw-data/
export DEST_S3_BUCKET=s3://dev-iceberg-warehouse/
export DLQ_S3_BUCKET=s3://dev-iceberg-dlq/

# For staging
export DBT_ENV=staging
export S3_SOURCE_BUCKET=s3://staging-raw-data/
export DEST_S3_BUCKET=s3://staging-iceberg-warehouse/
export DLQ_S3_BUCKET=s3://staging-iceberg-dlq/

# For production
export DBT_ENV=production
export S3_SOURCE_BUCKET=s3://prod-raw-data/
export DEST_S3_BUCKET=s3://prod-iceberg-warehouse/
export DLQ_S3_BUCKET=s3://prod-iceberg-dlq/
```

## Running dbt

### Run all models

```bash
# Development
dbt run --profiles-dir ~/.dbt --target development

# Staging
dbt run --profiles-dir ~/.dbt --target staging

# Production
dbt run --profiles-dir ~/.dbt --target production
```

### Run specific models

```bash
dbt run --select processed_data --target development
dbt run --select failed_files --target development
```

### Run tests

```bash
dbt test --profiles-dir ~/.dbt --target development
```

### Generate documentation

```bash
dbt docs generate
dbt docs serve
```

## Data Models

### Staging Layer (stg_)

- **stg_raw_csv_data**: Raw CSV files from S3
- **stg_csv_validation**: Validation checks and status

### Mart Layer (marts/)

- **processed_data**: Validated and processed CSV data in Iceberg tables
- **failed_files** (dlq/): Files that failed validation or processing

## Data Flow

```
S3 Source Bucket (CSV files)
    ↓
gcs_to_s3_to_iceberg.py (Hourly)
    ↓
Raw Data (stg_raw_csv_data)
    ↓
Validation (stg_csv_validation)
    ├─→ Valid Files → processed_data (Iceberg) ✓
    └─→ Failed Files → failed_files (DLQ) ✗
```

## Validation Rules

Files are validated based on:

1. **Content Validation**
   - Non-empty file content
   - Valid CSV format (contains commas)

2. **Size Validation**
   - Record count between 0 and 1,000,000
   - Files exceeding size generate warnings

3. **Schema Validation**
   - Required columns present
   - Data types match expected schema

## Dead Letter Queue (DLQ)

Failed files are tracked in the `failed_files` table with:
- Failure reason
- Error details
- File location in DLQ bucket
- Timestamp of failure

### Resolving DLQ Issues

1. Check the `failed_files` table for error details
2. Investigate the original file in the source bucket
3. Fix the file format or data issues
4. Reprocess the file

```sql
select * from marts.dlq.failed_files
where is_resolved = false
order by failed_timestamp desc;
```

## Integration with gcs_to_s3_to_iceberg.py

The dbt project works alongside the Python script:

1. **Python Script**: Reads CSV from S3, validates, writes to Iceberg
2. **dbt Project**: Models and tests the validated data, tracks failures
3. **DLQ Handling**: Both tools use the same DLQ bucket for broken files

## Testing Data Quality

Run tests to validate:

- No duplicate files in processed_data
- All required columns populated
- Record counts are positive
- Processing status is valid

```bash
dbt test --select processed_data
```

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DBT_ENV` | Current environment | development, staging, production |
| `S3_SOURCE_BUCKET` | Source bucket for raw CSV files | s3://dev-raw-data/ |
| `DEST_S3_BUCKET` | Destination bucket for Iceberg tables | s3://dev-iceberg-warehouse/ |
| `DLQ_S3_BUCKET` | DLQ bucket for failed files | s3://dev-iceberg-dlq/ |

## Troubleshooting

### Connection Issues

```bash
dbt debug --profiles-dir ~/.dbt --target development
```

### Model Failures

Check logs:
```bash
cat logs/dbt.log
```

### Test Failures

```bash
dbt test --fail-fast --target development
```

## Documentation

Generate and view dbt documentation:

```bash
dbt docs generate
dbt docs serve
```

Access documentation at: http://localhost:8000

## Contributing

1. Create a new branch for changes
2. Add models to appropriate layer (staging/marts)
3. Add tests for new models
4. Run `dbt test` to validate
5. Update documentation
6. Submit pull request

## Contact & Support

For issues or questions about the dbt project:
- Check existing dbt documentation: https://docs.getdbt.com
- Review model dependencies: `dbt run --dry-run`
- Generate dependency graph: `dbt docs generate`

## License

Same as parent project
