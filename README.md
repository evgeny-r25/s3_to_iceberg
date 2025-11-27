# GCS to S3 to Iceberg ETL Pipeline (DuckDB Edition)

A high-performance ETL pipeline that transfers data from Google Cloud Storage to Amazon S3 and processes it using DuckDB, outputting partitioned Parquet files ready for Iceberg consumption.

## Features

- **DuckDB-powered**: Replaces PySpark with DuckDB for faster local processing
- **Modern Python**: Uses Python 3.11+ features with full type hints
- **Data Validation**: Pandera schemas for data quality checks
- **Beautiful Logging**: Loguru for structured, colorful logging
- **CLI Interface**: Typer-based command-line interface
- **Parallel Processing**: Concurrent file transfers with configurable workers
- **Partitioned Output**: Hive-style partitioned Parquet files

## Installation

```bash
# Using pip
pip install -r requirements.txt

# Or using pip with pyproject.toml
pip install -e .

# Or using uv (recommended)
uv pip install -e .
```

## Quick Start

```bash
# Process all data for a specific date
python gcs_to_s3_to_iceberg.py run --date 2024-01-15

# Process a specific hour
python gcs_to_s3_to_iceberg.py run --date 2024-01-15 --hour 1400

# With verbose logging
python gcs_to_s3_to_iceberg.py run --date 2024-01-15 -v

# Dry run (simulate without changes)
python gcs_to_s3_to_iceberg.py run --date 2024-01-15 --dry-run

# Process specific prefixes only
python gcs_to_s3_to_iceberg.py run --date 2024-01-15 -p "ko69zatkf75s,17ohic5adt6o"
```

## CLI Commands

### `run` - Execute ETL Pipeline

```bash
python gcs_to_s3_to_iceberg.py run [OPTIONS]

Options:
  -d, --date TEXT           Date to process (YYYY-MM-DD format) [required]
  -h, --hour TEXT           Hour to process (HHMM format)
  -p, --prefixes TEXT       Comma-separated list of prefixes
  --gcs-bucket TEXT         GCS source bucket name
  --s3-bucket TEXT          S3 destination bucket name
  -w, --max-workers INT     Maximum parallel workers [default: 32]
  --gcs-credentials TEXT    Path to GCS service account JSON
  -v, --verbose             Enable verbose logging
  --log-file PATH           Path to log file
  --dry-run                 Simulate without making changes
```

### `list-prefixes` - Show Configured Prefixes

```bash
python gcs_to_s3_to_iceberg.py list-prefixes
```

### `list-events` - Show Event Type Configurations

```bash
python gcs_to_s3_to_iceberg.py list-events
```

### `validate-config` - Check Configuration

```bash
python gcs_to_s3_to_iceberg.py validate-config
```

## Configuration

### GCS Credentials

Place your GCS service account JSON file at `gcs_key.json` or specify the path:

```bash
python gcs_to_s3_to_iceberg.py run --date 2024-01-15 --gcs-credentials /path/to/credentials.json
```

### AWS Credentials

The pipeline uses the AWS credential chain. Configure using:

- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM role (when running on AWS infrastructure)

### Environment Variables

```bash
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcs_key.json
```

## Pipeline Phases

1. **Phase 1 - GCS to S3 Copy**: Transfers compressed CSV files from GCS to S3
2. **Phase 2 - Path Resolution**: Validates and resolves S3 file paths
3. **Phase 3 - Data Loading**: Loads CSV data into DuckDB
4. **Phase 4 - Data Processing**: Transforms, filters, and enriches data
5. **Phase 5 - Partitioned Tables**: Writes event-specific partitioned Parquet files
6. **Phase 6 - Aggregates**: Computes and writes ad impression aggregates

## Output Structure

```
s3://bucket/processed-data/celerdata/
├── prefix1_session/
│   └── date=2024-01-15/
│       └── data_0.parquet
├── prefix1_install/
│   └── date=2024-01-15/
│       └── data_0.parquet
├── prefix1_ad_impression/
│   └── date=2024-01-15/
│       └── data_0.parquet
└── prefix1_ad_impression_revenue_preaggregates/
    └── date=2024-01-15/
        └── data_0.parquet
```

## Data Validation

The pipeline uses Pandera for data validation:

```python
from gcs_to_s3_to_iceberg import RawEventSchema

# Validates that:
# - created_at is not null
# - activity_kind is one of: session, install, install_update, event
```

## Performance Tips

1. **Increase workers** for faster GCS→S3 transfers:
   ```bash
   python gcs_to_s3_to_iceberg.py run --date 2024-01-15 -w 64
   ```

2. **Process specific prefixes** for faster iteration:
   ```bash
   python gcs_to_s3_to_iceberg.py run --date 2024-01-15 -p "prefix1,prefix2"
   ```

3. **Use hour filtering** to process smaller batches:
   ```bash
   python gcs_to_s3_to_iceberg.py run --date 2024-01-15 --hour 0800
   ```

## Differences from PySpark Version

| Feature | PySpark Version | DuckDB Version |
|---------|-----------------|----------------|
| Processing Engine | Apache Spark | DuckDB |
| Cluster Required | Yes | No |
| Memory Management | Distributed | Single-node |
| Output Format | Iceberg Tables | Partitioned Parquet |
| Dependencies | Heavy (JVM) | Lightweight |
| Startup Time | Minutes | Seconds |

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run linting
ruff check .
ruff format .

# Run type checking
mypy gcs_to_s3_to_iceberg.py

# Run tests
pytest
```

## Troubleshooting

### "No files found" warnings

This is normal if certain prefixes don't have data for the specified date/hour.

### S3 permission errors

Ensure your AWS credentials have the following permissions:
- `s3:GetObject`
- `s3:PutObject`
- `s3:ListBucket`

### GCS authentication errors

Verify your service account has:
- `storage.objects.list`
- `storage.objects.get`

## License

MIT License
