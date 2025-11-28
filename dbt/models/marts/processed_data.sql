-- Mart model for processed and validated data
-- Loads validated CSV data into Iceberg tables in S3
-- Only includes files that passed validation
-- Includes detailed logging of S3 write operations

{{ config(
    materialized='table',
    schema='marts',
    tags=['mart', 'iceberg', 'validated'],
    partition_by=['load_date'],
    cluster_by=['file_name'],
    post_hook=[
        "{% if execute %}
            {% set row_count = run_query('SELECT COUNT(*) as cnt FROM ' ~ this.schema ~ '.' ~ this.name).columns[0].values()[0] %}
            {% set total_records = run_query('SELECT SUM(record_count) as cnt FROM ' ~ this.schema ~ '.' ~ this.name).columns[0].values()[0] %}
            {% do log('
===============================================================================
  S3 ICEBERG WRITE STATISTICS [processed_data]
===============================================================================
  Table:                 ' ~ this.schema ~ '.' ~ this.name ~ '
  Rows Written:          ' ~ row_count | default(0) ~ '
  Total Records:         ' ~ total_records | default(0) ~ '
  Destination Bucket:    s3://dev-raw-data-testing/dev-iceberg-warehouse/
  Format:                Apache Iceberg (Parquet-based)
  Partitioning:          load_date
  Clustering:            file_name
  Status:                WRITE COMPLETE [OK]

  Iceberg Configuration:
    Table Location:      s3://dev-raw-data-testing/dev-iceberg-warehouse/
    Compression:         SNAPPY
    Data Format:         PARQUET
    Version:             2 (Iceberg v2)
===============================================================================
            ', info=true) %}
        {% endif %}"
    ]
) }}

with validated_data as (
    select
        file_name,
        load_timestamp,
        file_hash,
        record_count,
        processing_status
    from {{ ref('stg_csv_validation') }}
    where is_valid_file = true
        and processing_status = 'READY_FOR_PROCESSING'
),

processed as (
    select
        file_name,
        load_timestamp,
        file_hash,
        record_count,
        processing_status,
        -- Add processed date for partitioning
        cast(load_timestamp as date) as load_date,
        -- Add metadata
        current_timestamp as inserted_at,
        current_timestamp as updated_at
    from validated_data
)

select
    file_name,
    load_timestamp,
    load_date,
    file_hash,
    record_count,
    processing_status,
    inserted_at,
    updated_at
from processed
