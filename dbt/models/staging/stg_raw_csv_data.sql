-- Staging model for raw CSV data from S3
-- Performs initial cleaning and structuring of data
-- Validates data format before moving to marts
-- Includes detailed logging of source read operations

{{ config(
    materialized='view',
    schema='stg',
    tags=['staging', 'csv', 'hourly'],
    post_hook=[
        "{% if execute %}
            {% set file_count = run_query('SELECT COUNT(DISTINCT file_name) as cnt FROM ' ~ this.schema ~ '.' ~ this.name).columns[0].values()[0] %}
            {% set total_records = run_query('SELECT SUM(record_count) as cnt FROM ' ~ this.schema ~ '.' ~ this.name).columns[0].values()[0] %}
            {% set total_size = run_query('SELECT SUM(content_length) as cnt FROM ' ~ this.schema ~ '.' ~ this.name).columns[0].values()[0] %}
            {% do log('
===============================================================================
  SOURCE READ STATISTICS [stg_raw_csv_data]
===============================================================================
  Files Read:           ' ~ file_count | default(0) ~ '
  Total Records:        ' ~ total_records | default(0) ~ '
  Total Size:           ' ~ (total_size | default(0) / (1024 * 1024)) | round(2) ~ ' MB
  Source Bucket:        s3://dev-raw-data-testing/dev-source-raw/
  Data Retention:       Last 7 days
  Status:               READ COMPLETE [OK]
===============================================================================
            ', info=true) %}
        {% endif %}"
    ]
) }}

with raw_data as (
    select
        file_name,
        load_timestamp,
        file_hash,
        record_count,
        raw_content,
        -- Add processing timestamp
        current_timestamp as processed_at,
        -- Calculate content length
        length(raw_content) as content_length,
        -- Mark for validation
        case
            when raw_content is null then false
            when length(raw_content) = 0 then false
            else true
        end as is_valid_content
    from {{ source('s3_raw', 'csv_data') }}
    where load_timestamp >= current_date - interval '7 days'  -- Keep 7 days of data
)

select
    file_name,
    load_timestamp,
    file_hash,
    record_count,
    raw_content,
    content_length,
    processed_at,
    is_valid_content
from raw_data
