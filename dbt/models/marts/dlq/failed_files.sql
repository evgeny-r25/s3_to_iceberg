-- DLQ (Dead Letter Queue) model for failed files
-- Tracks files that failed validation or processing
-- These should be investigated and moved to the DLQ bucket

{{ config(
    materialized='table',
    schema='dlq',
    tags=['dlq', 'error-tracking', 'iceberg'],
    partition_by=['failure_date']
) }}

with failed_data as (
    select
        file_name,
        load_timestamp,
        file_hash,
        record_count,
        validation_error,
        processing_status,
        validation_timestamp
    from {{ ref('stg_csv_validation') }}
    where is_valid_file = false
        or processing_status = 'FAILED_VALIDATION'
),

enriched_failures as (
    select
        file_name,
        load_timestamp,
        file_hash,
        record_count,
        validation_error as failure_reason,
        processing_status,
        validation_timestamp as failed_timestamp,
        cast(validation_timestamp as date) as failure_date,
        -- Generate S3 location for failed file
        concat(
            '{{ var("dlq_s3_bucket") }}',
            '/',
            format_timestamp('%Y/%m/%d', validation_timestamp),
            '/',
            file_name
        ) as file_location_dlq,
        -- Add tracking info
        current_timestamp as dlq_inserted_at,
        false as is_resolved
    from failed_data
)

select
    file_name,
    load_timestamp,
    failure_date,
    file_hash,
    record_count,
    failure_reason,
    processing_status,
    failed_timestamp,
    file_location_dlq,
    dlq_inserted_at,
    is_resolved
from enriched_failures
