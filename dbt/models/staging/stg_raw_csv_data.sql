-- Staging model for raw CSV data from S3
-- Performs initial cleaning and structuring of data
-- Validates data format before moving to marts

{{ config(
    materialized='view',
    schema='stg',
    tags=['staging', 'csv', 'hourly']
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
