-- Validation staging model for CSV data
-- Performs format and content validation using DuckDB
-- Identifies files that should be moved to DLQ

{{ config(
    materialized='view',
    schema='stg',
    tags=['staging', 'validation', 'hourly']
) }}

with raw_data as (
    select * from {{ ref('stg_raw_csv_data') }}
),

validation_checks as (
    select
        file_name,
        load_timestamp,
        file_hash,
        record_count,
        is_valid_content,
        -- Validation checks
        case
            when is_valid_content = false then 'INVALID_CONTENT'
            when record_count = 0 then 'EMPTY_FILE'
            when record_count > 1000000 then 'FILE_TOO_LARGE'
            else null
        end as validation_error,

        -- Validation result
        case
            when is_valid_content = true
                and record_count > 0
                and record_count <= 1000000
            then true
            else false
        end as is_valid_file,

        -- Validation timestamp
        current_timestamp as validation_timestamp
    from raw_data
)

select
    file_name,
    load_timestamp,
    file_hash,
    record_count,
    is_valid_file,
    validation_error,
    validation_timestamp,
    case
        when is_valid_file then 'READY_FOR_PROCESSING'
        else 'FAILED_VALIDATION'
    end as processing_status
from validation_checks
