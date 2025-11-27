-- Mart model for processed and validated data
-- Loads validated CSV data into Iceberg tables
-- Only includes files that passed validation

{{ config(
    materialized='table',
    schema='marts',
    tags=['mart', 'iceberg', 'validated'],
    partition_by=['load_date'],
    cluster_by=['file_name']
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
