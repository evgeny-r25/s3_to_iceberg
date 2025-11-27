-- Analysis: Daily processing statistics
-- Shows success rates and throughput metrics

with processed as (
    select
        load_date,
        count(*) as total_files,
        sum(record_count) as total_records,
        min(load_timestamp) as first_load,
        max(load_timestamp) as last_load
    from {{ ref('processed_data') }}
    group by load_date
),

failed as (
    select
        cast(failed_timestamp as date) as failure_date,
        count(*) as failed_files,
        count(distinct failure_reason) as distinct_failures
    from {{ ref('failed_files') }}
    group by failure_date
),

combined as (
    select
        coalesce(p.load_date, f.failure_date) as date,
        coalesce(p.total_files, 0) as successful_files,
        coalesce(p.total_records, 0) as total_records,
        coalesce(f.failed_files, 0) as failed_files,
        coalesce(f.distinct_failures, 0) as distinct_failure_types,
        case
            when (coalesce(p.total_files, 0) + coalesce(f.failed_files, 0)) > 0
            then round(
                100.0 * coalesce(p.total_files, 0) /
                (coalesce(p.total_files, 0) + coalesce(f.failed_files, 0)),
                2
            )
            else 0
        end as success_rate
    from processed p
    full outer join failed f
        on p.load_date = f.failure_date
)

select
    date,
    successful_files,
    failed_files,
    (successful_files + failed_files) as total_files,
    total_records,
    distinct_failure_types,
    success_rate,
    case
        when success_rate >= 99 then 'EXCELLENT'
        when success_rate >= 95 then 'GOOD'
        when success_rate >= 90 then 'ACCEPTABLE'
        else 'POOR'
    end as quality_rating
from combined
order by date desc
