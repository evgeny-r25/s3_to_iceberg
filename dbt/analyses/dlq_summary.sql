-- Analysis: Summary of failed files in Dead Letter Queue
-- This analysis helps identify common failure patterns

with dlq_data as (
    select
        failure_reason,
        count(*) as failure_count,
        min(failed_timestamp) as first_failure,
        max(failed_timestamp) as last_failure,
        count(distinct file_name) as distinct_files,
        avg(record_count) as avg_record_count
    from {{ ref('failed_files') }}
    where is_resolved = false
    group by failure_reason
)

select
    failure_reason,
    failure_count,
    distinct_files,
    first_failure,
    last_failure,
    avg_record_count,
    case
        when failure_count > 10 then 'CRITICAL'
        when failure_count > 5 then 'HIGH'
        when failure_count > 1 then 'MEDIUM'
        else 'LOW'
    end as severity
from dlq_data
order by failure_count desc
