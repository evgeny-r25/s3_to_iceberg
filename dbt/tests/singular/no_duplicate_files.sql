-- Test to ensure no duplicate files in processed_data
-- Each file_name should be unique in the mart

select file_name, count(*) as occurrence_count
from {{ ref('processed_data') }}
group by file_name
having count(*) > 1
