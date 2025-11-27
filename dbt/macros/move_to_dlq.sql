-- Macro to handle moving failed files to DLQ (Dead Letter Queue) bucket
-- Generates S3 move commands and tracks failures

{% macro move_to_dlq(file_name, failure_reason, file_location) %}
    {%- set dlq_bucket = var('dlq_s3_bucket') -%}

    {%- if execute -%}
        {%- set dlq_path = dlq_bucket ~ '/' ~ modules.datetime.datetime.now().strftime('%Y/%m/%d/%H') ~ '/' ~ file_name -%}

        -- Log the move operation
        {% do log('Moving file to DLQ: ' ~ file_name, info=true) %}
        {% do log('Failure reason: ' ~ failure_reason, info=false) %}
        {% do log('DLQ destination: ' ~ dlq_path, info=false) %}

        -- Return the DLQ path and metadata
        {{ return({
            'dlq_path': dlq_path,
            'file_name': file_name,
            'failure_reason': failure_reason,
            'original_location': file_location,
            'timestamp': modules.datetime.datetime.now().isoformat()
        }) }}
    {%- else -%}
        {{ return({}) }}
    {%- endif -%}
{% endmacro %}
