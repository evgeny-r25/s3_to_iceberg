-- Macro to generate manifest of processed and failed files
-- Used for integration with gcs_to_s3_to_iceberg.py script

{% macro generate_s3_manifest() %}
    {%- if execute -%}
        {%- set manifest = {
            'processed_files': [],
            'failed_files': [],
            'timestamp': modules.datetime.datetime.now().isoformat(),
            'environment': var('environment')
        } -%}

        {% do log('=== S3 to Iceberg Pipeline Manifest ===', info=true) %}
        {% do log('Environment: ' ~ var('environment'), info=true) %}
        {% do log('Generated at: ' ~ manifest.timestamp, info=true) %}
        {% do log('Source bucket: ' ~ var('s3_source_bucket'), info=true) %}
        {% do log('Destination bucket: ' ~ var('destination_s3_bucket_iceberg'), info=true) %}
        {% do log('DLQ bucket: ' ~ var('dlq_s3_bucket'), info=true) %}

        {{ return(manifest) }}
    {%- else -%}
        {{ return({}) }}
    {%- endif -%}
{% endmacro %}
