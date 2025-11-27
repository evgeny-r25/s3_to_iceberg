-- Macro for validating CSV file format and content
-- Returns validation result with error details

{% macro validate_csv_file(file_content, file_name, max_size=1000000) %}
    {%- if execute -%}
        {%- set validation_result = {
            'is_valid': true,
            'errors': [],
            'warnings': []
        } -%}

        {%- if file_content is none or file_content == '' -%}
            {%- set _ = validation_result.update({'is_valid': false}) -%}
            {%- set _ = validation_result['errors'].append('File content is empty') -%}
        {%- endif -%}

        {%- set content_length = file_content | length -%}
        {%- if content_length > max_size -%}
            {%- set _ = validation_result['warnings'].append('File size exceeds ' ~ max_size ~ ' bytes') -%}
        {%- endif -%}

        {%- if ',' not in file_content -%}
            {%- set _ = validation_result.update({'is_valid': false}) -%}
            {%- set _ = validation_result['errors'].append('File does not appear to be valid CSV format (no commas found)') -%}
        {%- endif -%}

        {%- if file_name is none or file_name == '' -%}
            {%- set _ = validation_result.update({'is_valid': false}) -%}
            {%- set _ = validation_result['errors'].append('File name is missing') -%}
        {%- endif -%}

        {{ return(validation_result) }}
    {%- else -%}
        {{ return({}) }}
    {%- endif -%}
{% endmacro %}
