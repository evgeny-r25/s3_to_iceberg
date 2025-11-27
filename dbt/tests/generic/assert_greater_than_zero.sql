-- Custom test to assert a numeric column is greater than zero

{% test assert_greater_than_zero(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} <= 0 or {{ column_name }} is null
{% endtest %}
