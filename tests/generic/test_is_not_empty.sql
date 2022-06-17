{% test is_not_empty(model) %}

    select count(*) as n_records
    from {{ model }}
    having n_records = 0

{% endtest %}