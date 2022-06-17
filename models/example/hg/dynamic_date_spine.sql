{% set min_metric_date %}
select min(date) from {{ ref('date_table') }}
{% endset %}

{% set min_metric_date_result = run_query(min_metric_date) %}

{% if execute %}
{# Return the first column #}
{% set results_list = min_metric_date_result.columns[0][0] %}
{% else %}
{% set results_list = '2000-01-01' %}
{% endif %}

with test as (
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2022-01-01' as date)",
    end_date="'" ~ results_list ~ "'"
   )
}}
)

select 
    *, 
    '{{ results_list }}' as aaa
from test