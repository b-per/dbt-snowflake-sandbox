{{
    config(
        materialized = 'monday_incremental_lastrun_auto',
        delete_date_column = 'date(created_at)', 
        days_range = 0, 
        insert_date_column = 'inserted_at',
        existing_where = true
    )
}}

select *
from {{ ref('my_first_dbt_model') }}
where id = 1