{# https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/include/global_project/macros/materializations/models/incremental/is_incremental.sql #}

{% macro is_incremental() %}
    {#-- do not run introspective queries in parsing #}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {{ return(relation is not none
                  and relation.type == 'table'
                  and model.config.materialized in ['incremental','monday_incremental_lastrun_auto']
                  and not should_full_refresh()) }}
    {% endif %}
{% endmacro %}
