{% materialization monday_incremental_lastrun_auto, adapter='snowflake' -%}

  {% set original_query_tag = set_query_tag() %}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}



  {#-- Set Config --#}
  {%- set delete_date_column = config.require('delete_date_column') -%}
  {%- set days_range = config.require('days_range') -%}
  {%- set existing_where = config.require('existing_where') -%}

  {#-- By default, insert_date_column is insert_date but it can be overwritten in the config --#}
  {%- set insert_date_column = config.get('insert_date_column', default='insert_date') -%}


  {% set materialized_sql %}

  with materialization_cte as (

      -- initial sql
      {{ sql }}

      -- incremental logic
      {% if is_incremental() %}
      -- this filter will only be applied on an incremental run

        {% if not existing_where %}
          where 1 = 1
        {% endif %}
          
          and {{ delete_date_column }} 
              between 
                  DATEADD(
                      day, 
                      -{{ days_range }}, 
                      (select max({{ insert_date_column }}) from {{ this }})
                  ) 
                  and current_date 
      {% endif %}

  )


  select 
      *,
      current_timestamp as {{ insert_date_column }}
  from materialization_cte

  {% endset %}


  {#-- Delete some Data (only if the table has been built before) --#}
  {% if existing_relation is not none %}
      {% set delete_query %}
          delete from {{ this }} 
          where  
            {{ delete_date_column }} > DATEADD(day, -{{days_range}}, (select max({{ insert_date_column }}) from {{ this }} ))
            and {{ delete_date_column }} < current_date
      {% endset %}

      {% do run_query(delete_query) %}
  {% endif %}



  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, materialized_sql) %}

  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, materialized_sql) %}

  {% elif full_refresh_mode %}
    {% set build_sql = create_table_as(False, target_relation, materialized_sql) %}

  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, materialized_sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}
    {% set build_sql = get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {% do drop_relation_if_exists(load_relation(tmp_relation)) %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = target_relation.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
