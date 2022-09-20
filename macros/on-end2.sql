{% macro on_end2(table='load_results', schema=target.schema ~ '_audit', database = target.database, macro_transform_res_dict = None) %}

{#
to be used like
on-run-end: 
  - "{{ on_end2(table='load_results', schema='<yourschema>', database = '<yoursdb>', macro_transform_res_dict=example_transform_res_1) }}"

#}

{# we  only want to run queries when we are in "execute" mode and not "parse" mode #}
{% if execute %}

    -- we create the target schema for the metadata table if it doesn't exist
    create schema if not exists {{database}}.{{schema}};

    -- we create the target table for the metadata table if it doesn't exist
    -- the info for each model will be stored in an object, parsed from JSON
    create table if not exists {{database}}.{{schema}}.{{table}} (
        json_payload object, 
        invocation_id string,
        run_started_at datetime,
        dbt_cloud_project_id string,
        dbt_cloud_job_id string,
        dbt_cloud_run_id string,
        dbt_cloud_run_reason string
    ) ;

    -- we build all the different objects based on the value of the "results" object
    {% set all_objs = [] %}
    -- each object of results is a node (model or test) that we can parse
    {% for res in results %}
        
        {% if macro_transform_res_dict %}
        -- if we provide a "transform macro" we transform res
            {% set res_dict =res.to_dict() %}
            {% set my_obj = macro_transform_res_dict(res_dict) %}
        {% else %}
        -- otherwise we load the full res object
            {% set my_obj = res.to_dict() %}
        {% endif %}
        
        {% do all_objs.append(my_obj | tojson) %}

    {% endfor %}

    -- we insert the data into the metadata table, 1 line per model/test
    insert into {{database}}.{{schema}}.{{table}}
    {% for my_obj in all_objs %}
        select 
            parse_json($${{ tojson(my_obj) }}$$), 
            '{{ invocation_id }}',
            '{{run_started_at}}'::datetime,
            '{{ env_var("DBT_CLOUD_PROJECT_ID","not_applicable") }}',
            '{{ env_var("DBT_CLOUD_JOB_ID","not_applicable") }}',
            '{{ env_var("DBT_CLOUD_RUN_ID","not_applicable") }}',
            '{{ env_var("DBT_CLOUD_RUN_REASON","not_applicable") }}'

        {% if not loop.last %}
        union all
        {%  endif %}

    {% endfor %}

{% endif %}
{% endmacro %}


