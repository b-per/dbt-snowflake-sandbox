{% macro on_end() %}
{% if execute %}

    create schema if not exists {{target.database}}.{{target.schema}}_audit;
    create table if not exists {{target.database}}.{{target.schema}}_audit.load_results (json_payload object) ;
    {# {{ log('Results1: ' ~ results, info=True) }} #}

    insert into {{target.database}}.{{target.schema}}_audit.load_results 
    {% for res in results %}
        {% set my_obj = {
            'status': res.status, 
            'adapter_response': res.adapter_response,
            'failures': res.failures,
            'unique_id': res.node.unique_id,
            'relation_name': res.node.relation_name or '',
            'tags': res.node.config.materialized,
            'tags': res.node.tags,
            'invocation_id': invocation_id,
            'dbt_cloud_project_id': env_var('DBT_CLOUD_PROJECT_ID','not_applicable'),
            'dbt_cloud_job_id': env_var('DBT_CLOUD_JOB_ID','not_applicable'),
            'dbt_cloud_run_id': env_var('DBT_CLOUD_RUN_ID','not_applicable'),
            'dbt_cloud_run_reason': env_var('DBT_CLOUD_RUN_REASON','not_applicable')
            }
        %}
    select parse_json('{{ tojson(my_obj) }}')
    {% if not loop.last %}
        union all
    {%  endif %}
    {% endfor %}

{% endif %}
{% endmacro %}