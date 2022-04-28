{% macro on_end(table='load_results', schema=target.schema ~ '_audit', database = target.database) %}
{% if execute %}

    create schema if not exists {{database}}.{{schema}};
    create table if not exists {{database}}.{{schema}}.{{table}} (json_payload object, run_started_at datetime) ;
    {#  
    {{ log('Results1: ' ~ results[0] | list, info=True) }}
    {{ log('Results1: ' ~ obj_to_dict(results), info=True) }}
    #}
    

    insert into {{database}}.{{schema}}.{{table}}
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
    select parse_json('{{ tojson(my_obj) }}'), '{{run_started_at}}'::datetime
    {% if not loop.last %}
        union all
    {%  endif %}
    {% endfor %}

{% endif %}
{% endmacro %}