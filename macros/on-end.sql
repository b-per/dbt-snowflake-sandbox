{% macro on_end(table='load_results', schema=target.schema ~ '_audit', database = target.database) %}

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

        -- reconstructing the result timing
        {% set res_timing_list = [] %}
        {% for res_timing in res.timing %}
            {% do res_timing_list.append(
                {
                    'name': res_timing.name,
                    'started_at' : res_timing.started_at | string,
                    'completed_at' : res_timing.completed_at | string,
                }
            ) %}
        {% endfor %}

        -- reconstructing the column lists
        {% set columns_list = {} %}
        {% for col_key, col_val in res.node.columns.items() %}
            {% do columns_list.update(  { col_key : 
                {
                    'name': col_val.name,
                    'description': col_val.description,
                    'meta': col_val.meta,
                    'data_type': col_val.data_type,
                    'quote': col_val.quote,
                    'tags': col_val.tags,
                }})
             %}
        {% endfor %}

        -- reconstructing the hooks
        {% set pre_hooks = [] %}
        {% for indiv_pre_hook in res.node.config.pre_hook or [] %}
            {% do pre_hooks.append(
                {
                    'sql': indiv_pre_hook.sql,
                    'transaction' : indiv_pre_hook.transaction,
                    'index' : indiv_pre_hook.index or '',
                }
            ) %}
        {% endfor %}

        {% set post_hooks = [] %}
        {% for indiv_post_hook in res.node.config.post_hook or [] %}
            {% do post_hooks.append(
                {
                    'sql': indiv_post_hook.sql,
                    'transaction' : indiv_post_hook.transaction,
                    'index' : indiv_post_hook.index or '',
                }
            ) %}
        {% endfor %}

        -- building the full object
        {% set my_obj = {
            'status' : res.status,
            'timing' : res_timing_list,
            'thread_id' : res.thread_id,
            'execution_time' : res.execution_time,
            'adapter_response' : res.adapter_response,
            'message' : res.message,
            'failures' : res.failures,
            'node' : {
                'unique_id': res.node.unique_id,
                'database': res.node.database or '',
                'schema': res.node.schema or '',
                'name': res.node.name or '',
                'package_name': res.node.package_name or '',
                'original_file_path': res.node.original_file_path or '',
                'checksum': res.node.checksum.checksum or '',
                'tags': res.node.tags or '',
                'refs': res.node.refs or '',
                'sources': res.node.sources or '',
                'metrics': res.node.metrics or '',
                'depends_on': res.node.depends_on.nodes or '',
                'description': res.node.description or '',
                'columns': columns_list,
                'meta': res.node.meta or '',
                'docs': {'show' : res.node.docs.show},
                'patch_path': res.node.patch_path or '',
                'compiled_path': res.node.compiled_path or '',
                'build_path': res.node.build_path or '',
                'deferred': res.node.deferred or '',
                'unrendered_config': res.node.unrendered_config or '',
                'created_at': res.node.created_at or '',
                'config_call_dict': res.node.config_call_dict or '',
                'relation_name': res.node.relation_name or '',
                'compiled_sql': res.node.compiled_sql or '',
                'config': {
                    'materialized': res.node.config.materialized or '',
                    'incremental_strategy': res.node.config.incremental_strategy or '',
                    'persist_docs': res.node.config.persist_docs or '',
                    'post_hook': post_hooks,
                    'pre_hook': pre_hooks,
                    'quoting': res.node.config.quoting or '',
                    'column_types': res.node.config.column_types or '',
                    'full_refresh': res.node.config.full_refresh or '',
                    'unique_key': res.node.config.unique_key or '',
                    'on_schema_change': res.node.config.on_schema_change or '',
                    'grants': res.node.config.grants or '',
                }
            }
        }
        %}
        {% do all_objs.append(my_obj) %}
    {% endfor %}

    -- we insert the data into the metadata table, 1 line per model/test
{#
        {{ log(res | tojson,1) }}
        
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
#}

{% endif %}
{% endmacro %}


