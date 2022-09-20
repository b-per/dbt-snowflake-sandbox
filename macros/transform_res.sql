{% macro example_transform_res_1(res_dict) %}
    
    -- build the new dict from scratch
    {% set my_obj = {} %}

    -- how to add keys
    {% do my_obj.update({'adapter_response': res_dict['adapter_response']}) %}
    {% do my_obj.update({'node': res_dict['node']}) %}
    
    -- how to remove keys (the second argument of pop is if the key doesn't exist)
    {% do my_obj['node'].pop('config','') %}

    {{ return(my_obj) }}

{% endmacro %}



{% macro example_transform_res_2(res_dict) %}
    
    -- build the new dict from results and and remove nodes
    {% set my_obj = res_dict %}
    
    -- how to remove keys (the second argument of pop is if the key doesn't exist)
    {% do my_obj['node'].pop('compiled_sql','') %}

    {{ return(my_obj) }}

{% endmacro %}