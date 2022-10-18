{% macro sourcexxx(source_name, table_name) %}

    {{ log(graph | tojson, 1) }}
    {{ return(builtins.source(source_name, table_name)) }}

{% endmacro %}