{% macro source(source_name, table_name) %}

    -- {{graph.sources}}
    {{ return(graph.sources) }}
    {{ return(builtins.source(source_name, table_name)) }}

{% endmacro %}