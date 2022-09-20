{% set key_del = 'kb1.kb2.kb3' %}

{% set key_del_list = key_del.split('.') %}
{{ key_del_list }}


{% for lvl in key_del_list[0:-1] %}
    --lvl
    {{lvl}}
{% endfor %}


{% set myd={"ka1":"va1","kb1":{"kb2":{"kb3":"vb3"}}} %}
{% set myd2={"ka1":"va1","kb1":{"kb2":{"kb3":"vb3","kb3b":"vb3b"}}} %}

{#
{% do myd.get('kb1',{}).pop('kb2','') %}
#}

-----
{{ myd | tojson }}


{% macro popkeyold(key_del_list, myd2) %}
    {% if key_del_list | length == 1 %}
        {% do myd2.pop(key_del_list[0],'') %}
    {% elif myd2.get(key_del_list[0],{}) is not string %}
        {{ return(popkey(key_del_list[1:],myd2.get(key_del_list[0],{}))) }}
    {% endif %}
{% endmacro %}

{% macro remove_keys_dict(nested_keys, og_dict) %}
    {% for nested_key in nested_keys %}
        {% set og_dict =  popkey(nested_key.split('.'), og_dict) %}
    {% endfor %}
{% endmacro %}

{#
{% for key_del_list in [['kb1','kb2'],['ka1']] %}
{% set myd2 =  popkey(key_del_list, myd2) %}
{% endfor %}


{{ myd2 | tojson }}
#}

{{ remove_keys_dict(['ka1','kb0.kb2.kb3'], myd2) }}
{{ myd2 }}