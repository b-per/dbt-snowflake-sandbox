{% macro obj_to_dict(obj) %}
    {% if (obj is string) or (obj is integer) %}
        {{ return(obj) }}
    {% elif obj is iterable %}
    {% set mylist = [] %}
    {% for indiv_obj in obj %}
        {#
        {% set mydict[key] = obj_to_dict(obj.key) %}
        #}
        {% set _ = mylist.append(obj_to_dict(indiv_obj)) %}
    {% endfor %}
    {{ return(mylist) }}
    {% else %}
    {% set mydict = {} %}
    {% for key in obj %}
        {{ log('KEY: ' ~ key, True) }}
        {#
        {% set mydict[key] = obj_to_dict(obj.key) %}
        #}
        {{ log('VAL: ' ~ obj.key, True) }}
        {% set _ = mydict.update({key: obj_to_dict(obj.key)}) %}
    {% endfor %}
    {{ return(mydict) }}
    {% endif %}
{% endmacro %}