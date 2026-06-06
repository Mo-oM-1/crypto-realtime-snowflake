{% macro flatten_napta_json(source_ref, json_col, scalar_cols, nested_cols, array_cols, custom_cols=[]) %}

SELECT
    {%- set ns = namespace(first=true) -%}

    {%- for col in scalar_cols %}
    {%- if not ns.first %},{% endif %}
    value:{{ col.path }}::{{ col.type }} AS {{ col.alias }}
    {%- set ns.first = false -%}
    {%- endfor %}

    {%- for col in nested_cols %}
    ,value:{{ col.path }}::{{ col.type }} AS {{ col.alias }}
    {%- endfor %}

    {%- for arr in array_cols %}
    {%- for i in range(arr.max_index) %}
    {%- if arr.sub_fields is defined %}
    {%- for sub in arr.sub_fields %}
    ,value:{{ arr.path }}[{{ i }}].{{ sub.name }}::{{ sub.type }} AS {{ arr.alias_prefix | upper }}_{{ i }}_{{ sub.name | upper }}
    {%- endfor %}
    {%- else %}
    ,value:{{ arr.path }}[{{ i }}]::{{ arr.type }} AS {{ arr.alias_prefix | upper }}_{{ i }}
    {%- endif %}
    {%- endfor %}
    {%- endfor %}

    {%- for col in custom_cols %}
    ,{{ col.expression }} AS {{ col.alias }}
    {%- endfor %}

FROM {{ source_ref }},
LATERAL FLATTEN(input => {{ json_col }})

{% endmacro %}
