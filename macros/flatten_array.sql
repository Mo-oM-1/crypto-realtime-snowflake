{% macro flatten_array(prefix, json_path, max_index, fields, col_prefix=none) %}
{%- set pfx = col_prefix if col_prefix else prefix | upper | replace('.', '_') -%}
{%- for i in range(max_index) -%}
{%- for field in fields -%}
    {{ json_path }}[{{ i }}].{{ field.name }}::{{ field.type }} AS {{ pfx }}_{{ i }}_{{ field.alias | upper }}{{ "," if not (loop.last and i == max_index - 1) }}
{% endfor -%}
{%- endfor -%}
{% endmacro %}

{% macro flatten_array_simple(prefix, json_path, max_index, cast_type) %}
{%- for i in range(max_index) -%}
    {{ json_path }}[{{ i }}]::{{ cast_type }} AS {{ prefix | upper }}_{{ i }}{{ "," if not loop.last }}
{% endfor -%}
{% endmacro %}
