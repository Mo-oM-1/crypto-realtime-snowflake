{% macro flatten_whoz_array(prefix, array_path, max_index=none, fields=none, cast_type='STRING', value_prefix='value') %}
{%- if max_index is not none -%}
{%- for i in range(max_index) -%}
{%- if fields -%}
{%- for field in fields -%}
    {{ value_prefix }}:{{ array_path }}[{{ i }}].{{ field.name }}::{{ field.type }} AS {{ prefix }}_{{ i }}_{{ field.alias }}{{ "," if not (i == max_index - 1 and loop.last) }}
{% endfor -%}
{%- else -%}
    {{ value_prefix }}:{{ array_path }}[{{ i }}]::{{ cast_type }} AS {{ prefix }}_{{ i }}{{ "," if i < max_index - 1 }}
{% endif -%}
{%- endfor -%}
{%- else -%}
    {{ value_prefix }}:{{ array_path }}::{{ cast_type }} AS {{ prefix }}
{%- endif -%}
{% endmacro %}
