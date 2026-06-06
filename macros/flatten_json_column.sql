{% macro flatten_json_column(source_relation, json_column, id_columns=[], exclude_keys=[]) %}

{%- set id_columns_upper = [] -%}
{%- for col in id_columns -%}
    {%- do id_columns_upper.append(col | upper) -%}
{%- endfor -%}

{%- set exclude_keys_upper = [] -%}
{%- for k in exclude_keys -%}
    {%- do exclude_keys_upper.append(k | upper) -%}
{%- endfor -%}

{%- set type_query %}
    SELECT
        f.key AS key_name,
        TYPEOF(f.value) AS value_type
    FROM {{ source_relation }},
         LATERAL FLATTEN(input => PARSE_JSON({{ json_column }})) f
    GROUP BY f.key, TYPEOF(f.value)
    ORDER BY f.key
{% endset -%}

{%- set type_results = run_query(type_query) -%}

{%- set scalar_keys = [] -%}
{%- set object_keys = [] -%}
{%- set array_keys = [] -%}

{%- if execute -%}
    {%- for row in type_results.rows -%}
        {%- set key_name = row[0] -%}
        {%- set value_type = row[1] -%}
        {%- if key_name | upper not in id_columns_upper and key_name | upper not in exclude_keys_upper -%}
            {%- if value_type == 'OBJECT' and key_name not in object_keys -%}
                {%- do object_keys.append(key_name) -%}
            {%- elif value_type == 'ARRAY' and key_name not in array_keys -%}
                {%- do array_keys.append(key_name) -%}
            {%- elif value_type not in ('OBJECT', 'ARRAY') and key_name not in scalar_keys -%}
                {%- do scalar_keys.append(key_name) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

{%- set sub_keys_map = {} -%}
{%- if execute and object_keys | length > 0 -%}
    {%- for obj_key in object_keys -%}
        {%- set sub_query %}
            SELECT DISTINCT f2.key AS sub_key
            FROM {{ source_relation }},
                 LATERAL FLATTEN(input => PARSE_JSON({{ json_column }}):{{ obj_key }}) f2
            WHERE TYPEOF(f2.value) NOT IN ('OBJECT', 'ARRAY')
            ORDER BY f2.key
        {% endset -%}
        {%- set sub_results = run_query(sub_query) -%}
        {%- do sub_keys_map.update({obj_key: sub_results.columns[0].values()}) -%}
    {%- endfor -%}
{%- endif -%}

SELECT
    {%- for id_col in id_columns %}
    {{ id_col }},
    {%- endfor %}

    {%- set ns = namespace(first=true) -%}

    {%- for key in scalar_keys %}
    {%- if not ns.first %},{% endif %}
    PARSE_JSON({{ json_column }}):{{ key }}::VARCHAR AS {{ key }}
    {%- set ns.first = false -%}
    {%- endfor %}

    {%- for obj_key in object_keys %}
    {%- if obj_key in sub_keys_map and sub_keys_map[obj_key] | length > 0 %}
    {%- for sub_key in sub_keys_map[obj_key] %}
    {%- if not ns.first %},{% endif %}
    PARSE_JSON({{ json_column }}):{{ obj_key }}.{{ sub_key }}::VARCHAR AS {{ obj_key }}_{{ sub_key }}
    {%- set ns.first = false -%}
    {%- endfor %}
    {%- else %}
    {%- if not ns.first %},{% endif %}
    PARSE_JSON({{ json_column }}):{{ obj_key }}::VARIANT AS {{ obj_key }}
    {%- set ns.first = false -%}
    {%- endif %}
    {%- endfor %}

    {%- for key in array_keys %}
    {%- if not ns.first %},{% endif %}
    PARSE_JSON({{ json_column }}):{{ key }}::VARIANT AS {{ key }}
    {%- set ns.first = false -%}
    {%- endfor %}

FROM {{ source_relation }}

{% endmacro %}
