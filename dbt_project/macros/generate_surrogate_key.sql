-- macros/generate_surrogate_key.sql
-- ────────────────────────────────────
-- Macro de surrogate key compatible Snowflake
-- dbt_utils.generate_surrogate_key est préféré, mais ce fallback
-- garantit que le projet fonctionne même sans le package installé.

{% macro generate_surrogate_key(field_list) %}

    {%- set fields = [] -%}

    {%- for field in field_list -%}
        {%- set _ = fields.append(
            "coalesce(cast(" ~ field ~ " as varchar), 'NULL_PLACEHOLDER')"
        ) -%}
    {%- endfor -%}

    MD5(
        CONCAT_WS('|', {{ fields | join(', ') }})
    )

{% endmacro %}
