{% macro date_trunc_month(column) %}
    {%- if target.type == 'bigquery' -%}
        DATE(DATE_TRUNC({{ column }}, MONTH))
    {%- else -%}
        DATE(DATE_TRUNC('month', {{ column }}))
    {%- endif -%}
{% endmacro %}
