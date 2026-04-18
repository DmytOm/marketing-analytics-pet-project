{% macro safe_divide(numerator, denominator, decimal_places=2) %}
    ROUND(
        {{ numerator }} / NULLIF( {{ denominator }}, 0),
        {{ decimal_places }}
    )
{% endmacro %}
