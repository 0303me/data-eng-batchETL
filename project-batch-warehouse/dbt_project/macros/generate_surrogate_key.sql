{% macro generate_surrogate_key(cols) -%}
  md5(concat({{ cols | join(", ") }}))
{%- endmacro %}