{% macro remove_special_characters(input_string) -%}
    REGEXP_REPLACE(COLLATE( {{ input_string }} , '' ), '[+,.()-]' , ' ', 1, 0) 
{%- endmacro %}