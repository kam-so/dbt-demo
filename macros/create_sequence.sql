{% macro create_sequence(database_nm,schema_nm,sequence_nm,start_num,increment_num,comment) %}
CREATE OR REPLACE SEQUENCE {{database_nm}}.STAGIN{{schema_nm}}.{{sequence_nm}}
    START {{start_num}}
    INCREMENT {{increment_num}}
    COMMENT = '{{comment}}';
{% endmacro %}