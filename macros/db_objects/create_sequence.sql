{% macro create_sequence() %}

    CREATE SEQUENCE IF NOT EXISTS LANDING.SEQ_LAND_SEGMENT_IDENTIFY START 1 INCREMENT 1 COMMENT = 'sequence for segment data at landing stage';
    CREATE SEQUENCE IF NOT EXISTS STAGING.SEQ_STG_SEGMENT_IDENTIFY START 1 INCREMENT 1 COMMENT = 'sequence for segment data at staging stage';
    CREATE SEQUENCE IF NOT EXISTS WAREHOUSE.SEQ_CONTACT_DIM START 1 INCREMENT 1 COMMENT = 'sequence for contact dim table';

{% endmacro %}
