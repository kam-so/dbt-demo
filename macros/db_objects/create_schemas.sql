{% macro create_schemas() %}

    CREATE SCHEMA IF NOT EXISTS LANDING COMMENT = 'ingested raw data from external source - no transformation - just load ';
    CREATE SCHEMA IF NOT EXISTS STAGING COMMENT = 'intermediate area used for data transformation/processing ';
    CREATE SCHEMA IF NOT EXISTS WAREHOUSE COMMENT = 'data warehouse for dim & fact tables ';
    CREATE SCHEMA IF NOT EXISTS MARKETING COMMENT = 'DM for Marketing';

{% endmacro %}
