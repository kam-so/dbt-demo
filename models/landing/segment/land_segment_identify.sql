{{
  config(
    materialized='incremental',
    schema= 'landing',
    sort= 'load_id'
  )
}}

SELECT DEV_DB.LANDING.SEQ_LAND_SEGMENT_IDENTIFY.NEXTVAL LAND_SEGMENT_IDENTIFY_KEY
      ,*
FROM {{ source('aws_datalake','RAW_SEGMENT_IDENTIFY_T')}}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where load_id > (select max(load_id) from {{ this }})

{% endif %}
