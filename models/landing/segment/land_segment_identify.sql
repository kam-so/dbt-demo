{{
  config(
    materialized='incremental'
  )
}}

SELECT {{ var("lnd_schema") }}.SEQ_LAND_SEGMENT_IDENTIFY.NEXTVAL LAND_SEGMENT_IDENTIFY_KEY
      ,*
FROM {{ source('aws_datalake','RAW_SEGMENT_IDENTIFY_T')}}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where {{ var("segment_identify_key") }} > (select max( {{ var("segment_identify_key") }}) from {{ this }})

{% endif %}
