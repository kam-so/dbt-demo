{{
  config(
    materialized='incremental'
  )
}}

SELECT *
FROM {{ source('aws_datalake','RAW_SEGMENT_IDENTIFY_T') }}

{% if is_incremental() %} 

  -- this filter will only be applied on an incremental run
  where {{ var("segment_identify_key") }} > (select max( {{ var("segment_identify_key") }}) from {{ this }})

{% endif %}
limit 100
