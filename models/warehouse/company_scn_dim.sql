{{
  config(
    materialized='view',
    schema= 'warehouse'
  )
}}

select *
from {{ ref('stg_segment_scn_company') }}