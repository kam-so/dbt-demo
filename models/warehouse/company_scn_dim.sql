{{
  config(
    materialized='view',
    schema= 'warehouse'
  )
}}

select   enr_company_scn_id as company_scn_id,
       , TRIM(enrichment:company:company_scn)::string as company_scn
       , UPPER(TRIM(enrichment:company:company_scn))::string as company_scn_upper
       , TRIM(enrichment:company:company_verdict)::string as company_verdict
       , enrichment
from {{ ref('stg_segment_scn_company') }}