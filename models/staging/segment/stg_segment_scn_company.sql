{{
  config(
    materialized='incremental',
    schema= 'staging',
    unique_key = 'enr_company_scn_id'
  )
}}

with scn_full as
(
    select *
    from {{ ref('stg_segment_identify') }}
    where GOT_COMPANY_SCN_FLAG
        and NOT GOT_TRA_CLEARBIT_FLAG
        and NOT GOT_TRA_CLEARBIT_REVEAL_FLAG
)
,
scn_unique as
(
    select enr_company_scn_id, max(ORIGINALTIMESTAMP) ORIGINALTIMESTAMP
    from scn_full
    group by enr_company_scn_id
)

SELECT LOAD_ID
      ,scn_full.enr_company_scn_id
      ,scn_full.enr_company_scn
      ,scn_full.enr_company_scn_upper
      ,scn_full.enr_company_verdict
      ,scn_full.enrichment
FROM scn_unique 
  inner join
     scn_full
  on 
     scn_unique.ORIGINALTIMESTAMP = scn_full.ORIGINALTIMESTAMP
  and 
     scn_unique.enr_company_scn_id = scn_full.enr_company_scn_id

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  --where scn_full.STG_SEGMENT_IDENTIFY_KEY > (select max(STG_SEGMENT_IDENTIFY_KEY) from {{ this }})
  where {{ var("segment_identify_key") }} > (select max( {{ var("segment_identify_key") }}) from {{ this }})

{% endif %}
