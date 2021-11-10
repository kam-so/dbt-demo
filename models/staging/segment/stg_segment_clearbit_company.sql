{{
  config(
    materialized='incremental',
    schema= 'staging',
    unique_key = 'tra_clearbit_company_legal_name_upper'
  )
}}

with cb_comp_full as
(
    select *
    from {{ ref('stg_segment_identify') }}
    where GOT_TRA_CLEARBIT_FLAG
)
,
cb_comp_unique as
(
    select {{remove_special_characters('tra_clearbit_company_legal_name_upper')}} , max(ORIGINALTIMESTAMP) ORIGINALTIMESTAMP
    from cb_comp_full
    group by tra_clearbit_company_legal_name_upper
)

SELECT
       cb_comp_full.LOAD_ID
      ,cb_comp_full.tra_clearbit_company_legal_name_upper
      ,cb_comp_full.tra_clearbit_company_category_industry
      ,cb_comp_full.tra_clearbit_company_category_industry_group
      ,cb_comp_full.tra_clearbit_company_category_sector
      ,cb_comp_full.tra_clearbit_company_domain
      ,cb_comp_full.tra_clearbit_company_name
      ,cb_comp_full.tra_clearbit_company_name_upper
      ,cb_comp_full.tra_clearbit_company_legal_name
      ,cb_comp_full.tra_clearbit_company_metrics_annual_revenue
      ,cb_comp_full.tra_clearbit_company_metrics_employees
      ,cb_comp_full.tra_clearbit_company_metrics_employees_range
      ,cb_comp_full.tra_clearbit_company_metrics_estimated_annual_revenue
      ,cb_comp_full.tra_clearbit_person_employment_seniority
      ,cb_comp_full.tra_clearbit_person_employment_title
      ,cb_comp_full.TRAITS
FROM cb_comp_unique 
  inner join
     cb_comp_full
  on 
     cb_comp_unique.ORIGINALTIMESTAMP = cb_comp_full.ORIGINALTIMESTAMP
  and 
     cb_comp_unique.tra_clearbit_company_legal_name_upper = cb_comp_full.tra_clearbit_company_legal_name_upper


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  --where cb_comp_full.scn_full.STG_SEGMENT_IDENTIFY_KEY > (select max(STG_SEGMENT_IDENTIFY_KEY) from {{ this }})
  where {{ var("segment_identify_key") }} > (select max( {{ var("segment_identify_key") }}) from {{ this }})

{% endif %}
