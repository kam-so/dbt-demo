{{
  config(
    materialized='incremental',
    schema= 'staging',
    unique_key = 'enr_email_formatted'
  )
}}

with userid_full as
(
    select *
    from {{ ref('stg_segment_identify') }}
    where USERID is not null and USERID <> ''
        and NOT GOT_TRA_CLEARBIT_FLAG
        and NOT GOT_TRA_CLEARBIT_REVEAL_FLAG
        and enr_email_formatted is not null
)
,
userid_unique as
(
    select enr_email_formatted, max(ORIGINALTIMESTAMP) ORIGINALTIMESTAMP
    from userid_full
    group by enr_email_formatted
)

SELECT userid_full.userid
      ,userid_full.tra_family_name
      ,userid_full.tra_given_name
      ,userid_full.enr_email_formatted
      ,enrichment:email:email_verdict as email_verdict
      ,userid_full.enr_company_scn_id
      ,userid_full.enr_company_scn
      ,userid_full.enr_company_scn_upper
      ,userid_full.traits
      ,userid_full.enrichment
FROM userid_unique 
  inner join
     userid_full
  on 
     userid_unique.ORIGINALTIMESTAMP = userid_full.ORIGINALTIMESTAMP
  and 
     userid_unique.enr_email_formatted = userid_full.enr_email_formatted
order by userid_full.userid

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where STG_SEGMENT_IDENTIFY_KEY > (select max(STG_SEGMENT_IDENTIFY_KEY) from {{ this }})

{% endif %}
