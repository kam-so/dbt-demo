{{
  config(
    materialized='incremental',
    schema= 'staging'
  )
}}

with identify as
(
    SELECT 
         LAND_SEGMENT_IDENTIFY_KEY
        ,METADATA
        ,TRIM(ANONYMOUSID) as ANONYMOUSID
        ,CHANNEL
        ,CONTEXT
        ,INTEGRATIONS
        ,MESSAGEID
        ,ORIGINALTIMESTAMP
        ,PROJECTID
        ,RECEIVEDAT
        ,SENTAT
        ,TIMESTAMP
        ,TRAITS
        ,TYPE
        ,WRITEKEY
        ,TRIM(LOWER(EMAIL)) as EMAIL
        ,TRIM(USERID) as USERID
        ,VERSION
        ,SOURCE_SYSTEM
        ,ORIGINAL
        ,ENRICHMENT
        ,METADATA_FILENAME
        ,METADATA_FILE_ROW_NUMBER
        ,LOAD_TIMESTAMP
        ,LOAD_ID
        , TRIM(traits:address:city)::string as tra_address_city
        , TRIM(traits:address:country)::string as tra_address_country
        , TRIM(traits:address:postal_code)::string as tra_address_postal_code
        , TRIM(traits:company:name)::string as tra_company_name
        , UPPER(TRIM(traits:company:name))::string as tra_company_name_upper
        , to_timestamp ( substr(traits:create_date,0,10) || ' ' || substr(traits:create_date,12,8),'YYYY-MM-DD HH24:MI:SS') as tra_create_date
        , LOWER(TRIM(traits:email))::string as tra_email
        , TRIM(traits:family_name)::string as tra_family_name
        , TRIM(traits:given_name)::string as tra_given_name
        , TRIM(traits:role)::string as tra_role
        , TRIM(traits:clearbit_company_category_industry)::string as tra_clearbit_company_category_industry
        , TRIM(traits:clearbit_company_category_industry_group)::string as tra_clearbit_company_category_industry_group
        , TRIM(traits:clearbit_company_category_sector)::string as tra_clearbit_company_category_sector
        , TRIM(traits:clearbit_company_domain)::string as tra_clearbit_company_domain
        , TRIM(traits:clearbit_company_name)::string as tra_clearbit_company_name
        , UPPER(TRIM(traits:clearbit_company_name))::string as tra_clearbit_company_name_upper
        , TRIM(traits:clearbit_company_legal_name)::string as tra_clearbit_company_legal_name
        , UPPER(TRIM(traits:clearbit_company_legal_name))::string as tra_clearbit_company_legal_name_upper
        , TRIM(traits:clearbit_company_metrics_annual_revenue)::string as tra_clearbit_company_metrics_annual_revenue
        , TRIM(traits:clearbit_company_metrics_employees)::string as tra_clearbit_company_metrics_employees
        , TRIM(traits:clearbit_company_metrics_employees_range)::string as tra_clearbit_company_metrics_employees_range
        , TRIM(traits:clearbit_company_metrics_estimated_annual_revenue)::string as tra_clearbit_company_metrics_estimated_annual_revenue
        , TRIM(traits:clearbit_person_employment_seniority)::string as tra_clearbit_person_employment_seniority
        , TRIM(traits:clearbit_person_employment_title)::string as tra_clearbit_person_employment_title
        , TRIM(traits:clearbit_reveal_company_legal_name)::string as tra_clearbit_reveal_company_legal_name
        , UPPER(TRIM(traits:clearbit_reveal_company_legal_name))::string as tra_clearbit_reveal_company_legal_name_upper
        , TRIM(traits:clearbit_reveal_company_name)::string as tra_clearbit_reveal_company_name
        , UPPER(TRIM(traits:clearbit_reveal_company_name))::string as tra_clearbit_reveal_company_name_upper
        , enrichment:company:company_scn_id::numeric as enr_company_scn_id
        , TRIM(enrichment:company:company_scn)::string as enr_company_scn
        , UPPER(TRIM(enrichment:company:company_scn))::string as enr_company_scn_upper
        , TRIM(enrichment:company:company_submitted)::string as enr_company_submitted
        , to_timestamp ( substr(enrichment:company:company_validated_ts,0,10) || ' ' || substr(enrichment:company:company_validated_ts,12,8),'YYYY-MM-DD HH24:MI:SS') as enr_company_validated_ts
        , TRIM(enrichment:company:company_verdict)::string as enr_company_verdict
        , LOWER(TRIM(enrichment:company:domain_submitted))::string as enr_company_domain_submitted
        , LOWER(TRIM(enrichment:email:email_formatted))::string as enr_email_formatted
        , enrichment:email:email_has_known_bounces::boolean as enr_email_has_known_bounces
        , enrichment:email:email_has_mx::boolean as enr_email_has_mx
        , enrichment:email:email_has_suspected_bounces::boolean as enr_email_has_suspected_bounces
        , enrichment:email:email_has_valid_syntax::boolean as enr_email_has_valid_syntax
        , TRIM(enrichment:email:email_host)::string as enr_email_host
        , TRIM(enrichment:email:email_ip_address)::string as enr_email_ip_address
        , enrichment:email:email_is_disposable::boolean as enr_email_is_disposable
        , enrichment:email:email_is_role::boolean as enr_email_is_role
        , TRIM(enrichment:email:email_local)::string as enr_email_local
        , enrichment:email:email_score::float as enr_email_score
        , TRIM(enrichment:email:email_submitted)::string as enr_email_submitted
        , TRIM(enrichment:email:email_suggestion)::string as enr_email_suggestion
        , to_timestamp ( substr(enrichment:email:email_validated_ts,0,10) || ' ' || substr(enrichment:email:email_validated_ts,12,8),'YYYY-MM-DD HH24:MI:SS') as enr_email_validated_ts
        , TRIM(enrichment:email:email_verdict)::string as enr_email_verdict
        , TRIM(enrichment:location:country:country_iso2)::string as enr_country_iso2
        , TRIM(enrichment:location:country:country_iso3)::string as enr_country_iso3
        , TRIM(enrichment:location:country:country_name)::string as enr_country_name
        , TRIM(enrichment:location:country:country_official_name)::string as enr_country_official_name
        , TRIM(enrichment:location:country:country_submitted)::string as enr_country_submitted
        , to_timestamp ( substr(enrichment:location:country:country_validated_ts,0,10) || ' ' || substr(enrichment:location:country:country_validated_ts,12,8),'YYYY-MM-DD HH24:MI:SS') as enr_country_validated_ts
        , TRIM(enrichment:location:country:country_verdict)::string as enr_country_verdict
        , TRIM(enrichment:location:country:siemens_zone)::string as enr_siemens_zone
    FROM {{ ref('land_segment_identify') }}

    WHERE metadata_filename not like '%error%' 
)
,
all_result as
(
  select STAGING.SEQ_STG_SEGMENT_IDENTIFY.NEXTVAL STG_SEGMENT_IDENTIFY_KEY
        ,CAST( NVL2(ENR_COMPANY_SCN_ID,TRUE,FALSE) AS BOOLEAN) GOT_COMPANY_SCN_FLAG
        , CONTAINS(LOWER(TRAITS),'"clearbit_company') as GOT_TRA_CLEARBIT_FLAG
        , CONTAINS(LOWER(TRAITS),'"clearbit_reveal_company') as GOT_TRA_CLEARBIT_REVEAL_FLAG
        ,CAST( NVL2(USERID,TRUE,FALSE) AS BOOLEAN) GOT_USERID_FLAG
        ,CAST( NVL2(ANONYMOUSID,TRUE,FALSE) AS BOOLEAN) GOT_ANONYMOUSID_FLAG
        ,* 
  from identify
  where NOT ( ANONYMOUSID is NULL and ( USERID IS NULL OR USERID = '' ) )
)
select * from all_result

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where LAND_SEGMENT_IDENTIFY_KEY > (select max(LAND_SEGMENT_IDENTIFY_KEY) from {{ this }})

{% endif %}
