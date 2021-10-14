{{ config(materialized='table') }}

SELECT DEMO_DB.DEMO_STAGING.SEQ_STG_SEGMENT_IDENTIFY.NEXTVAL STG_SEGMENT_IDENTIFY_KEY
     ,SRC_SEGMENT_IDENTIFY_KEY
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
     , CONTAINS(TRAITS,'"address":') as tra_address_flag
     , CONTAINS(TRAITS,'"company":') as tra_company_flag
     , CONTAINS(TRAITS,'"clearbit_reveal_company') as tra_clearbit_reveal_flag
     , CONTAINS(TRAITS,'"clearbit_company') as tra_clearbit_flag
     , CONTAINS(ENRICHMENT,'"company":') enr_company_flag
     , CONTAINS(ENRICHMENT,'"email":') enr_email_flag
     , CONTAINS(ENRICHMENT,'"location":') as enr_location_flag
     , TRIM(traits:address:city)::string as tra_address_city
     , TRIM(traits:address:country)::string as tra_address_country
     , TRIM(traits:address:postal_code)::string as tra_address_postal_code
     , TRIM(traits:company:name)::string as tra_company_name
     , UPPER(TRIM(traits:company:name))::string as tra_company_name_upper
     , DEMO_STAGING.CONVERT_TIMESTAMP(traits:create_date)::timestamp as tra_create_date
     , LOWER(TRIM(traits:email))::string as tra_email
     , TRIM(traits:family_name)::string as tra_family_name
     , TRIM(traits:given_name)::string as tra_given_name
     , TRIM(traits:role)::string as tra_role
     , TRIM(traits:clearbit_company_category_industry)::string as tra_clearbit_company_category_industry
     , TRIM(traits:clearbit_company_category_industry_group)::string as tra_clearbit_company_category_industry_group
     , TRIM(traits:clearbit_company_category_sector)::string as tra_clearbit_company_category_sector
     , TRIM(traits:clearbit_company_domain)::string as tra_clearbit_company_domain
     , TRIM(traits:clearbit_company_legal_name)::string as tra_clearbit_company_legal_name
     , UPPER(TRIM(traits:clearbit_company_legal_name))::string as tra_clearbit_company_legal_name_upper
     , TRIM(traits:clearbit_company_metrics_annual_revenue)::string as tra_clearbit_company_metrics_annual_revenue
     , TRIM(traits:clearbit_company_metrics_employees)::string as tra_clearbit_company_metrics_employees
     , TRIM(traits:clearbit_company_metrics_employees_range)::string as tra_clearbit_company_metrics_employees_range
     , TRIM(traits:clearbit_company_metrics_estimated_annual_revenue)::string as tra_clearbit_company_metrics_estimated_annual_revenue
     , TRIM(traits:clearbit_company_name)::string as tra_clearbit_company_name
     , UPPER(TRIM(traits:clearbit_company_name))::string as tra_clearbit_company_name_upper
     , TRIM(traits:clearbit_person_employment_seniority)::string as tra_clearbit_person_employment_seniority
     , TRIM(traits:clearbit_person_employment_title)::string as tra_clearbit_person_employment_title
     , TRIM(traits:clearbit_reveal_company_legal_name)::string as tra_clearbit_reveal_company_legal_name
     , UPPER(TRIM(traits:clearbit_reveal_company_legal_name))::string as tra_clearbit_reveal_company_legal_name_upper
     , enrichment:company:company_scn_id::numeric as enr_company_scn_id
     , TRIM(enrichment:company:company_scn)::string as enr_company_scn
     , UPPER(TRIM(enrichment:company:company_scn))::string as enr_company_scn_upper
     , TRIM(enrichment:company:company_submitted)::string as enr_company_submitted
     , DEMO_STAGING.CONVERT_TIMESTAMP(enrichment:company:company_validated_ts)::timestamp as enr_company_validated_ts
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
     , DEMO_STAGING.CONVERT_TIMESTAMP(enrichment:email:email_validated_ts)::timestamp as enr_email_validated_ts
     , TRIM(enrichment:email:email_verdict)::string as enr_email_verdict
     , TRIM(enrichment:location:country:country_iso2)::string as enr_country_iso2
     , TRIM(enrichment:location:country:country_iso3)::string as enr_country_iso3
     , TRIM(enrichment:location:country:country_name)::string as enr_country_name
     , TRIM(enrichment:location:country:country_official_name)::string as enr_country_official_name
     , TRIM(enrichment:location:country:country_submitted)::string as enr_country_submitted
     , DEMO_STAGING.CONVERT_TIMESTAMP(enrichment:location:country:country_validated_ts)::timestamp as enr_country_validated_ts
     , TRIM(enrichment:location:country:country_verdict)::string as enr_country_verdict
     , TRIM(enrichment:location:country:siemens_zone)::string as enr_siemens_zone
FROM DEMO_DB.DEMO_INGESTION.SRC_SEGMENT_IDENTIFY
WHERE LOAD_ID <= 202109140121243970000000003
    AND metadata_filename not like '%error%'  