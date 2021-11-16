{{
  config(
    materialized='incremental',
    schema= 'staging'
  )
}}

with identify as
(
    SELECT 
         METADATA
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
        ,TYPE
        ,WRITEKEY
        ,TRIM(LOWER(EMAIL)) as EMAIL
        ,TRIM(USERID) as USERID
        ,VERSION
        ,SOURCE_SYSTEM
        ,ORIGINAL
        ,METADATA_FILENAME
        ,METADATA_FILE_ROW_NUMBER
        ,LOAD_TIMESTAMP
        ,LOAD_ID
        ,TRAITS
        ,ENRICHMENT

    FROM {{ ref('land_segment_identify') }}

    WHERE metadata_filename not like '%error%' 
)
,
all_result as
(
  select CAST( NVL2(enrichment:company:company_scn_id,TRUE,FALSE) AS BOOLEAN) GOT_COMPANY_SCN_FLAG
        , CONTAINS(LOWER(TRAITS),'"clearbit_company') as GOT_TRA_CLEARBIT_FLAG
        , CONTAINS(LOWER(TRAITS),'"clearbit_reveal_company') as GOT_TRA_CLEARBIT_REVEAL_FLAG
        , CONTAINS(LOWER(TRAITS),'"email') as GOT_TRA_CONTACT_FLAG
        , REGEXP_REPLACE(COLLATE( traits:company:name , '' ), '[+,.()-]' , ' ', 1, 0) as tra_company_name_upper
        , enrichment:company:company_scn_id::numeric  as enr_company_scn_id
        ,* 
  from identify
  where NOT ( ANONYMOUSID is NULL and ( USERID IS NULL OR USERID = '' ) )
)
select * from all_result

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  --where LAND_SEGMENT_IDENTIFY_KEY > (select max(LAND_SEGMENT_IDENTIFY_KEY) from {{ this }})
  where {{ var("segment_identify_key") }} > (select max( {{ var("segment_identify_key") }}) from {{ this }})

{% endif %}
