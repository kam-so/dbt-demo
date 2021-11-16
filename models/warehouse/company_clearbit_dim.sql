{{
  config(
    materialized='view',
    schema= 'warehouse'
  )
}}

select *,
       , TRIM(traits:clearbit_company_category_industry)::string as company_category_industry
       , TRIM(traits:clearbit_company_category_industry_group)::string as company_category_industry_group
       , TRIM(traits:clearbit_company_category_sector)::string as company_category_sector
       , TRIM(traits:clearbit_company_domain)::string as company_domain
       , TRIM(traits:clearbit_company_name)::string as company_name
       , UPPER(TRIM(traits:clearbit_company_name))::string as company_name_upper
       , TRIM(traits:clearbit_company_legal_name)::string as company_legal_name
       , UPPER(TRIM(traits:clearbit_company_legal_name))::string as company_legal_name_upper
       , TRIM(traits:clearbit_company_metrics_annual_revenue)::string as company_metrics_annual_revenue
       , TRIM(traits:clearbit_company_metrics_employees)::numeric as company_metrics_employees
       , TRIM(traits:clearbit_company_metrics_employees_range)::string as company_metrics_employees_range
       , TRIM(traits:clearbit_company_metrics_estimated_annual_revenue)::string as company_metrics_estimated_annual_revenue
       , traits
from {{ ref('stg_segment_clearbit_company') }}