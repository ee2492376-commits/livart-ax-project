-- models/marts/mart_dart_financials.sql
{{ config(materialized='table') }}

with src as (
    select *
    from {{ ref('stg_dart_financials') }}
    where statement_type = '손익계산서'
      and fs_div = 'CFS'
),

pivoted as (
    select
        corp_code,
        corp_name,
        year,
        report_type,
        report_date,
        max(case when account_nm = '매출액'     then pure_amount end) as revenue,
        max(case when account_nm = '영업이익'   then pure_amount end) as operating_profit,
        max(case when account_nm = '당기순이익' then pure_amount end) as net_income
    from src
    group by corp_code, corp_name, year, report_type, report_date
),

with_lags as (
    select
        *,
        lag(revenue, 4)          over (partition by corp_code order by report_date) as revenue_yoy_base,
        lag(operating_profit, 4) over (partition by corp_code order by report_date) as op_yoy_base,
        lag(revenue, 1)          over (partition by corp_code order by report_date) as revenue_qoq_base
    from pivoted
)

select
    corp_code,
    corp_name,
    year,
    report_type,
    report_date,
    revenue,
    operating_profit,
    net_income,
    -- 파생 피처
    case when nullif(revenue_yoy_base, 0) is not null
         then (revenue::numeric / revenue_yoy_base - 1) * 100 end as revenue_yoy_pct,
    case when nullif(op_yoy_base, 0) is not null
         then (operating_profit::numeric / op_yoy_base - 1) * 100 end as op_yoy_pct,
    case when nullif(revenue_qoq_base, 0) is not null
         then (revenue::numeric / revenue_qoq_base - 1) * 100 end as revenue_qoq_pct,
    case when nullif(revenue, 0) is not null
         then (operating_profit::numeric / revenue) * 100 end as op_margin_pct
from with_lags
order by corp_code, report_date