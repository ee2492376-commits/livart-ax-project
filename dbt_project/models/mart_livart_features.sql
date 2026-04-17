-- models/marts/mart_livart_features.sql
{{ config(materialized='table') }}

with price as (
    select
        trade_date,
        livart_close,
        livart_pct_change,
        usdkrw_close,
        usdkrw_pct_change
    from {{ ref('mart_livart_analysis') }}
),

fin as (
    select
        corp_code,
        corp_name,
        report_date,
        revenue,
        operating_profit,
        net_income,
        revenue_yoy_pct,
        op_yoy_pct,
        op_margin_pct
    from {{ ref('mart_dart_financials') }}
    where corp_code = '00300548' /* 현대리바트 corp_code 상수 걸기. 아래 주석 참고 */
),

-- 각 trade_date에 대해 "이전에 공시된 가장 최신 재무"를 매핑
latest_fin_per_day as (
    select
        p.trade_date,
        (
            select f.report_date
            from fin f
            where f.report_date <= p.trade_date
            order by f.report_date desc
            limit 1
        ) as latest_report_date
    from price p
)

select
    p.trade_date,
    p.livart_close,
    p.livart_pct_change,
    p.usdkrw_close,
    p.usdkrw_pct_change,
    f.report_date       as fin_report_date,
    f.revenue,
    f.operating_profit,
    f.net_income,
    f.revenue_yoy_pct,
    f.op_yoy_pct,
    f.op_margin_pct
from price p
left join latest_fin_per_day l on l.trade_date = p.trade_date
left join fin f                  on f.report_date = l.latest_report_date
order by p.trade_date