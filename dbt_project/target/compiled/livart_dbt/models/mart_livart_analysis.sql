

with stg as (
    select *
    from "dbt"."public"."stg_market_data"
),

livart_price as (
    select
        trade_date,
        close as livart_close,
        ((close / nullif(lag(close) over (order by trade_date), 0)) - 1) * 100 as livart_pct_change
    from stg
    where symbol = '079430.KS'
),

usdkrw_fx as (
    select
        trade_date,
        close as usdkrw_close,
        ((close / nullif(lag(close) over (order by trade_date), 0)) - 1) * 100 as usdkrw_pct_change
    from stg
    where symbol = 'KRW=X'
)

select
    p.trade_date,
    p.livart_close,
    p.livart_pct_change,
    f.usdkrw_close,
    f.usdkrw_pct_change
from livart_price p
join usdkrw_fx f
    on p.trade_date = f.trade_date
order by p.trade_date