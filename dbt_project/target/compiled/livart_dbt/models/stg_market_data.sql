with source_data as (
    select
        cast(trade_date as date) as trade_date,
        symbol,
        cast(open as numeric(18, 6)) as open,
        cast(high as numeric(18, 6)) as high,
        cast(low as numeric(18, 6)) as low,
        cast(close as numeric(18, 6)) as close,
        cast(adj_close as numeric(18, 6)) as adj_close,
        cast(volume as bigint) as volume,
        loaded_at
    from raw_market_data
)

select *
from source_data