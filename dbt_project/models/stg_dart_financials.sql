with base as (
    select
        cast(year as int) as year,
        corp_code,
        corp_name,
        account_nm,
        fs_div,
        statement_type,
        reprt_code,
        report_type,
        cast(amount as bigint) as amount,
        loaded_at
    from raw_dart_financial_accounts
),

normalized as (
    select
        *,
        case
            when report_type = 'q1' then make_date(year, 3, 31)
            when report_type = 'half' then make_date(year, 6, 30)
            when report_type = 'q3' then make_date(year, 9, 30)
            when report_type = 'annual' then make_date(year, 12, 31)
            else null
        end as report_date,
        case
            when report_type = 'q1' then 1
            when report_type = 'half' then 2
            when report_type = 'q3' then 3
            when report_type = 'annual' then 4
            else null
        end as quarter_seq
    from base
),

with_prev as (
    select
        *,
        lag(amount) over (
            partition by year, corp_code, fs_div, statement_type, account_nm
            order by quarter_seq
        ) as prev_amount
    from normalized
)

select
    year,
    corp_code,
    corp_name,
    account_nm,
    fs_div,
    statement_type,
    reprt_code,
    report_type,
    report_date,
    amount,          -- 누적금액(YTD)
    case
        when quarter_seq = 1 then amount
        when quarter_seq in (2, 3, 4) and prev_amount is not null then amount - prev_amount
        else null
    end as pure_amount,
    loaded_at
from with_prev
