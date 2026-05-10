with coupons as (
    select * from {{source ('pg_raw_schema', 'coupons_df')}}
)

select * from coupons