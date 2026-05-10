
with source as (

    select * from {{ source('pg_raw_schema', 'coupons_df') }}

),

renamed as (

    select
        id as coupon_id,
        upper(trim(code)) as coupon_code,
        discount_pct,
        max_uses,
        used_count,
        is_active,
        cast(nullif(trim(expires_at), 'None') as timestamp) as expires_at,
        created_at,
        updated_at

    from source

)

select * from renamed



