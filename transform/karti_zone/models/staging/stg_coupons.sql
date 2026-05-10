

with source as (

    select * from {{ source('pg_raw_schema', 'coupons_df') }}

),

renamed as (

    select
        id,
        code,
        discount_pct,
        max_uses,
        used_count,
        is_active,
        expires_at,
        created_at,
        updated_at

    from source

)

select * from renamed

