

with source as (

    select * from {{ source('pg_raw_schema', 'orders_df') }}

),

renamed as (

    select
        id,
        customer_id,
        coupon_id,
        status,
        subtotal,
        discount_amount,
        total,
        notes,
        created_at,
        updated_at

    from source

)

select * from renamed

