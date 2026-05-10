
with source as (

    select * from {{ source('pg_raw_schema', 'orders_df') }}

),

renamed as (

    select
        id as order_id,
        customer_id,
        coupon_id,
        lower(trim(status)) as order_status,
        subtotal,
        discount_amount,
        total,
        nullif(nullif(trim(notes), 'None'), '') as notes,
        created_at,
        updated_at

    from source
)

select * from renamed

