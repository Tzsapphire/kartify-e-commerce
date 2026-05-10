

with source as (

    select * from {{ source('pg_raw_schema', 'shipping_df') }}

),

renamed as (

    select
        id,
        order_id,
        status,
        carrier,
        tracking_number,
        address,
        city,
        state,
        country,
        postal_code,
        shipped_at,
        delivered_at,
        created_at,
        updated_at

    from source

)

select * from renamed

