

with source as (

    select * from {{ source('pg_raw_schema', 'order_items_df') }}

),

renamed as (

    select
        id as order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        total_price,
        created_at,
        updated_at

    from source

)

select * from renamed

