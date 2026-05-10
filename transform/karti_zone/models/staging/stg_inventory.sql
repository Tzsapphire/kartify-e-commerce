

with source as (

    select * from {{ source('pg_raw_schema', 'inventory_df') }}

),

renamed as (

    select
        id,
        product_id,
        quantity,
        reorder_lvl,
        created_at,
        updated_at

    from source

)

select * from renamed

