-- dbt run-operation generate_base_model --args '{"source_name": "raw1", "table_name": "inventory_df"}'


with source as (

    select * from {{ source('pg_raw_schema', 'inventory_df') }}

),

renamed as (

    select
        id as inventory_id,
        product_id,
        quantity,
        reorder_lvl as reorder_level,
        created_at,
        updated_at

    from source

)

select * from renamed
