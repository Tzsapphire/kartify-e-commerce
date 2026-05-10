

with source as (

    select * from {{ source('pg_raw_schema', 'products_df') }}

),

renamed as (

    select
        id,
        category_id,
        name,
        slug,
        description,
        price,
        image_url,
        is_active,
        created_at,
        updated_at

    from source

)

select * from renamed

