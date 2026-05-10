

with source as (

    select * from {{ source('pg_raw_schema', 'products_df') }}

),

renamed as (

    select
        id as product_id,
        category_id,
        lower(trim(name)) as product_name,
        lower(trim(slug)) as product_slug,
        trim(description) as product_description,
        price,
        nullif((image_url), 'None') as image_url,
        is_active,
        created_at,
        updated_at

    from source

)

select * from renamed

