

with source as (

    select * from {{ source('pg_raw_schema', 'categories_df') }}

),

renamed as (

    select
        id as category_id, --ensure no duplicates
        initcap(trim(name)) as category_name,
        lower(slug) as category_slug, --ensure no duplicates
        is_active as cat_is_active,
        created_at,
        updated_at

    from source

)

select * from renamed

