

with source as (

    select * from {{ source('pg_raw_schema', 'categories_df') }}

),

renamed as (

    select
        id,
        name,
        slug,
        is_active,
        created_at,
        updated_at

    from source

)

select * from renamed

