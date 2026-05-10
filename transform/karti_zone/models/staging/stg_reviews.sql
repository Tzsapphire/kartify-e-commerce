

with source as (

    select * from {{ source('pg_raw_schema', 'reviews_df') }}

),

renamed as (

    select
        id,
        product_id,
        customer_id,
        rating,
        comment,
        created_at,
        updated_at

    from source

)

select * from renamed

