

with source as (

    select * from {{ source('pg_raw_schema', 'reviews_df') }}

),

renamed as (

    select
        id as review_id,
        product_id,
        customer_id,
        rating,
        nullif(trim(comment), 'None') as comment,
        created_at,
        updated_at

    from source

)

select * from renamed

