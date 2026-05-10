

with source as (

    select * from {{ source('pg_raw_schema', 'payments_df') }}

),

renamed as (

    select
        id,
        order_id,
        method,
        status,
        amount,
        reference,
        created_at,
        updated_at

    from source

)

select * from renamed

