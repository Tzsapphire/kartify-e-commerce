

with source as (

    select * from {{ source('pg_raw_schema', 'payments_df') }}

),

renamed as (

    select
        id as payment_id,
        order_id,
        lower(trim(method)) as payment_method,
        lower(trim(status)) as payment_status,
        amount,
        nullif(trim(reference), '') as payment_reference,
        created_at,
        updated_at

    from source

)

select * from renamed
