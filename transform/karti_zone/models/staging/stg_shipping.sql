

with source as (

    select * from {{ source('pg_raw_schema', 'shipping_df') }}

),

renamed as (

    select
        id as shipping_id,
        order_id,
        trim(lower(status)),
        nullif(trim(carrier), 'None') as carrier, 
        nullif(trim(tracking_number), 'None') as tracking_number,
        trim(address),
        initcap(city),
        initcap(state),
        initcap(trim(country)),
        nullif(postal_code, 'None'),
        shipped_at,
        delivered_at,
        created_at,
        updated_at

    from source

)

select * from renamed

