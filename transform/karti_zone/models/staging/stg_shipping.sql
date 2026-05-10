

with source as (

    select * from {{ source('pg_raw_schema', 'shipping_df') }}

),

renamed as (

    select
        id as shipping_id,
        order_id,
        trim(lower(status)) as status,
        nullif(trim(carrier), 'None') as carrier, 
        nullif(trim(tracking_number), 'None') as tracking_number,
        trim(address) as shipping_address,
        initcap(city) as city,
        initcap(state) as state,
        initcap(trim(country)) as country,
        nullif(postal_code, 'None') as postal_code,
        shipped_at,
        delivered_at,
        created_at,
        updated_at

    from source

)

select * from renamed

