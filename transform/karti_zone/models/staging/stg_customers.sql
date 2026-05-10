

with source as (

    select * from {{ source('pg_raw_schema', 'customers_df') }}

),

renamed as (

    select
        id as customer_id,
        lower(trim(first_name)) as first_name,
        lower(trim(last_name)) as last_name,
        concat(trim(first_name), ' ',trim(last_name)) as full_name,
        lower(trim(email)) as email,
        nullif(trim(phone), 'None') as phone,
        nullif(lower(trim(address)), 'None') as address,
        nullif(lower(city), 'None') as city,
        nullif(lower(state), 'None') as state,
        nullif(lower(country), 'None') as country,
        nullif(trim(postal_code), 'None') as postal_code,
        password,
        is_active,
        created_at,
        updated_at

    from source

)

select * from renamed

