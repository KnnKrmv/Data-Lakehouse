{{ config(alias='customers') }}

with source as (

    select *
    from {{ source('bronze', 'customers') }}

),

deduped as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by customer_id
                order by updated_at desc
            ) as _rn
        from source
    )
    where _rn = 1

)

select
    cast(customer_id as integer)           as customer_id,
    first_name,
    last_name,
    full_name,
    cast(birth_date as date)               as birth_date,
    gender,
    email,
    phone,
    country,
    city,
    address,
    cast(registration_date as timestamp)   as registration_date,
    customer_segment,
    cast(is_active as boolean)             as is_active,
    cast(created_at as timestamp)          as created_at,
    cast(updated_at as timestamp)          as updated_at
from deduped
where customer_id is not null
