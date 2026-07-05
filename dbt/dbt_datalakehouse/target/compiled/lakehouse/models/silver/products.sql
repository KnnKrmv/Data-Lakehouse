

with source as (

    select *
    from bronze.sales.products

),

deduped as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by product_id
                order by updated_at desc
            ) as _rn
        from source
    )
    where _rn = 1

)

select
    cast(product_id as integer)            as product_id,
    product_name,
    category,
    brand,
    color,
    size,
    cast(unit_price as decimal(10,2))      as unit_price,
    cast(cost_price as decimal(10,2))      as cost_price,
    unit_of_measure,
    cast(is_active as boolean)             as is_active,
    cast(created_at as timestamp)          as created_at,
    cast(updated_at as timestamp)          as updated_at
from deduped
where product_id is not null