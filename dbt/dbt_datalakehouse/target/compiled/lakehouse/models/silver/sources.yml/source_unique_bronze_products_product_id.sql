
    
    

select
    product_id as unique_field,
    count(*) as n_records

from bronze.sales.products
where product_id is not null
group by product_id
having count(*) > 1


