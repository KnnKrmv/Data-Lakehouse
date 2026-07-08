
  
    

    create table gold.sales.product_performance__dbt_tmp
      
      
    as (
      

with txn as (
    select * from silver.sales.transactions
),

product as (
    select * from silver.sales.products
),

agg as (
    select
        product_id,
        sum(quantity)   as total_quantity_sold,
        count(*)        as total_transactions,
        sum(net_amount) as total_revenue
    from txn
    group by product_id
)

select
    agg.product_id,
    product.product_name,
    product.category,
    product.brand,
    product.color,
    product.unit_price,
    agg.total_quantity_sold,
    agg.total_transactions,
    agg.total_revenue
from agg
left join product on agg.product_id = product.product_id
    );

  