
  
    

    create table gold.sales.customer_metrics__dbt_tmp
      
      
    as (
      

with txn as (
    select * from silver.sales.transactions
),

customer as (
    select * from silver.sales.customers
),

agg as (
    select
        customer_id,
        min(transaction_date)                                 as first_purchase_date,
        max(transaction_date)                                 as last_purchase_date,
        count(*)                                              as total_orders,
        sum(net_amount)                                       as total_spent,
        round(sum(net_amount) * 1.0 / nullif(count(*), 0), 2) as avg_order_value
    from txn
    group by customer_id
)

select
    agg.customer_id,
    customer.full_name,
    customer.country,
    customer.city,
    customer.customer_segment,
    agg.first_purchase_date,
    agg.last_purchase_date,
    agg.total_orders,
    agg.total_spent,
    agg.avg_order_value
from agg
left join customer on agg.customer_id = customer.customer_id
    );

  