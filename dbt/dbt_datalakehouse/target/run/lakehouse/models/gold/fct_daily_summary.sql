
  
    

    create table gold.sales.daily_summary__dbt_tmp
      
      
    as (
      

with txn as (
    select * from silver.sales.transactions
)

select
    cast(transaction_date as date)                                    as metric_date,
    count(*)                                                          as total_transactions,
    sum(net_amount)                                                   as total_revenue,
    sum(quantity)                                                     as total_quantity,
    sum(case when upper(status) = 'CANCELLED' then 1 else 0 end)      as cancelled_count,
    sum(case when upper(status) = 'PENDING' then 1 else 0 end)        as pending_count,
    round(sum(net_amount) * 1.0 / nullif(count(*), 0), 2)             as avg_order_value
from txn
group by cast(transaction_date as date)
    );

  