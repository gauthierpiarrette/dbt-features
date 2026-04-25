with dates as (
    select distinct order_date as feature_date from {{ ref('stg_orders') }}
),
customers as (
    select distinct customer_id from {{ ref('stg_orders') }}
),
grid as (
    select c.customer_id, d.feature_date
    from customers c cross join dates d
)
select
    g.feature_date,
    g.customer_id,
    coalesce(count(o.order_id), 0) as orders_count_7d,
    coalesce(count(o.order_id), 0) > 1 as is_repeat_customer
from grid g
left join {{ ref('stg_orders') }} o
  on o.customer_id = g.customer_id
 and o.order_date between g.feature_date - interval 7 day and g.feature_date
group by 1, 2
