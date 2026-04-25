select
    customer_id,
    count(*) as lifetime_order_count
from {{ ref('stg_orders') }}
group by 1
