select
    order_id,
    customer_id,
    cast(order_ts as date) as order_date,
    amount
from {{ ref('raw_orders') }}
