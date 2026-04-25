select
    feature_date,
    sum(orders_count_7d) as total_orders_7d
from {{ ref('customer_features_daily') }}
group by 1
