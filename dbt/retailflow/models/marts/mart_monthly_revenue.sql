select
    date_trunc('month', ordered_at) as month,
    count(order_id) as total_orders,
    round(sum(total_order_value)::numeric, 2) as total_revenue,
    round(avg(total_order_value)::numeric, 2) as avg_order_value
from {{ ref('fct_orders') }}
where order_status = 'delivered'
group by 1
order by 1
