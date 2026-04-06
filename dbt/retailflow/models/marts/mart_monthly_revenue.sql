select
    date_trunc(ordered_at, MONTH) as month,
    count(order_id) as total_orders,
    round(CAST(sum(total_order_value) AS NUMERIC), 2) as total_revenue,
    round(CAST(avg(total_order_value) AS NUMERIC), 2) as avg_order_value
from {{ ref('fct_orders') }}
where order_status = 'delivered'
group by 1
order by 1
