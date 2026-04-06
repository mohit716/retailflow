select
    c.customer_state,
    count(o.order_id) as total_orders,
    round(CAST(sum(o.total_order_value) AS NUMERIC), 2) as total_revenue
from {{ ref('fct_orders') }} o
left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
group by 1
order by 2 desc
