select
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) as ordered_at,
    CAST(order_delivered_customer_date AS TIMESTAMP) as delivered_at,
    CAST(order_estimated_delivery_date AS TIMESTAMP) as estimated_delivery_at
from `retailflow-492517.retailflow.raw_orders`
where order_id is not null
