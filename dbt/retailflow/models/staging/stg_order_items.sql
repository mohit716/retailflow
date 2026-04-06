select
    order_id,
    product_id,
    seller_id,
    CAST(price AS NUMERIC) as price,
    CAST(freight_value AS NUMERIC) as freight_value
from `retailflow-492517.retailflow.raw_order_items`
where order_id is not null
