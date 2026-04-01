select
    order_id,
    product_id,
    seller_id,
    price::numeric as price,
    freight_value::numeric as freight_value
from raw_order_items
where order_id is not null
