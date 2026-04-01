select
    order_id,
    payment_type,
    payment_installments::int as payment_installments,
    payment_value::numeric as payment_value
from raw_payments
where order_id is not null
