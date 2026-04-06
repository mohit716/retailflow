select
    order_id,
    payment_type,
    CAST(payment_installments AS INT64) as payment_installments,
    CAST(payment_value AS NUMERIC) as payment_value
from `retailflow-492517.retailflow.raw_payments`
where order_id is not null
