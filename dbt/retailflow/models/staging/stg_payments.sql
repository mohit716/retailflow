select
    order_id,
    payment_type,
    CAST(payment_installments AS INTEGER) as payment_installments,
    CAST(payment_value AS NUMERIC) as payment_value
from {{ source('raw', 'raw_payments') }}
where order_id is not null

