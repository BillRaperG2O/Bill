    select
        id,
        orderid as order_id,
        paymentmethod,
        status,
        ({{ cents_to_dollars('amount',4) }}) as amount,
        created as created_at,
        _batched_at

    from {{ source('stripe', 'payment') }}

