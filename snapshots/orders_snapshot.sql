{% snapshot orders_snapshot %}

{{
    config(
      target_database='wer_snapshots',
      target_schema='snapshots',
      unique_key='id',

      strategy='check',
      check_cols=['user_id','order_date','status','_etl_loaded_at']

    )
}}

select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}