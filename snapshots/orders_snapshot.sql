{% snapshot orders_snapshot %}

{{
    config(
      target_database='wer_snapshots',
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      invalidate_hard_deletes=True,
      updated_at="_etl_loaded_at"
      

    )
}}

select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}