with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from bill_dbt_training.dbt_braper.stg_orders
    group by status

)

select *
from all_values
where value_field not in (
    'completed','shipped','return_pending'
)