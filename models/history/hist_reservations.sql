{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}

with reservations as (
    select
        *,
        {{ dbt_housekeeping() }}
    from {{ ref('stg_raw_reservations') }}
)

select * from reservations
{% if is_incremental() %}

  where updated > (select max(updated) from {{ this }})

{% endif %}
