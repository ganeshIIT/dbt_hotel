{{  save_history(
        input_rel = ref('stg_raw_reservations'),
        key_column = 'reservation_hkey',
        diff_column = 'reservation_hdiff',
        load_ts_column = 'updated',

        high_watermark_column = 'updated'
) }}


{# {{
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

{% endif %} #}
