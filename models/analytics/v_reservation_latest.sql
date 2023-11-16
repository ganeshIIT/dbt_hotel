{{
    config(
        materialized='view'
    )
}}


with reservations as (
    select * from {{ ref('hist_reservations') }}
    qualify row_number() over(partition by reservationid order by updated desc) = 1
)

select * from reservations