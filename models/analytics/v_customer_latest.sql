{{
    config(
        materialized='view'
    )
}}

with customers as (
    select * from {{ ref('hist_customers') }} where to_date = '9999-12-31'
)

select * from customers