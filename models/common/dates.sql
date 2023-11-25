{{
  config(
    materialized = 'incremental',
    unique_key='DATE_DAY',
    )
}}

{{ dbt_date.get_date_dimension("2015-01-01", "2025-12-31") }}