with customers as (
    select * from {{ ref('snapshot_customers') }}
)

select
    customer_hkey,
    customer_hdiff,
    customerid,
    firstname,
    lastname,
    gender, 
    country,
    city,
    zipcode,
    dateofbirth,
    updated,
    dbt_updated_at,
    dbt_valid_from as from_date,
    nvl2(dbt_valid_to, dateadd('s', -1, dbt_valid_to), '9999-12-31') as to_date,
    {{ dbt_housekeeping() }}
from customers
