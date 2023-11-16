with
customers as (
    select
        customer_data:CUSTOMERID::varchar as customerid,
        customer_data:FIRSTNAME::varchar as firstname,
        customer_data:LASTNAME::varchar as lastname,
        customer_data:GENDER::varchar as gender,
        customer_data:COUNTRY::varchar as country,
        customer_data:CITY::varchar as city,
        customer_data:ZIPCODE::varchar as zipcode,
        customer_data:DATEOFBIRTH::date as dateofbirth,
        updated
    from {{ source('raw', 'customers') }}
),

hashed as (
    select
            {{ dbt_utils.generate_surrogate_key(["customerid"]) }} as customer_hkey,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "firstname",
                        "lastname",
                        "gender",
                        "country",
                        "city",
                        "zipcode",
                        "dateofbirth",
                        "updated",
                    ]
                )
            }} as customer_hdiff,
            *
from customers
)

select *
from hashed
