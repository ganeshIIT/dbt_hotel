with
reservations as (
    select
        reservation_data:RESERVATIONID::varchar as reservationid,
        reservation_data:CUSTOMERID::varchar as customerid,
        reservation_data:DATEBOOKED::timestamp as datebooked,
        reservation_data:NIGHTSCHARGED::int as nightscharged,
        reservation_data:STAYFROM::date as stayfrom,
        reservation_data:STAYTO::date as stayto,
        reservation_data:STATUS::varchar as status,
        updated
    from {{ source('raw', 'reservations') }}
),

hashed as (
    select
            {{ dbt_utils.generate_surrogate_key(["RESERVATIONID"]) }} as reservation_hkey,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "CUSTOMERID",
                        "DATEBOOKED",
                        "NIGHTSCHARGED",
                        "STAYFROM",
                        "STAYTO",
                        "STATUS",
                        "updated",
                    ]
                )
            }} as reservation_hdiff,
            *
from reservations
)

select *
from hashed
