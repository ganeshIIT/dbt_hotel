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
            {{ dbt_utils.generate_surrogate_key(["RESERVATIONID"]) }} as position_hkey,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "RESERVATIONID",
                        "CUSTOMERID",
                        "DATEBOOKED",
                        "NIGHTSCHARGED",
                        "STAYFROM",
                        "STAYTO",
                        "STATUS",
                        "updated",
                    ]
                )
            }} as position_hdiff,
            *,
            '{{ run_started_at }}' as load_ts_utc
from reservations
)

select *
from hashed
