{{ config(severity = 'warn') }}


select customerid, reservationid, stayfrom, stayto
from {{ ref('stg_raw_reservations') }}
where status = 'active'
qualify stayfrom < lag(stayto, 1, stayfrom -1) over(partition by customerid order by stayfrom)

