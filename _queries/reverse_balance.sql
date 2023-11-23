with reservations as (
select *, false as is_reverse
from hist_reservations
-- where customerid = 2895 
),
reverse_facts as (
select 
reservationid, customerid,
status, updated,
lag(status) over(partition by customerid, reservationid order by updated) as previous_status,
lag(nightscharged) over(partition by customerid, reservationid order by updated) as previous_nights_charged,
case 
    when previous_status = 'active' and status = 'cancelled' then nightscharged * -2 
    when previous_status = 'cancelled' and status = 'cancelled' then nightscharged * -1
    when previous_status = 'active' and status = 'active' then previous_nights_charged-nightscharged 
else null
end as rb_nights_charged
, true as is_reverse
from hotel_dbt.gl.hist_reservations 
-- where customerid = 2895 
qualify rb_nights_charged is not null

),
combined as (
select reservationid, customerid, 
status, updated as asat_date, nightscharged, is_reverse
from reservations 
union all
select reservationid, customerid, 
status, updated, rb_nights_charged, is_reverse
from reverse_facts 
)

select * from combined
--where customerid = 2895
order by reservationid, asat_date;



--total number of parts sold
-- SELECT SUM(nightscharged) FROM combined where customerid = 2895;
-- parts sold on day two (delta)
-- SELECT SUM(nightscharged) FROM combined where customerid = 2895 and asat_date <= '2023-11-21 03:15:54.569';



-- parts sold on first of Jan as at day one
SELECT SUM(nightscharged) FROM combined
WHERE asat_date< '2023-11-21 03:15:54.569'
and customerid = 2895;
-- AND is_reverse = false;

