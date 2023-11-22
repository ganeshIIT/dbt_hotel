select * from hotel_dbt.gl.hist_reservations;

select * from HOTEL_DBT.GL.V_CUSTOMER_LATEST;
select * from HOTEL_DBT.GL.V_RESERVATION_LATEST;    


select * from hotel_prod.raw.reservations where reservation_data:RESERVATIONID 
in (153208) ;

create schema hotel_dbt.test;

create table hotel_dbt.test.test_reservations
clone hotel_prod.raw.reservations;

select object_insert(reservation_data, 'STATUS', 'cancelled', true) from hotel_dbt.test.test_reservations;

select * from hotel_dbt.gl.hist_reservations;
select max(dbt_batch_utc) from hotel_dbt.gl.hist_reservations;
select max(from_date) from hotel_dbt.gl.hist_customers;

select * from hotel_dbt.snapshots.snapshot_customers;

drop schema hotel_dbt.snapshots;
drop schema hotel_prod.snapshots


select customerid, count(*) from hotel_dbt.gl.hist_reservations group by all order by 2 desc;

select * from hotel_dbt.gl.hist_customers where customerid in (4862,6639);

select * from hotel_dbt.gl.hist_reservations where customerid = 6639 order by stayfrom;

select * from hotel_dbt.gl.hist_reservations where customerid = 7574 order by stayfrom;

select customerid, reservationid, stayfrom, stayto,
stayfrom < lag(stayto, 1, stayfrom -1) over(partition by customerid order by stayfrom) as bin
from hotel_dbt.gl.hist_reservations where customerid = 6639;

select customerid, reservationid, stayfrom, stayto
from hotel_dbt.gl.hist_reservations
where status = 'active'
qualify stayfrom < lag(stayto, 1, stayfrom -1) over(partition by customerid order by stayfrom);

select * from hotel_dbt.gl.hist_customers where customerid in (6639);

select *, customer_data:CUSTOMERID from hotel_prod.raw.customers where customer_data:CUSTOMERID in (6639);



select * from hotel_dbt.snapshots.snapshot_customers where customerid = 6639; 

select * from hotel_dbt.gl.hist_customers where customerid = 6639; 

select * from hotel_dbt.gl.obt_reservations where customerid = 6639;



---old
update hotel_prod.raw.customers 
set customer_data = object_insert(customer_data, 'CITY', 'Des Moines', true), updated = '2020-01-01 00:00:00.000'
where customer_data:CUSTOMERID in (6639);


update hotel_prod.raw.reservations 
set reservation_data = object_insert(reservation_data, 'STATUS' ,'active', true), 
updated = '2020-01-01 00:00:00.000' 
--select *, reservation_data:RESERVATIONID from hotel_prod.raw.reservations 
where reservation_data:RESERVATIONID 
in (
 173557
,176482
,174254
,153502
,135415
,153698
,192258
,192680
,175053
,175100
,178810
,135706
,174707
,191634
,154002
,153208
,171739
,188489
,194295
,120578
,174100
,127715
,135851
,128728
,191289
,134805
,192173
,193822
,191126
,193707
);




--new
update hotel_prod.raw.customers 
set customer_data = object_insert(customer_data, 'CITY', 'New York', true), updated = current_timestamp()
where customer_data:CUSTOMERID in (6639);

update hotel_prod.raw.reservations 
set reservation_data = object_insert(reservation_data, 'STATUS' ,'cancelled', true), 
updated = current_timestamp() 
--select *, reservation_data:RESERVATIONID from hotel_prod.raw.reservations 
where reservation_data:RESERVATIONID 
in (
173557
,176482
,174254
,153502
,135415
,153698
,192258
,192680
,175053
,175100
,178810
,135706
,174707
,191634
,154002
,153208
,171739
,188489
,194295
,120578
,174100
,127715
,135851
,128728
,191289
,134805
,192173
,193822
,191126
,193707
);

select r.reservationid
from hotel_dbt.gl.stg_raw_reservations r
where status = 'active'
qualify stayfrom < lag(stayto, 1, stayfrom -1) over(partition by customerid order by stayfrom);




select dbt_batch_utc, count(*) from hotel_dbt.gl.hist_reservations group by all;
select * from hotel_dbt.gl.hist_reservations where reservationid= 153208;

select status, count(*) from hotel_dbt.gl.hist_reservations group by all;


-- delete from gl.hist_reservations where updated > '2020-01-01 00:00:00.000';

select * from hotel_dbt.gl.obt_reservations where reservationid= 153208;

select customerid, reservationid, stayfrom, stayto
from hotel_dbt.gl.stg_raw_reservations
where status = 'active'
qualify stayfrom < lag(stayto, 1, stayfrom -1) over(partition by customerid order by stayfrom);

select customerid, reservationid, stayfrom, stayto
from hotel_dbt.gl.hist_reservations
where status = 'active'
qualify stayfrom < lag(stayto, 1, stayfrom -1) over(partition by customerid order by stayfrom);
