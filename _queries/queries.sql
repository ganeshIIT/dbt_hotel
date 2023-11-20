select * from hotel_dbt.gl.hist_reservations;

select * from HOTEL_DBT.GL.V_CUSTOMER_LATEST;
select * from HOTEL_DBT.GL.V_RESERVATION_LATEST;    


select * from hotel_prod.raw.reservations;

create schema hotel_dbt.test;

create table hotel_dbt.test.test_reservations
clone hotel_prod.raw.reservations;

select object_insert(reservation_data, 'STATUS', 'cancelled', true) from hotel_dbt.test.test_reservations;

select * from hotel_dbt.gl.hist_reservations;
select max(dbt_batch_utc) from hotel_dbt.gl.hist_reservations;
select max(from_date) from hotel_dbt.gl.hist_customers;

select * from hotel_dbt.snapshots.snapshot_customers;

