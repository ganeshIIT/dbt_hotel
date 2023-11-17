{{
    config(
        materialized='view'
    )
}}

with

customers as (
    select * from {{ ref('hist_customers') }}
),

reservations as (
    select * from {{ ref('hist_reservations') }}
),

calendar as (
    select * from {{ ref('calendar') }}
),


obt as (

    select
        c.customer_hkey,
        c.customer_hdiff,
        c.customerid,
        c.firstname,
        c.lastname,
        c.gender,
        c.country,
        c.city,
        c.zipcode,
        c.dateofbirth,
        -- c.updated,
        c.dbt_updated_at,
        c.from_date,
        c.to_date,
        r.reservation_hkey,
        r.reservation_hdiff,
        r.reservationid,
        r.datebooked,
        r.nightscharged,
        r.stayfrom,
        r.stayto,
        r.status,
        r.updated,
        ca.year as booked_year,
        ca.month as booked_month,
        ca.monthname as booked_monthname,
        ca.day as booked_day,
        ca.dayname as booked_dayname,
        ca.quarter as booked_quarter,
        ca.week as booked_week,
        ca.dayofmonth as booked_dayofmonth,
        ca.dayofweek as booked_dayofweek,
        ca.dayofyear as booked_dayofyear,
        ca.weekofyear as booked_weekofyear,
        ca.week_of_month as booked_week_of_month,
        ca.week_of_quarter as booked_week_of_quarter
    from customers as c
    inner join reservations as r
        on
            c.customerid = r.customerid
            and r.datebooked between c.from_date and c.to_date
    inner join calendar as ca on ca.date_day = cast(r.datebooked as date)
)

select * from obt
