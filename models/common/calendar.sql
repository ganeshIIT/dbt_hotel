{{
    config(
        materialized='incremental',
        unique_key='DATE_DAY',
        on_schema_change='fail'
    )
}}


WITH RECURSIVE CalendarDates AS (
SELECT DATEFROMPARTS(2023 -11, 1, 1) AS calendar_date
UNION ALL
SELECT DATEADD(DAY, 1, calendar_date)
FROM CalendarDates
WHERE calendar_date < DATEADD(YEAR, 11, DATEFROMPARTS(2023, 1, 1)) - 1
)

SELECT
calendar_date AS DATE_DAY
,YEAR(calendar_date) AS YEAR
,MONTH(calendar_date) AS MONTH
,MONTHNAME(calendar_date) AS MONTHNAME
,DAY(calendar_date) AS DAY
,DAYNAME(calendar_date) AS DAYNAME
,QUARTER(calendar_date) AS QUARTER
,WEEK(calendar_date) AS WEEK

,DAYOFMONTH(calendar_date) AS DAYOFMONTH
,DAYOFWEEK(calendar_date) AS DAYOFWEEK
,DAYOFYEAR(calendar_date) AS DAYOFYEAR

,LAST_DAY (calendar_date) AS LAST_DAY_OF_MONTH
,LAST_DAY (calendar_date, 'QUARTER') AS LAST_DAY_OF_QUARTER
,LAST_DAY (calendar_date, 'YEAR') AS LAST_DAY_OF_YEAR
,LAST_DAY (calendar_date, 'WEEK') AS LAST_DAY_OF_WEEK

,WEEKOFYEAR(calendar_date) AS WEEKOFYEAR
,YEAROFWEEK(calendar_date) AS YEAROFWEEK
,YEAROFWEEKISO(calendar_date) AS YEAROFWEEKISO
,WEEKISO(calendar_date) AS WEEKISO
,DAYOFWEEKISO(calendar_date) AS DAYOFWEEKISO

,CEIL(DAYOFMONTH(calendar_date)/7) AS WEEK_OF_MONTH

,CASE
WHEN QUARTER(calendar_date) = 1 THEN DATEFROMPARTS(YEAR(calendar_date), 1, 1)
WHEN QUARTER(calendar_date) = 2 THEN DATEFROMPARTS(YEAR(calendar_date), 4, 1)
WHEN QUARTER(calendar_date) = 3 THEN DATEFROMPARTS(YEAR(calendar_date), 7, 1)
ELSE DATEFROMPARTS(YEAR(calendar_date), 10, 1)
END AS FIRST_DAY_OF_QUARTER
,DATEDIFF (DAY, FIRST_DAY_OF_QUARTER, DATE_DAY) + 1 AS DAY_OF_QUARTER
,DATEDIFF (DAY, FIRST_DAY_OF_QUARTER, LAST_DAY_OF_QUARTER) + 1 AS DAYS_IN_QUARTER
,CEIL(DAY_OF_QUARTER/7) AS WEEK_OF_QUARTER
,{{ dbt_housekeeping() }}

FROM CalendarDates


{% if is_incremental() %}

  where DATE_DAY > (select max(DATE_DAY) from {{ this }})
  or DATE_DAY < (select min(DATE_DAY) from {{ this }})

{% endif %}