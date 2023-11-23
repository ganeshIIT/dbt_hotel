WITH reservations AS (
    SELECT
        *,
        FALSE AS is_reverse
    FROM
        {{ ref('hist_reservations') }}
        -- where customerid = 2895
),
reverse_facts AS (
    SELECT
        reservationid,
        customerid,
        datebooked,
        status,
        updated,
        LAG(status) over(PARTITION BY customerid, reservationid ORDER BY updated) AS previous_status,
        LAG(nightscharged) over(PARTITION BY customerid, reservationid ORDER BY updated) AS previous_nights_charged,
        CASE
            WHEN previous_status = 'active' AND status = 'cancelled' THEN nightscharged * -2
            WHEN previous_status = 'cancelled' AND status = 'cancelled' THEN nightscharged * -1
            WHEN previous_status = 'active' AND status = 'active' THEN previous_nights_charged - nightscharged
            ELSE NULL
        END AS rb_nights_charged,
        TRUE AS is_reverse
    FROM
        reservations -- where customerid = 2895
        qualify rb_nights_charged IS NOT NULL
),
combined AS (
    SELECT
        reservationid,
        customerid,
        datebooked,
        status,
        updated AS asat_date,
        nightscharged,
        is_reverse
    FROM
        reservations
    UNION ALL
    SELECT
        reservationid,
        customerid,
        datebooked,
        status,
        updated,
        rb_nights_charged,
        is_reverse
    FROM
        reverse_facts
),
hashed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["RESERVATIONID"]) }} AS reservation_hkey,
        {{ dbt_utils.generate_surrogate_key(
            [ "CUSTOMERID", "DATEBOOKED", "NIGHTSCHARGED", "STATUS", "ASAT_DATE", "IS_REVERSE" ]
        ) }} AS reservation_hdiff,*
    FROM
        combined
)
SELECT
    *,
    {{ dbt_housekeeping() }}
FROM
    hashed --where customerid = 2895
ORDER BY
    reservationid,
    asat_date
