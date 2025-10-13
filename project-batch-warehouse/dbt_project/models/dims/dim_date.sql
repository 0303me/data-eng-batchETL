{{ config(materialized='table') }}
with dates as (
  select
    d::date as date_day,
    extract(year from d) as year,
    extract(month from d) as month,
    extract(day from d) as day,
    strftime(d, '%Y-%m') as yyyymm,
    strftime(d, '%A') as weekday_name,
    extract(isodow from d) as isodow
  from generate_series(date '2018-01-01', date '2026-12-31', interval 1 day) t(d)
)
select * from dates