with src as (
  select * from {{ source('bronze', 'yellow_trips_parquet') }}
),
renamed as (
  select
    cast(VendorID as int) as vendor_id,
    cast(tpep_pickup_datetime as timestamp) as pickup_ts,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,
    cast(PULocationID as int) as pu_location_id,
    cast(DOLocationID as int) as do_location_id,
    cast(passenger_count as int) as passenger_count,
    cast(trip_distance as double) as trip_distance_mi,
    cast(fare_amount as double) as fare_amount,
    cast(total_amount as double) as total_amount,
    cast(tip_amount as double) as tip_amount,
    cast(payment_type as int) as payment_type,
    cast(RatecodeID as int) as rate_code_id
  from src
),
dedup as (
  -- Some TLC months have duplicates; rough dedup by all columns + window
  select *,
    row_number() over (partition by vendor_id, pickup_ts, dropoff_ts, pu_location_id, do_location_id, fare_amount order by pickup_ts) as rn
  from renamed
)
select * exclude(rn)
from dedup
where rn = 1
and pickup_ts is not null
and dropoff_ts is not null