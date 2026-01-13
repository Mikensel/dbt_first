{{config(
    materialized='external',
    location='./output/trips_2025_transformed.parquet',
    format ='parquet'
)}}

with source_data as (
    select * exclude (VendorID, RatecodeID)
    from {{ source( 'tlc_taxi_trips' , 'raw_yellow_trip_data_2025' )}}
), 
filtered_data as (
    select *
    from source_data
    where 
    passenger_count > 0
    and trip_distance > 0
    and total_amount > 0
    and tpep_pickup_datetime < tpep_dropoff_datetime
    and store_and_fwd_flag = 'N'
    and tip_amount > 0
    and payment_type in (1,2)
),

transformed_data as (
    select 
    cast (passenger_count as bigint) as passenger_count,
    case
    when payment_type = 1 then 'credit_card'
    when payment_type = 2 then 'cash'
    end as payment_method,

    datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes,

    * exclude (passenger_count, payment_type)
    from filtered_data
),

final_data as (
    select *,
        cast(tpep_pickup_datetime as date) as pickup_date,
        cast(tpep_dropoff_datetime as date) as dropoff_date
    from transformed_data
    WHERE
        pickup_date  >= '2025-01-01' AND pickup_date  < '2026-01-01'
        AND dropoff_date >= '2025-01-01' AND dropoff_date < '2026-01-01'

)
select * exclude (pickup_date, dropoff_date)
from final_data
where trip_duration_minutes > 0
