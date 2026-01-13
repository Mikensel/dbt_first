--select * from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" limit 10;
--select count (*) from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet";


/* select VendorID, count(*) as trips_count
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
group by VendorId;

select payment_type, count(*) as trips_count
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
group by payment_type;

select RatecodeId, count (*) as trips_count
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
group by RatecodeId;

select store_and_fwd_flag, count (*) as trips_count
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
group by store_and_fwd_flag; */
/* 
select PULocationID, count (*) as trips_count
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
group by PULocationID; */

--select DOLocationID, count (*) as trips_count
--from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
--group by DOLocationID;

/* select  count(*) as trips_count
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
where tpep_pickup_datetime > tpep_dropoff_datetime;

select *
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
where tpep_pickup_datetime > tpep_dropoff_datetime 
Limit 10; */

/* select  count(*) as trips_count
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
where trip_distance <= 0;

select  *
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
where trip_distance < 0
limit 10;

select  *
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
where trip_distance =0
limit 10; */

/*select  count(*) as trips_count
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
where total_amount <= 0;

select *
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
where total_amount = 0
limit 10;

select *
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
where total_amount < 0
limit 10;*/

select * exclude(VendorID, RatecodeID)
from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
limit 10 ;
