SELECT tpep_dropoff_datetime, tpep_pickup_datetime, trip_distance, 
trip_distance * 3600 / (date_part('second', tpep_pickup_datetime) - date_part('second', tpep_dropoff_datetime)) 
AS avg_mph 
FROM yellow_trip_data;