SELECT lpep_dropoff_datetime, lpep_pickup_datetime, trip_distance, 
trip_distance * 3600 / (date_part('second', lpep_pickup_datetime) - date_part('second', lpep_dropoff_datetime)) 
AS avg_mph 
FROM green_trip_data;