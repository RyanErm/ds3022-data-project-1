SELECT *, 
trip_distance * 3600 / (date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime))  
AS avg_mph 
FROM yellow_trip_data