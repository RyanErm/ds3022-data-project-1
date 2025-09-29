SELECT tpep_pickup_datetime,
hour(tpep_pickup_datetime)
AS hour_of_day
FROM yellow_trip_data;