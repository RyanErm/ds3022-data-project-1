SELECT *,
week(tpep_pickup_datetime)
AS week_of_year
FROM yellow_trip_data