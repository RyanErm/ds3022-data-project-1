SELECT lpep_pickup_datetime,
week(lpep_pickup_datetime)
AS week_of_year
FROM green_trip_data;