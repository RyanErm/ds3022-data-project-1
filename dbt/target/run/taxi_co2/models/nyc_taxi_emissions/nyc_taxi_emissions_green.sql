
  
  create view "emissions"."main"."nyc_taxi_emissions_green__dbt_tmp" as (
    SELECT
    green_trip_data.trip_distance, green_trip_data.passenger_count, green_trip_data.lpep_dropoff_datetime, green_trip_data.lpep_pickup_datetime,
    (green_trip_data.trip_distance * emissions_data.co2_grams_per_mile) / 1000 AS trip_co2_kgs, 
    week(lpep_pickup_datetime) AS week_of_year, 
    hour(lpep_pickup_datetime) AS hour_of_day, 
    trip_distance * 3600 / (date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime)) AS avg_mph, 
    CASE dayofweek(lpep_pickup_datetime)
      WHEN 0 THEN 'Sunday'
      WHEN 1 THEN 'Monday'
      WHEN 2 THEN 'Tuesday'
      WHEN 3 THEN 'Wednesday'
      WHEN 4 THEN 'Thursday'
      WHEN 5 THEN 'Friday'
      WHEN 6 THEN 'Saturday'
    END AS day_of_week, 
    CASE month(lpep_pickup_datetime)
      WHEN 1 THEN 'January'
      WHEN 2 THEN 'February'
      WHEN 3 THEN 'March'
      WHEN 4 THEN 'April'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'June'
      WHEN 7 THEN 'July'
      WHEN 8 THEN 'August'
      WHEN 9 THEN 'September'
      WHEN 10 THEN 'October'
      WHEN 11 THEN 'November'
      WHEN 12 THEN 'December'
    END AS month_of_year
FROM green_trip_data
JOIN emissions_data
  ON emissions_data.vehicle_type = 'green_taxi'
  );
