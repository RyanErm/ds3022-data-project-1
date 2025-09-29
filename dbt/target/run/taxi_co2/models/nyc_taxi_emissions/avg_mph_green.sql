
  
  create view "emissions"."main"."avg_mph_green__dbt_tmp" as (
    SELECT *,
trip_distance * 3600 / (date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime)) 
AS avg_mph 
FROM green_trip_data
  );
