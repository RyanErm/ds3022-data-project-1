
  
  create view "emissions"."main"."avg_mph_yellow__dbt_tmp" as (
    SELECT *, 
trip_distance * 3600 / (date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime))  
AS avg_mph 
FROM yellow_trip_data
  );
