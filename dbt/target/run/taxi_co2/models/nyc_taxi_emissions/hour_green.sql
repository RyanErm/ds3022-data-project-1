
  
  create view "emissions"."main"."hour_green__dbt_tmp" as (
    SELECT *,
hour(lpep_pickup_datetime)
AS hour_of_day
FROM green_trip_data
  );
