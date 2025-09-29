
  
  create view "emissions"."main"."hour_yellow__dbt_tmp" as (
    SELECT *,
hour(tpep_pickup_datetime)
AS hour_of_day
FROM yellow_trip_data
  );
