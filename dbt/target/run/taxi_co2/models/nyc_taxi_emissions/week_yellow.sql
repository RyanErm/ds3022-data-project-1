
  
  create view "emissions"."main"."week_yellow__dbt_tmp" as (
    SELECT *,
week(tpep_pickup_datetime)
AS week_of_year
FROM yellow_trip_data
  );
