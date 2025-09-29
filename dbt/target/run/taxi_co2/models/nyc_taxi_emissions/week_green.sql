
  
  create view "emissions"."main"."week_green__dbt_tmp" as (
    SELECT *,
week(lpep_pickup_datetime)
AS week_of_year
FROM green_trip_data
  );
