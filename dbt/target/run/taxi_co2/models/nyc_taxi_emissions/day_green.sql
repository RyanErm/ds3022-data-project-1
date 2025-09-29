
  
  create view "emissions"."main"."day_green__dbt_tmp" as (
    SELECT
  *,
  CASE dayofweek(lpep_pickup_datetime)
    WHEN 0 THEN 'Sunday'
    WHEN 1 THEN 'Monday'
    WHEN 2 THEN 'Tuesday'
    WHEN 3 THEN 'Wednesday'
    WHEN 4 THEN 'Thursday'
    WHEN 5 THEN 'Friday'
    WHEN 6 THEN 'Saturday'
  END AS day_of_week
FROM green_trip_data
  );
