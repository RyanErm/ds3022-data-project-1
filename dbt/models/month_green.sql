SELECT
  lpep_pickup_datetime,
  CASE month(lpep_pickup_datetime)
    WHEN 1 THEN ''
    WHEN 2 THEN ''
    WHEN 3 THEN ''
    WHEN 4 THEN ''
    WHEN 5 THEN ''
    WHEN 6 THEN ''
    WHEN 7 THEN ''
    WHEN 8 THEN ''
    WHEN 9 THEN ''
    WHEN 10 THEN ''
    WHEN 11 THEN ''
    WHEN 12 THEN ''
  END AS month_of_year
FROM green_trip_data;