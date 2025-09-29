SELECT
    green_table_data.trip_distance,
    (green_table_data.trip_distance * (
        SELECT co2_grams_per_mile
        FROM {{ ref('emissions_data') }}
        WHERE vehicle_type = 'green_taxi'
    )) / 1000 AS trip_co2_kgs
FROM {{ ref('green_table_data') }};