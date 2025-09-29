SELECT
    yellow_table_data.trip_distance,
    (yellow_table_data.trip_distance * (
        SELECT co2_grams_per_mile
        FROM {{ ref('emissions_data') }}
        WHERE vehicle_type = 'yellow_taxi'
    )) / 1000 AS trip_co2_kgs
FROM {{ ref('yellow_table_data') }};