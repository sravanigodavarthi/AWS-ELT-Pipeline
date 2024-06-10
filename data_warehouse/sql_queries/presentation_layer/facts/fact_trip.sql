INSERT INTO presentation_layer.fact_trip(
                pickup_date_key,
                dropoff_date_key,
                pickup_time_key,
                dropoff_time_key,
                pickup_location_key,
                dropoff_location_key,
                trip_duration,
                trip_distance,
                passenger_count,
                rate_code_id,
                payment_type,
                fare_amount,
                total_amount
            )
            SELECT
                TO_CHAR(pickup_datetime, 'YYYYMMDD')::INT AS pickup_date_key,
                TO_CHAR(dropoff_datetime, 'YYYYMMDD')::INT AS dropoff_date_key,
                EXTRACT(HOUR FROM pickup_datetime)::SMALLINT AS pickup_time_key,
                EXTRACT(HOUR FROM dropoff_datetime)::SMALLINT AS dropoff_time_key,
                pickup_location_id AS pickup_location_key,
                dropoff_location_id AS dropoff_location_key,
                EXTRACT(EPOCH FROM dropoff_datetime - pickup_datetime)::INT AS trip_duration,
                trip_distance,
                passenger_count,
                rate_code_id,
                payment_type,
                fare_amount,
                total_amount
            FROM
                staging_layer.staging_nyc_taxi_trip;