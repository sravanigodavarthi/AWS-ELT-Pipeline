INSERT INTO staging_layer.staging_nyc_taxi_trip(
                dropoff_datetime,
                pickup_datetime,
                passenger_count,
                trip_distance,
                pickup_location_id,
                dropoff_location_id,
                rate_code_id,
                payment_type,
                fare_amount,
                total_amount
                )
                SELECT 
                    DISTINCT
                    tpep_dropoff_datetime AS dropoff_datetime,
                    tpep_pickup_datetime AS pickup_datetime,
                    passenger_count,
                    trip_distance,
                    PULocationID AS pickup_location_id,
                    DOLocationID AS dropoff_location_id,
                    RateCodeID AS rate_code_id,
                    payment_type,
                    fare_amount,
                    total_amount
                FROM raw_layer.raw_nyc_taxi_trip
                WHERE tpep_dropoff_datetime > tpep_pickup_datetime
                AND RateCodeID BETWEEN 1 AND 6
                AND payment_type BETWEEN 1 AND 6
                AND fare_amount IS NOT NULL 
                AND total_amount IS NOT NULL
                LIMIT 10000;