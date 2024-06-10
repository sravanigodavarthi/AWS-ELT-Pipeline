INSERT INTO presentation_layer.dim_time (
            time_key, 
            hour, 
            time_of_day
            )
            WITH combined_times AS
            (
                    SELECT
                        EXTRACT(HOUR FROM pickup_datetime)::SMALLINT AS hour
                    FROM
                        staging_layer.staging_nyc_taxi_trip
                    UNION ALL
                    SELECT
                        EXTRACT(HOUR FROM dropoff_datetime)::SMALLINT AS hour
                    FROM
                        staging_layer.staging_nyc_taxi_trip
                )
            SELECT
                DISTINCT 
                hour as time_key,
                hour,
                CASE
                    WHEN hour BETWEEN 6 AND 11 THEN 'Morning'
                    WHEN hour BETWEEN 12 AND 16 THEN 'Afternoon'
                    WHEN hour BETWEEN 17 AND 20 THEN 'Evening'
                    when hour BETWEEN 21 AND 23 THEN 'Night'
                    when hour BETWEEN 0 AND 1 THEN 'Late Night'
                    ELSE 'Early Morning'
                END AS time_of_day
            FROM
                combined_times;