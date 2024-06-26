INSERT INTO presentation_layer.dim_date(
            date_key, 
            date, 
            day_of_month, 
            day_of_week, 
            date_of_week_name, 
            month, 
            year, 
            is_weekend
            )
            WITH combined_dates AS
            (
                    SELECT 
                        DATE_TRUNC('day', pickup_datetime) AS date
                    FROM
                        staging_layer.staging_nyc_taxi_trip
                    UNION ALL
                    SELECT
                        DATE_TRUNC('day', dropoff_datetime) AS date
                    FROM
                        staging_layer.staging_nyc_taxi_trip
                )
            SELECT
                DISTINCT
                TO_CHAR(date, 'YYYYMMDD')::INT AS date_key,
                date,
                EXTRACT(DAY FROM date) AS day_of_month,
                EXTRACT(DOW FROM date) AS day_of_week,
                CASE
                    WHEN EXTRACT(DOW FROM date) = 0 THEN 'Sunday'
                    WHEN EXTRACT(DOW FROM date) = 1 THEN 'Monday'
                    WHEN EXTRACT(DOW FROM date) = 2 THEN 'Tuesday'
                    WHEN EXTRACT(DOW FROM date) = 3 THEN 'Wednesday'
                    WHEN EXTRACT(DOW FROM date) = 4 THEN 'Thursday'
                    WHEN EXTRACT(DOW FROM date) = 5 THEN 'Friday'
                    WHEN EXTRACT(DOW FROM date) = 6 THEN 'Saturday'
                END AS date_of_week_name,
                EXTRACT(MONTH FROM date) AS month,
                EXTRACT(YEAR FROM date) AS year,
                CASE
                    WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE
                    ELSE FALSE
                END AS is_weekend
            FROM
                combined_dates;