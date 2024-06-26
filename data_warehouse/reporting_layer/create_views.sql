CREATE SCHEMA IF NOT EXISTS reporting_layer;

CREATE OR REPLACE VIEW reporting_layer.pickup_date(
    pickup_date_key,
    pickup_date,
    pickup_day_of_month,
    pickup_day_of_week,
    pickup_date_of_week_name,
    pickup_month,
    pickup_year,
    pickup_is_weekend
    )
    AS
    SELECT
    date_key,
    date,
    day_of_month,
    day_of_week,
    date_of_week_name,
    month,
    year,
    is_weekend
    FROM
    presentation_layer.dim_date;

CREATE OR REPLACE VIEW reporting_layer.dropoff_date(
                dropoff_date_key,
                dropoff_date,
                dropoff_day_of_month,
                dropoff_day_of_week,
                dropoff_date_of_week_name,
                dropoff_month,
                dropoff_year,
                dropoff_is_weekend
            )
            AS
            SELECT
                date_key,
                date,
                day_of_month,
                day_of_week,
                date_of_week_name,
                month,
                year,
                is_weekend
            FROM
                presentation_layer.dim_date;

CREATE OR REPLACE VIEW reporting_layer.pickup_time(
                pickup_time_key,
                pickup_hour,
                pickup_time_of_day
            )
            AS
            SELECT
                time_key,
                hour,
                time_of_day
            FROM
                presentation_layer.dim_time;

CREATE OR REPLACE VIEW reporting_layer.dropoff_time(
                dropoff_time_key,
                dropoff_hour,
                dropoff_time_of_day
            )
            AS
            SELECT
                time_key,
                hour,
                time_of_day
            FROM
                presentation_layer.dim_time;


CREATE OR REPLACE VIEW reporting_layer.pickup_location(
                pickup_location_key,
                pickup_borough,
                pickup_zone,
                pickup_service_zone
            )
            AS
            SELECT
                LocationID,
                borough,
                zone,
                service_zone
            FROM
                presentation_layer.dim_location;

CREATE OR REPLACE VIEW reporting_layer.dropoff_location(
                dropoff_location_key,
                dropoff_borough,
                dropoff_zone,
                dropoff_service_zone
            )
            AS
            SELECT
                LocationID,
                borough,
                zone,
                service_zone
            FROM
                presentation_layer.dim_location;
