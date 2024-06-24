![Star Badge](https://img.shields.io/static/v1?label=%F0%9F%8C%9F&message=If%20Useful&style=style=flat&color=BC4E99)
![Open Source Love](https://badges.frapsoft.com/os/v1/open-source.svg?v=103)

## Project Description:
The New York City Taxi and Limousine Commission (TLC) wants to analyze the taxi trip data every month to understand what customers do and what they need. This will help them make better decisions to improve their services and make rides better for passengers.

The project’s goal is to create an ELT pipeline that can manage a large database of taxi trip records, set to update automatically with new data each month. This system will also include data modeling, which helps to provide actionable insights quickly and easily.

### Data Details: 
The dataset is sourced from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
* **Trip Record Data:** The February 2024 dataset, “NYC_TLC_trip_data_2024_02.parquet,” contains around 3 million yellow taxi trip records. Each record has 19 details about the trip, like pick-up and drop-off timestamps, pick-up and drop-off locations, trip distances, breakdown of fares, types of rates, methods of payment, and driver-reported passenger counts. The detailed explanation of this data is provided in the data dictionary. https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

* **Geographical Details**: The “taxi_zone_lookup.csv” provides key location details such as service zones, taxi zones, and boroughs. This information is important in understanding patterns of travel and areas of high demand, thereby contributing to a more comprehensive analysis.

# NYC Taxi - Data Warehouse on AWS <img src="assets/NYC_taxi.png" align="right" width="100" />

<br>
<div align = center>
<img src="assets/architecture.png" align="center" width="1000" />

</div>

## Data Model for Taxi Trip Analysis
Data modeling plays a crucial role in defining the structure of a data warehouse by designing schemas that make data easier to retrieve and analyze.

The data model below is a Star Schema with Role-Playing Dimensions, a common design approach in Kimball's dimensional modeling. This includes a central fact table (fact_trip) and surrounding dimension tables (dim_date, dim_time, dim_location).
<br>
<div align = center>
<img src="assets/schema.png" align="center" width="800" />

</div>

**Grain:** The `fact_trip` table's grain represents the most detailed level of data, with each row corresponding to a single trip.

**Fact Table:** The fact_trip table is designed to store measurable and quantitative metrics that adhere to the defined grain, enabling a wide range of analysis and reporting.

* **Additive Facts:** `fare_amount`, `tip_amount`, and `total_amount` are sourced directly from the data and can be summed across various dimensions.
* **Derived Facts:** `trip_duration` is derived from the difference between pickup_datetime and dropoff_datetime, representing the length of the trip.

**Dimension Tables:** These tables provide context and descriptive attributes for the data in the fact table:
  * **Date Dimension**: `dim_date` contains unique date entries with attributes like day_of_month, day_of_week, etc.
  * **Time Dimension:** `dim_time` maintains unique entries per hour. The time and date dimensions are kept separate for granular analysis and storage efficiency.
  * **Location Dimension:** `dim_location` includes geographic data such as zone and borough.
  * **Payment and Rate Dimensions:** `dim_payment_type` and `dim_rate_code` hold static values as defined by the data dictionary.
  
**Relationships:** The PK (Primary Key) in dimension tables and FK (Foreign Key) in the fact table establish relationships, allowing for complex queries and analysis across different dimensions.

**Role play dimensions:** These dimensions allow a single physical dimension to be used multiple times in different contexts, each time playing a different “role.”

In this model `dim_date`, `dim_time`, and `dim_location` serve as role-playing dimensions, providing context for both pickup and drop off events. 

For example: - **Pickup Date**: Indicates when the trip began. - **Dropoff Date**: Signifies when the trip concluded.
      
Rather than using separate physical tables for pickup and dropoff details, which could lead to data redundancy, employing a single physical table for each dimension and creating views for each role (e.g., `pickup_date`, `dropoff_date`, `pickup_time`, `dropoff_time`) as shown below is a more efficient approach.
<div align="center">
  <img src="assets/date_view.png" align="center" width="400" />
  <img src="assets/location_view.png" align="center" width="400" />
  <img src="assets/time_view.png" align="center" width="400" />
</div>
This method ensures data integrity, minimizes storage requirements, and improves query performance. Role-playing dimensions are connected to the fact table through different foreign keys, reflecting their different roles in each context.

## ELT pipeline
The ETL pipeline is implemented through an Airflow DAG, which automates and monitors the data flow through the extraction, loading, and transformation phases.
