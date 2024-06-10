from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from data_lake.upload_to_s3 import main
from data_quality.data_quality_checks import check_data_quality
from operators.custom_redshift_operator import CustomRedshiftOperator

"""
DAG to upload data to AWS S3, copy it to AWS Redshift, 
perform transformations, and execute data modeling in Redshift.
"""

default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 6, 4),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'depends_on_past': False
}

with DAG(
        dag_id= "nyc_taxi_dwh_dag",
        description='ELT pipeline to load and transform NYC taxi data in Redshift',
        schedule='@monthly',
        default_args=default_args,
        catchup=False,
        template_searchpath=['/opt/airflow/data_warehouse/sql_queries']
        ) as dag:
        
            start_operator = EmptyOperator(task_id='begin_execution')
            
            upload_source_data_to_s3 = PythonOperator(
                task_id='upload_source_data_to_s3',
                python_callable=main
            )

            create_tables = CustomRedshiftOperator(
                task_id='create_tables',
                redshift_conn_id="nyc_taxi_redshift",
                sql_file="create_tables.sql"
                                    )
        
            load_raw_data_to_redshift_from_s3 = S3ToRedshiftOperator(
                task_id = 'load_raw_data_from_s3',
                schema = 'raw_layer',
                table = 'raw_nyc_taxi_trip',
                s3_bucket = 'nyc-tlc-taxi-trip-data',
                s3_key = '2024/raw_data/NYC_TLC_trip_data_2024_02.parquet',
                redshift_conn_id='nyc_taxi_redshift',
                aws_conn_id = 'aws_default',
                copy_options = ['FORMAT AS PARQUET']
            )
            
            transform_data_and_load_to_staging = CustomRedshiftOperator(
                task_id='transform_data_and_load_to_staging',
                sql_file='/staging_layer/staging_nyc_taxi_trip.sql',
                redshift_conn_id='nyc_taxi_redshift'
            )
            
            load_dim_date_table = CustomRedshiftOperator(
                task_id='load_dim_date_table',
                sql_file='/presentation_layer/dims/dim_date.sql',
                redshift_conn_id='nyc_taxi_redshift'
            )
            
            load_dim_time_table = CustomRedshiftOperator(
                task_id='load_dim_time_table',
                sql_file='/presentation_layer/dims/dim_time.sql',
                redshift_conn_id='nyc_taxi_redshift'
            )
        
            load_dim_location_table = S3ToRedshiftOperator(
                task_id = 'load_dim_location_table',
                schema = 'presentation_layer',
                table = 'dim_location',
                s3_bucket = 'nyc-zone-lookup',
                s3_key = 'taxi_zone_lookup.csv',
                redshift_conn_id='nyc_taxi_redshift',
                aws_conn_id = 'aws_default',
                copy_options = ["FORMAT AS CSV",
                                "DELIMITER ','",
                                "IGNOREHEADER 1"]
            )
            
            load_dim_rate_code_table = CustomRedshiftOperator(
                task_id='load_dim_rate_code_table',
                sql_file='/presentation_layer/dims/dim_rate_code.sql',
                redshift_conn_id='nyc_taxi_redshift'
            )
            
            load_dim_payment_type_table = CustomRedshiftOperator(
                task_id='load_dim_payment_type_table',
                sql_file='/presentation_layer/dims/dim_payment_type.sql',
                redshift_conn_id='nyc_taxi_redshift'
            )
            
            load_fact_trip_table = CustomRedshiftOperator(
                task_id='load_fact_trip_table'
                ,sql_file='/presentation_layer/facts/fact_trip.sql'
                ,redshift_conn_id='nyc_taxi_redshift'
            )
            
            create_views = CustomRedshiftOperator(
                task_id='create_views'
                ,sql_file='/reporting_layer/create_views.sql'
                ,redshift_conn_id='nyc_taxi_redshift'
            )
            
            run_data_quality_checks = PythonOperator(
                task_id = 'run_data_quality_checks',
                op_kwargs = {
                        'tables': ['presentation_layer.dim_date', 
                        'presentation_layer.dim_location', 
                        'presentation_layer.fact_trip'
                        ],
                        'redshift_conn_id': 'nyc_taxi_redshift'
                },
                python_callable=check_data_quality
            )
            
            end_operator = EmptyOperator(task_id='stop_execution')
            
            # task dependencies
            
            start_operator >> upload_source_data_to_s3
            
            upload_source_data_to_s3 >> create_tables
            
            create_tables >> load_raw_data_to_redshift_from_s3
            
            load_raw_data_to_redshift_from_s3 >> transform_data_and_load_to_staging
            
            transform_data_and_load_to_staging >> [load_dim_date_table, 
                                                load_dim_time_table, 
                                                load_dim_location_table, 
                                                load_dim_rate_code_table, 
                                                load_dim_payment_type_table] 

            [load_dim_date_table, 
            load_dim_time_table, 
            load_dim_location_table, 
            load_dim_rate_code_table, 
            load_dim_payment_type_table]  >> load_fact_trip_table
            
            load_fact_trip_table >> create_views
            
            create_views >> run_data_quality_checks
            
            run_data_quality_checks >> end_operator
            

            