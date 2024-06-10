import logging
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

def check_data_quality(tables, redshift_conn_id):
    """
    Check data quality in specified tables in AWS Redshift.

    Args:
        tables (list): List of table names to check.
        redshift_conn_id (str): Connection ID for AWS Redshift.

    Raises:
        ValueError: If any table returns no results.
    """
    redshift_hook = RedshiftSQLHook(redshift_conn_id = redshift_conn_id)
    for table in tables:
        # Check that entries are being copied to table
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if not records or not records[0] or records[0][0] < 1:
            logging.error(f"Data quality check failed. {table} returned no results")
            raise ValueError(f"Data quality check failed. {table} returned no results")
        logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")