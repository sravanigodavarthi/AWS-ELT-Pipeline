import boto3
import botocore.exceptions
import sys

"""
Upload source data files to AWS S3 using the boto3 AWS SDK.
"""

# Define the files to be uploaded along with their bucket names and S3 keys
FILES_TO_UPLOAD = [
    {
        "bucket_name": "nyc-tlc-taxi-trip-data",
        "file_name": "/opt/airflow/data/NYC_TLC_trip_data_2024_02.parquet",
        "s3_key": "2024/raw_data/NYC_TLC_trip_data_2024_02.parquet"
    },
    {
        "bucket_name": "nyc-zone-lookup",
        "file_name": "/opt/airflow/data/taxi_zone_lookup.csv",
        "s3_key": "taxi_zone_lookup.csv"
    }
]

def main():
    """
    Main function to upload files to S3.
    Establishes connection to S3 and uploads files if the bucket exists or is created.
    """
    s3_conn = connect_to_s3()
    for file in FILES_TO_UPLOAD:
        if create_bucket_if_not_exists(s3_conn, file["bucket_name"]):
            upload_data_to_s3(s3_conn, file["file_name"], file["bucket_name"], file["s3_key"])

def connect_to_s3():
    """
    Connect to Amazon S3 using boto3.

    Returns:
        S3 resource object if connection is successful.

    Raises:
        SystemExit: If connection to S3 fails.
    """
    try:
        s3 = boto3.resource('s3')
        print("Connection to S3 established successfully")
        return s3
    except Exception as e:
        print(f"Cannot connect to S3: {e}")
        sys.exit(1)

def create_bucket_if_not_exists(s3, bucket_name):
    """
    Check if an S3 bucket exists, and create it if it does not.

    Args:
        S3 resource object.
        bucket_name (str): Name of the S3 bucket.
        
    Returns:
        bool: True if the bucket exists or is created successfully, otherwise False.
    """
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
        return True
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Bucket {bucket_name} does not exist. Creating bucket...")
            try:
                s3.create_bucket(Bucket=bucket_name)
                print(f"Bucket {bucket_name} created successfully")
                return True
            except botocore.exceptions.ClientError as e:
                print(f"Failed to create bucket: {e}")
                return False

def upload_data_to_s3(s3, file_name, bucket_name, s3_key):
    """
    Upload a file to S3 if it does not already exist.

    Args:
        S3 resource object.
        file_name (str): Path to the local file to be uploaded.
        bucket_name (str): Name of the S3 bucket.
        s3_key (str): S3 key for the uploaded file.
    """
    try:
        s3_client = boto3.client('s3')
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        print(f"File {s3_key} already exists")
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Uploading file {file_name} to {bucket_name}/{s3_key}...")
            try:
                s3.meta.client.upload_file(Filename=file_name, Bucket=bucket_name, Key=s3_key)
                print(f"File {file_name} uploaded successfully to {bucket_name}/{s3_key}.")
            except botocore.exceptions.ClientError as e:
                print(f"Failed to upload file: {e}")

if __name__ == "__main__":
    main()