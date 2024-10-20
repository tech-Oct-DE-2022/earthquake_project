from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col
import os


def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """Download a file from GCS to the local system."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Downloaded {source_blob_name} to {destination_file_name}")


if __name__ == '__main__':
    # Set Google Cloud credentials environment variable
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = r'D:\Demp_Pycharm\GCP\gcp_sessions\my-bwt-learning-2024-a6b2387d3aeb.json'

    # GCS configuration
    bucket_name = 'earthquake_analysis_bucket1'
    gcs_file_path = 'raw/raw_data_20241019.json'  # GCS file path (without 'gs://')

    # Local path on your system (use raw string or forward slashes)
    local_file_path = r'C:\Users\DELL\Desktop\aaa\Other imp'  # Ensure directory exists

    # Download the file from GCS to local storage
    download_from_gcs(bucket_name, gcs_file_path, local_file_path)

    # Create Spark session
    spark = SparkSession.builder.appName("EarthquakeData").getOrCreate()

    # Load the data from the local file
    df = spark.read.json(local_file_path)

    # Flatten nested JSON structure
    df_flattened = df.select(
        col('properties.mag').alias('magnitude'),
        col('properties.place').alias('location'),
        from_unixtime(col('properties.time') / 1000).alias('timestamp'),  # Convert epoch to timestamp
        col('properties.url').alias('url'),
        col('geometry.coordinates')[0].alias('longitude'),
        col('geometry.coordinates')[1].alias('latitude'),
        col('geometry.coordinates')[2].alias('depth')
    )

    # Show a preview of the flattened data
    df_flattened.show()
