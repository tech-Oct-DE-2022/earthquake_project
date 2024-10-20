import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import requests
import os

# Set up Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'D:\Demp_Pycharm\GCP\gcp_sessions\my-bwt-learning-2024-a6b2387d3aeb.json'

project_id = 'my-bwt-learning-2024'


# DoFn to fetch earthquake data from the URL
class FetchEarthquakeData(beam.DoFn):
    def process(self, element):
        url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
        response = requests.get(url)
        if response.status_code == 200:
            yield response.text  # Yield the JSON data as a string
        else:
            raise Exception(f"Failed to fetch data from URL: {response.status_code}")


# DoFn to write data to Google Cloud Storage
class WriteToGCS(beam.DoFn):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def process(self, data):
        from google.cloud import storage
        from datetime import datetime

        # Initialize GCS client
        storage_client = storage.Client()

        # Define GCS bucket and file path
        bucket = storage_client.bucket(self.bucket_name)
        filename = f"raw_data_{datetime.now().strftime('%Y%m%d')}.json"
        blob = bucket.blob(f'raw/{filename}')

        # Write the data to GCS
        blob.upload_from_string(data, content_type='application/json')
        yield f"File uploaded to GCS: raw/{filename}"


def run(argv=None):
    # Set your GCS bucket name
    bucket_name = 'earthquake_analysis_bucket1'

    # Set up Beam pipeline options
    options = PipelineOptions([
        '--runner=DataflowRunner',
        '--project=my-bwt-learning-2024',
        '--job_name=earthquake',
        '--staging_location=gs://my-bwt-session-2024/stage_loc',
        '--temp_location=gs://my-bwt-session-2024/Temp_loc',
        '--region=asia-east1',
        '--num_workers=4'

    ])

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Start' >> beam.Create([None])  # Create an initial PCollection with a single element
            | 'Fetch Earthquake Data' >> beam.ParDo(FetchEarthquakeData())  # Fetch earthquake data
            | 'Write to GCS' >> beam.ParDo(WriteToGCS(bucket_name))  # Write the fetched data to GCS
        )


if __name__ == '__main__':
    # Execute the pipeline
    run()
