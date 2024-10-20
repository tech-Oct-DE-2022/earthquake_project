import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import requests
from datetime import datetime

# Replace with your GCP project ID
project_id = 'your-project-id'

# Define Dataflow pipeline options (adjust as needed)
options = PipelineOptions([
    '--runner=DataflowRunner',
    f'--project={project_id}',  # Use your GCP project ID
    '--job_name=earthquake',  # Set a descriptive job name
    '--staging_location=gs://your-bucket-name/staging',  # Replace with your GCS bucket for staging
    '--temp_location=gs://your-bucket-name/temp',  # Replace with your GCS bucket for temporary files
    '--region=your-desired-region',  # Choose your preferred Dataflow region
    '--num_workers=4',  # Adjust the number of workers for parallelization
])


class FetchEarthquakeData(beam.DoFn):
    """
    DoFn to fetch earthquake data from the USGS API.
    """

    def process(self, element):
        url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
        response = requests.get(url)
        if response.status_code == 200:
            yield response.json()  # Yield the JSON data as a dictionary
        else:
            raise Exception(f"Failed to fetch data from URL: {response.status_code}")


class WriteToGCS(beam.DoFn):
    """
    DoFn to write earthquake data to Google Cloud Storage.
    """

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def process(self, data):
        # Initialize GCS client
        from google.cloud import storage

        storage_client = storage.Client()

        # Define GCS bucket and file path
        bucket = storage_client.bucket(self.bucket_name)
        filename = f"raw_data_{datetime.now().strftime('%Y%m%d')}.json"
        blob = bucket.blob(f'raw/{filename}')

        # Write the data to GCS
        blob.upload_from_string(str(data), content_type='application/json')  # Ensure data is JSON string
        yield f"File uploaded to GCS: raw/{filename}"


def run(argv=None):
    # Replace with your GCS bucket name for storing earthquake data
    bucket_name = 'your-earthquake-data-bucket'

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Start' >> beam.Create([None])  # Create an initial PCollection with a single element
            | 'Fetch Earthquake Data' >> beam.ParDo(FetchEarthquakeData())  # Fetch earthquake data
            | 'Write to GCS' >> beam.ParDo(WriteToGCS(bucket_name))  # Write the fetched data to GCS
        )


if __name__ == '__main__':
    run()