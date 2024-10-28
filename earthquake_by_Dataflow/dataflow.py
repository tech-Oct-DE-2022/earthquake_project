import json
from datetime import datetime
import re
from requests.exceptions import HTTPError
import logging
import os
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import apache_beam as beam
from apache_beam import DoFn, Row
import utils_dataflow
# import upload_to_gcs, fetch_api_data, read_from_gcs, flatten_feature

# Define the FetchAPIData class
class FetchAPIData(DoFn):
    def process(self, element):
        try:
            geojson_data = utils_dataflow.fetch_api_data(element)  # Assuming 'element' is a URL
            yield geojson_data
        except HTTPError as e:
            logging.error(f"HTTP error occurred: {e}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

class FlattenFeature(DoFn):
    def process(self, feature_collection):
        logging.info(f"Processing feature_collection of type: {type(feature_collection)}")
        if isinstance(feature_collection, str):
            feature_collection = json.loads(feature_collection)
        flattened_records = utils_dataflow.flatten_feature(feature_collection)
        for record in flattened_records:
            yield record

class ConvertTimestamp(DoFn):
    def process(self, element):
        record = {}
        if hasattr(element, 'time'):
            try:
                time_value = element.time
                if time_value is not None:
                    timestamp_value = float(time_value) if isinstance(time_value, str) else time_value
                    record['time'] = datetime.utcfromtimestamp(timestamp_value / 1000).strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                print(f"Error converting 'time': {e}")

        if hasattr(element, 'updated'):
            try:
                updated_value = element.updated
                if updated_value is not None:
                    timestamp_value = float(updated_value) if isinstance(updated_value, str) else updated_value
                    record['updated'] = datetime.utcfromtimestamp(timestamp_value / 1000).strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                print(f"Error converting 'updated': {e}")

        if hasattr(element, 'place'):
            place_value = element.place
            if place_value:
                pattern = r'of\s+(.*)'
                match = re.search(pattern, place_value)
                if match:
                    record['area'] = match.group(1).strip()

        for field in element.__fields__:
            if field not in record:
                record[field] = getattr(element, field)
        yield record

class AddInsertDate(beam.DoFn):
    def process(self, element):
        record = json.loads(element)
        record['insert_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        yield record


if __name__ == "__main__":
    options = PipelineOptions()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\Demp_Pycharm\GCP\gcp_sessions\my-bwt-learning-2024-a6b2387d3aeb.json"
    google_cloud_option = options.view_as(GoogleCloudOptions)
    google_cloud_option.project = '"my-bwt-learning-2024"'
    google_cloud_option.region = 'bw-project-432203'
    google_cloud_option.job_name = 'historicaldata1'
    google_cloud_option.staging_location = 'gs://bwtp1/staging_location'
    google_cloud_option.temp_location = 'gs://bwtp1/temp_location'

    # Pulling data from the valid URL
    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'

    current_date = datetime.now().strftime("%Y%m%d")  # Format the date as YYYYMMDD

    # Step 1: Fetch GeoJSON data from the API
    with beam.Pipeline(options=options) as p1:
        api_urls = [url]
        geojson_data = (
            p1
            | "Create API URLs" >> beam.Create(api_urls)
            | "Fetch API Data" >> beam.ParDo(FetchAPIData())
        )
        output_path_1 = f'gs://daily_data_earth/bronze_daily_data/dataflow/{current_date}.json'
        upload_result_1 = (
            geojson_data
            | 'Format as JSON' >> beam.Map(lambda x: json.dumps(x))
            | 'Write to GCS' >> beam.io.WriteToText(output_path_1, file_name_suffix='.json', num_shards=1)
        )

    # Step 2: Read from GCS and process data
    input_path_2 = f'gs://daily_data_earth/bronze_daily_data/dataflow/{current_date}.*'
    with beam.Pipeline(options=options) as p2:
        read_result = (
            p2
            | 'Read from GCS' >> beam.io.ReadFromText(input_path_2)
        )
        flattened_data = (
            read_result
            | "Flatten Data" >> beam.ParDo(FlattenFeature())
        )
        processed_data = flattened_data | 'ConvertTimestamps' >> beam.ParDo(ConvertTimestamp())
        output_path_2 = f'gs://daily_data_earth/daily_silver_data/dataflow/{current_date}.json'
        upload_result_2 = (
            processed_data
            | 'Format as JSON' >> beam.Map(lambda x: json.dumps(x))
            | 'Write to GCS' >> beam.io.WriteToText(output_path_2, file_name_suffix='.json', num_shards=1)
        )

    # Step 3: Read from GCS and write to BigQuery
    input_path_3 = f'gs://daily_data_earth/daily_silver_data/dataflow/{current_date}.*'
    with beam.Pipeline(options=options) as p3:
        read_result_3 = (
            p3
            | 'Read from GCS2' >> beam.io.ReadFromText(input_path_3)
            | 'Add Insert Date' >> beam.ParDo(AddInsertDate())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                'my-bwt-learning-2024:earthquake_dataset.earthquake_data',
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )