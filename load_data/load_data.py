import requests
from google.cloud import storage
from datetime import datetime
import os

from utils import fetch_api_data

if __name__ == '__main__':

    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r'D:\Demp_Pycharm\GCP\gcp_sessions\my-bwt-learning-2024-a6b2387d3aeb.json'

    project_id='my-bwt-learning-2024'
# Set up Google Cloud Storage client
storage_client = storage.Client()
bucket_name = 'earthquake_analysis_bucket1'  # Replace with your bucket name
bucket = storage_client.bucket(bucket_name)


# Fetch earthquake data (monthly data)
url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
response = requests.get(url)
data = fetch_api_data(url)
print(data)

# if response.status_code == 200:
#     data = response.json()
#     # Store data to GCS
#     filename = f"raw_data_{datetime.now().strftime('%Y%m%d')}.json"
#     blob = bucket.blob(f'raw/{filename}')
#     blob.upload_from_string(response.text)
#     print(f'File uploaded to GCS: {filename}')
# else:
#     print(f'Failed to fetch data: {response.status_code}')
#
#
