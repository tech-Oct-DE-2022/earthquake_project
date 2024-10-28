import requests

def fetch_api_data(url):
    """" This method will fetch data from API"""

    response = requests.get(url)
    return response.json()