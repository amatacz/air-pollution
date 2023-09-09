import os
import requests
import json
import datetime
from dotenv import load_dotenv
from gcloud import storage


load_dotenv("code/secrets/.env")

API_KEY = os.environ["API_KEY"]
GCLOUD_PROJECT = os.environ.setdefault(
    "GCLOUD_PROJECT", "phonic-vortex-398110"
    )

cities = {"Poznan": "PL", "Gdansk": "PL", "Verona": "IT",
          "Milan": "IT", "Genoa": "IT", "Monte-Carlo": "MC"}


def cities_data_retrieve(
        cities: dict = cities, API_KEY: str = API_KEY
        ) -> dict:
    """
    Takes city and country code values from provided dictionary,
    retrieves data from OpenWeather Geocoding API URL
    and stores data in cities_dictionary.
    """

    cities_dictionary = {}
    cities_dictionary['cities'] = []
    for city_name, country_code in cities.items():
        try:
            url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{country_code}&appid={API_KEY}"
            response = requests.get(url)
            cities_dictionary['cities'].append(response.json())
        except Exception as e:
            print(e)

    return cities_dictionary


def cities_data_to_json(
        cities_dictionary: dict
        ) -> None:
    """
    Saves dictionary with cities data to JSON file.
    """

    with open("code/data/cities_data.json", "w", encoding='UTF-8') as f:
        json.dump(cities_dictionary, f, indent=2)


def coord_extraction(
        cities_json_file: str
        ) -> list:
    """
    Extracts cities name, latitude and longitude from cities data JSON file
    and saves it to coordination list.
    """

    coord_list = []
    with open(cities_json_file) as f:
        cities_data = json.load(f)

    for i in range(0, len(cities_data['cities'])):
        try:
            coord_list.append({
                'city': cities_data['cities'][i][0]['name'],
                'lat': cities_data['cities'][i][0]['lat'],
                'lon': cities_data['cities'][i][0]['lon']
                })
        except Exception as e:
            print(e)
    return coord_list


def air_pollution_retrieve(
        coordinates_list: list, API_KEY: str = API_KEY
        ) -> dict:
    """
    Takes latitude and longitude values from coordinates list
    and retrieves air pollution for each city using
    Open Weather Air Pollution API URL.
    Stores data in air pollution dictionary.
    """

    air_pollution_dict = {}
    for pair in coordinates_list:
        try:
            lat = pair['lat']
            lon = pair['lon']
            url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
            air_pollution_dict[pair['city']] = requests.get(url).json()
        except Exception as e:
            print(e)
    return air_pollution_dict


def air_pollution_data_to_json(
        air_pollution_data: dict,
        file: str = "code/data/air_pollution_data.json"
        ) -> None:
    """
    Saves air pollution dictionary to air pollution JSON file.
    """

    with open(file, "w") as f:
        json.dump(air_pollution_data, f, indent=2)


def historical_air_pollution_data_retrieve(
        start_date: str, end_date: str,
        coordinates_list: list, API_KEY: str = API_KEY,
        ) -> dict:
    """
    Takes start and end dates ('%d-%m-%Y %H:%M:%S' format)
    and retrieves historical air pollution data for cities from Coordination List.
    Stores it in dictionary.
    """

    historical_air_pollution_data_dict = {}

    formatted_start_date = datetime.datetime.strptime(start_date, "%d-%m-%Y %H:%M:%S")
    unix_start_date = int(datetime.datetime.timestamp(formatted_start_date))
    formatted_end_date = datetime.datetime.strptime(end_date, "%d-%m-%Y %H:%M:%S")
    unix_end_date = int(datetime.datetime.timestamp(formatted_end_date))

    for pair in coordinates_list:
        try:
            lat = pair['lat']
            lon = pair['lon']
            url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={unix_start_date}&end={unix_end_date}&appid={API_KEY}"
            historical_air_pollution_data_dict[pair['city']] = requests.get(url).json()
        except Exception as e:
            print(e)

    return historical_air_pollution_data_dict


def historical_air_pollution_data_to_json(
        historicalair_pollution_data: dict,
        file: str = "code/data/historical_air_pollution_data.json"
        ) -> None:
    """
    Saves air pollution dictionary to air pollution JSON file.
    """

    with open(file, "w") as f:
        json.dump(historicalair_pollution_data, f, indent=2)


def upload_data_to_bucket():
    """
    Uploads files with api data to GCP buckets.
    """

    client = storage.Client.from_service_account_json(
        "code/secrets/google_cloud_service_key.json"
        )

    air_pollution_bucket = client.bucket("air_pollution_bucket_amatacz")
    air_pollution_blob = air_pollution_bucket.blob(
        "air_pollution_data.json"
    )
    with open("code/data/air_pollution_data.json", "rb") as f:
        air_pollution_blob.upload_from_file(f)

    historical_air_pollution_blob = air_pollution_bucket.blob(
        "historical_air_pollution_data.json"
    )
    with open("code/data/historical_air_pollution_data.json", "rb") as f:
        historical_air_pollution_blob.upload_from_file(f)

    openweather_bucket = client.bucket("openweather_bucket_amatacz")
    openweather_blob = openweather_bucket.blob("cities_data.json")
    with open("code/data/cities_data.json", "rb") as f:
        openweather_blob.upload_from_file(f)
