from dotenv import load_dotenv
import os
import requests
import json

load_dotenv("code/secrets/.env")

API_KEY = os.environ["API_KEY"]

cities = {"Poznan": "PL", "Gdansk": "PL", "Verona": "IT",
          "Milan": "IT", "Genoa": "IT", "Monte-Carlo": "MC"}


def cities_data_retrieve(cities: dict = cities, API_KEY: str = API_KEY) -> dict:
    cities_dictionary = {}
    cities_dictionary['cities'] = []
    for city_name, country_code in cities.items():
        url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{country_code}&appid={API_KEY}"
        response = requests.get(url)
        cities_dictionary['cities'].append(response.json())

    return cities_dictionary


def cities_data_to_json(cities_dictionary: dict) -> None:
    with open("cities_coordinates.json", "w", encoding='UTF-8') as f:
        json.dump(cities_dictionary, f, indent=2)


def coord_extraction(cities_json_file: str) -> list:
    coord_list = []
    with open(cities_json_file) as f:
        cities_data = json.load(f)

    for i in range(0, len(cities_data['cities'])):
        coord_list.append({
            'city': cities_data['cities'][i][0]['name'],
            'lat': cities_data['cities'][i][0]['lat'],
            'lon': cities_data['cities'][i][0]['lon']
            })
    return coord_list


def air_pollution_retrieve(coordinates_list: list, API_KEY: str = API_KEY) -> dict:
    air_pollution_dict = {}
    for pair in coordinates_list:
        lat = pair['lat']
        lon = pair['lon']
        url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
        air_pollution_dict[pair['city']] = requests.get(url).json()
    return air_pollution_dict


def air_pollution_data_to_json(air_pollution_data: dict, file: str = "air_pollution_data.json") -> None:
    with open(file, "w") as f:
        json.dump(air_pollution_data, f, indent=2)
