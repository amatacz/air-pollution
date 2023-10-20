import functions_framework
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # add gcloud_functions
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # add gcloud


from shared.models.openweather_transformator import OpenWeatherDataIngestor
from shared.utils import DataConfigurator


@functions_framework.http
def gcloud_get_openweather_data_function(request, context=None) -> dict:
    '''
    Run a loop through all required cities to extract data 
    and return dictionary with all city data
    '''
    # Data Extract -> Transform Object
    OpenWeatherDataIngestorObject = OpenWeatherDataIngestor()

    # Utils object
    DataConfiguratorObject = DataConfigurator()

    # data placeholder
    all_city_data = {}

    for city in DataConfiguratorObject.load_cities_from_yaml():
        # get lon and lat for city
        coord_data = OpenWeatherDataIngestorObject.get_city_coordinates(city['name'], city['country_code'])
        # get air polluution_data for city
        air_pollution_data = OpenWeatherDataIngestorObject.get_city_air_pollution_data(coord_data['lat'], coord_data['lon'])
        historical_air_pollution = OpenWeatherDataIngestorObject.get_city_air_pollution_history_data(coord_data['lat'], coord_data['lon'], 1696320000, 1696356000)  # Timestamp podane na 3-10-2023 8-18, na próbę
        city_weather_data = OpenWeatherDataIngestorObject.get_city_weather_data(coord_data['city_name'], coord_data['country_code'])

        # append data placeholder
        all_city_data[city['name']] = coord_data
        all_city_data[city['name']]['air_pollution'] = air_pollution_data
        all_city_data[city['name']]['history_air_pollution'] = historical_air_pollution
        all_city_data[city['name']]['current_weather'] = city_weather_data

    return all_city_data


gcloud_get_openweather_data_function('x', 'x')
