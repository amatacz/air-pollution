import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta


class OpenWeatherDataExtractor:

    def __init__(self):
        load_dotenv('secrets/.env')
        self.openweather_api_key = os.environ['OPENWEATHER_API_KEY']

        ''' Air Pollution URLs'''
        self.AIR_POLLUTION_URL = 'http://api.openweathermap.org/data/2.5/air_pollution?'
        self.AIR_POLLUTION_HISTORY_URL = 'http://api.openweathermap.org/data/2.5/air_pollution/history?'
        ''' Weather URLs'''
        self.WEATHER_CURRENT_URL = 'http://api.openweathermap.org/data/2.5/weather?'
        self.WEATHER_DAILY_FORECAST_URL = 'http://api.openweathermap.org/data/2.5/forecast/daily?'
        ''' Geo URLs'''
        self.GEO_DIRECT_URL = 'http://api.openweathermap.org/geo/1.0/direct?'

    def get_geo_direct_cities_data(
            self,
            city_name: str,
            country_code: str,
    ) -> dict:
        ''' Get location data for a city. '''
        url = f"{self.GEO_DIRECT_URL}q={city_name},{country_code}&appid={self.openweather_api_key}"
        return self.get_data_from_url(url)

    def get_air_pollution_data(
            self,
            lat: float,
            lon: float,
    ) -> dict:
        ''' Get air pollution data for given coordinates.'''
        url = f"{self.AIR_POLLUTION_URL}lat={lat}&lon={lon}&appid={self.openweather_api_key}"
        return self.get_data_from_url(url)

    def get_air_pollution_history_data(
            self,
            lat: float,
            lon: float,
            unix_start_date: int,
            unix_end_date: int,
    ) -> dict:
        ''' Get historical air pollution data for given coordinates. '''
        url = f"{self.AIR_POLLUTION_HISTORY_URL}lat={lat}&lon={lon}&start={unix_start_date}&end={unix_end_date}&appid={self.openweather_api_key}"
        return self.get_data_from_url(url)

    def get_weather_data(
            self,
            city_name: str,
            country_code: str,
    ) -> dict:
        ''' Get current weather data for a city. '''
        url = f"{self.WEATHER_CURRENT_URL}q={city_name},{country_code}&appid={self.openweather_api_key}&units=metric"
        return self.get_data_from_url(url)

    def get_weather_daily_forecast_data(
            self,
            lat: float,
            lon: float,
            cnt: int = 1,
    ) -> dict:
        ''' Get current weather data for a city. '''
        url = f"{self.WEATHER_DAILY_FORECAST_URL}lat={lat}&lon={lon}&cnt={cnt}&appid={self.openweather_api_key}"
        return self.get_data_from_url(url)

    def get_data_from_url(
            self,
            url: str,
    ) -> dict:
        ''' Asserts response from url call. Return empty dict if fetching error occurs. '''
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raises a HTTPError if the HTTP request returned an unsuccessful status code
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data from URL: {url}. Error: {e}.")
            return {}  # or raise the exception, or return some default value
