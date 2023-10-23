from api_functions.data_extraction import OpenWeatherDataExtractor
from etl.data_configuration import DataConfigurator


class OpenWeatherDataIngestor:

    def __init__(self) -> None:
        pass

    def get_city_coordinates(
            self,
            city_name: str,
            country_code: str,
    ) -> dict:
        '''  Coords extraction for specific city name '''
        data = OpenWeatherDataExtractor().get_geo_direct_cities_data(city_name, country_code)
        if data:
            coords_data = {
                'city_name': data[0]['name'],
                'country_code': data[0]['country'],
                'lat': data[0]['lat'],
                'lon': data[0]['lon'],
            }

            return coords_data

    def get_city_air_pollution_data(
            self,
            lat: float,
            lon: float,) -> dict:
        data = OpenWeatherDataExtractor().get_air_pollution_data(lat, lon)
        if data:
            air_pollution_data = {
                'datetime': data['list'][0]['dt'],
                'air_components': data['list'][0]['components']
            }
        return air_pollution_data

    def get_city_air_pollution_history_data(
            self,
            lat: float,
            lon: float,
            unix_start_date: int,
            unix_end_date: int) -> dict:
        data = OpenWeatherDataExtractor().get_air_pollution_history_data(lat, lon, unix_start_date, unix_end_date)
        if data:
            air_pollution_history_data = {}
            for i in range(0, len(data['list'])):
                air_pollution_history_data[i] = {
                    'datetime': data['list'][i]['dt'],
                    'aqi': data['list'][0]['main']['aqi'],
                    'air_components': data['list'][i]['components']
                }

        return air_pollution_history_data

    def get_city_weather_data(
            self,
            city_name: str,
            country_code: str,) -> dict:
        data = OpenWeatherDataExtractor().get_weather_data(city_name, country_code)
        if data:
            city_weather_data = {
                'weather': data['weather'][0]['description'],
                'temp': data['main']['temp'],
                'min_temp': data['main']['temp_min'],
                'max_temp': data['main']['temp_max']
            }

        return city_weather_data

    # NIEDOSTĘPNE W DARMOWYM PLANIE
    # def get_city_weather_forecast_data(
    #         self,
    #         lat: float,
    #         lon: float,
    #         cnt: int = 1):
    #     data = OpenWeatherDataExtractor().get_weather_daily_forecast_data(lat, lon, cnt)
    #     if data:
    #         tomorrow_city_weather = {
    #             'weather': data['list'][0]['weather'][0]['description'],
    #             'temp': data['list'][0]['temp']['day'],
    #             'temp_min': data['list'][0]['temp']['min'],
    #             'temp_max': data['list'][0]['temp']['max'],
    #         }
    #         return tomorrow_city_weather
    #     else:
    #         return "No data"

    def all_city_data_ingest(self, cities: dict, start_date: str, end_date: str) -> dict:
        '''
        Run a loop through all required cities to extract data 
        and return dictionary with all city data
        '''
        # data placeholder
        all_city_data = {}

        for city in cities:
            # get lon and lat for city
            coord_data = self.get_city_coordinates(city['name'], city['country_code'])
            # get air polluution_data for city
            air_pollution_data = self.get_city_air_pollution_data(coord_data['lat'], coord_data['lon'])
            historical_air_pollution = self.get_city_air_pollution_history_data(coord_data['lat'], coord_data['lon'], 1696320000, 1696356000)  # Timestamp podane na 3-10-2023 8-18, na próbę
            city_weather_data = self.get_city_weather_data(coord_data['city_name'], coord_data['country_code'])

            # append data placeholder
            all_city_data[city['name']] = coord_data
            all_city_data[city['name']]['air_pollution'] = air_pollution_data
            all_city_data[city['name']]['history_air_pollution'] = historical_air_pollution
            all_city_data[city['name']]['current_weather'] = city_weather_data

        return all_city_data
