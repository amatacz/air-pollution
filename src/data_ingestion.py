from src.data_extraction import OpenWeatherDataExtractor

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
            air_pollution_history_data = {
                'datetime': data['list'][0]['dt'],
                'air_components': data['list'][0]['components']
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

    def get_city_weather_forecast_data(self):
        ''' TO DO '''
        pass
