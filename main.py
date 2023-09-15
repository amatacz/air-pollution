from src import (
    cloud_integration,
    data_ingestion,
    data_configuration
)

if __name__ == "__main__":
    # create data configurator object
    DataConfiguratorObject = data_configuration.DataConfigurator()

    # create cloud configurator object
    CloudIntegratorObject = cloud_integration.CloudIntegration()

    # create data extraction object
    OpenWeatherDataIngestorObject = data_ingestion.OpenWeatherDataIngestor()

    # data placeholder
    all_city_data = {}

    ''' run a loop through all required cities to extract data '''
    for city in DataConfiguratorObject.load_cities_from_yaml():
        # get lon and lat for each city
        coord_data = OpenWeatherDataIngestorObject.get_city_coordinates(city['name'], city['country_code'])
        air_pollution_data = OpenWeatherDataIngestorObject.get_city_air_pollution_data(coord_data['lat'], coord_data['lon'])
        historical_air_pollution = OpenWeatherDataIngestorObject.get_city_air_pollution_history_data(coord_data['lat'], coord_data['lon'], 1694671200, 1694757600)
        city_weather_data = OpenWeatherDataIngestorObject.get_city_weather_data(coord_data['city_name'], coord_data['country_code'])

        # append data placeholder
        all_city_data[city['name']] = coord_data
        all_city_data[city['name']]['air_pollution'] = air_pollution_data
        all_city_data[city['name']]['history_air_pollution'] = historical_air_pollution
        all_city_data[city['name']]['current_weather'] = city_weather_data

    # upload to cloud bucket
    CloudIntegratorObject.upload_data_to_cloud_from_dict('air_pollution_bucket_amatacz', all_city_data, f'all_city_data.json')
