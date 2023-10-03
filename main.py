from src import (
    cloud_integration,
    data_ingestion,
    data_configuration,
    data_transformation
)

if __name__ == "__main__":
    # create data configurator object
    DataConfiguratorObject = data_configuration.DataConfigurator()

    # create cloud configurator object
    CloudIntegratorObject = cloud_integration.CloudIntegration()

    # create data extraction object
    OpenWeatherDataIngestorObject = data_ingestion.OpenWeatherDataIngestor()

    # create data transformator object
    DataTransformatorObject = data_transformation.OpenWeatherHistoricalDataTransformator()

    # data placeholder
    all_city_data = {}

    ''' run a loop through all required cities to extract data '''
    for city in DataConfiguratorObject.load_cities_from_yaml():
        # get lon and lat for each city
        coord_data = OpenWeatherDataIngestorObject.get_city_coordinates(city['name'], city['country_code'])
        air_pollution_data = OpenWeatherDataIngestorObject.get_city_air_pollution_data(coord_data['lat'], coord_data['lon'])
        historical_air_pollution = OpenWeatherDataIngestorObject.get_city_air_pollution_history_data(coord_data['lat'], coord_data['lon'], 1696320000, 1696356000)  # Timestamp podane na 3-10-2023 8-18, na probe
        city_weather_data = OpenWeatherDataIngestorObject.get_city_weather_data(coord_data['city_name'], coord_data['country_code'])

        # append data placeholder
        all_city_data[city['name']] = coord_data
        all_city_data[city['name']]['air_pollution'] = air_pollution_data
        all_city_data[city['name']]['history_air_pollution'] = historical_air_pollution
        all_city_data[city['name']]['current_weather'] = city_weather_data

    # upload to cloud bucket
    CloudIntegratorObject.upload_data_to_cloud_from_dict('air_pollution_bucket_amatacz', all_city_data, f'all_city_data.json')

    # download data from bucket
    # NIE UŻYŁAM TEGO, BO PRZEPROCESOWAŁAM "all_city_data z linii 22, ale sprawdzałam - działa"
    # CloudIntegratorObject.download_data_from_cloud_to_dict('air_pollution_bucket_amatacz', 'all_city_data.json')

    # SEKCJA DATA TRANSFORMATION -> NIE MAM POJĘCIA JAK TO LOGICZNIE, OBIEKTOWO POUKŁADAĆ
    all_city_data_dict = DataTransformatorObject.save_data_to_dict(all_city_data) 
    all_city_data_frame = DataTransformatorObject.save_dict_to_df(all_city_data_dict)
    all_city_data_frame = DataTransformatorObject.data_cleaning(all_city_data_frame)

    # create BigQuery dataset
    CloudIntegratorObject.create_bigquery_dataset("air_pollution")

    ''' Run a loop through all cities in dataframe, create table and populate it with data. '''
    for city in all_city_data_frame['city'].unique():
        # gets table schema for city tables
        schema = DataConfiguratorObject.city_datatable_schema()
        # create city table
        CloudIntegratorObject.create_bigquery_table("air_pollution", city, schema=schema)  # create city table
        # create dataframe with city data
        city_data_frame = DataTransformatorObject.save_city_data_to_city_dataframe(city, all_city_data_frame)
        # populate table with data
        CloudIntegratorObject.insert_data_from_df_to_bigquery_table(city_data_frame, "air_pollution", city, schema)
