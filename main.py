from cloud_functions import cloud_integration
from etl import (
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

    # load cities from yaml file
    cities = DataConfiguratorObject.load_cities_from_yaml()

    # ingest all city data and return all_city_data dictionary
    all_city_data = OpenWeatherDataIngestorObject.all_city_data_ingest(cities, 1696320000, 1696356000)

    # upload to cloud bucket
    CloudIntegratorObject.upload_data_to_cloud_from_dict('air_pollution_bucket_amatacz', all_city_data, 'all_city_data.json')

    # download data from bucket
    CloudIntegratorObject.download_data_from_cloud_to_dict('air_pollution_bucket_amatacz', 'all_city_data.json')

    # get and transform historic air pollution data for all cities
    all_city_data_frame = DataTransformatorObject.historic_data_transform(all_city_data)

    # melt all_city_data_frame
    all_city_data_melted = DataTransformatorObject.melt_all_cities_data_frame(all_city_data_frame)

    ''' Run a loop through all cities in dataframe, create table and populate it with data. '''
    # get schema for city table 
    city_table_schema = DataConfiguratorObject.load_city_table_schema_from_yaml()
    for city in all_city_data_frame['city'].unique():
        # create dataframe with city data
        city_data_frame = DataTransformatorObject.save_city_data_to_city_dataframe(city, all_city_data_frame)
        # create dataset if not exists, create city table and populate it with data
        CloudIntegratorObject.create_dataset_table_and_insert_data("air_pollution", city, city_table_schema, city_data_frame)

    ''' Create dataset and table for unified city data and populate it with data.'''
    # get schema for unified city table
    unified_city_table_schema = DataConfiguratorObject.load_unified_city_table_schema_from_yaml()
    # create dataset if not exists, create city table and populate it with data
    CloudIntegratorObject.create_dataset_table_and_insert_data("air_pollution_dataset_unified", "unified_city_data", unified_city_table_schema, all_city_data_melted)