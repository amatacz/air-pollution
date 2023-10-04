import yaml
from google.cloud import bigquery


class DataConfigurator:

    def __init__(self) -> None:
        self.cities_yaml = 'conf/cities.yaml'

    def load_cities_from_yaml(self):
        ''' Extract data about cities for OpenWeather API calls '''
        try:
            with open(self.cities_yaml, 'r') as file:
                data = yaml.safe_load(file)
            return data.get("cities", [])
        except FileNotFoundError:
            print(f"Error: File {self.cities_yaml} not found.")
        except PermissionError:
            print(f"Error: No permission to read the file {self.cities_yaml}.")
        except yaml.YAMLError as exc:
            print(f"Error parsing the YAML file: {exc}.")
        return []

    def city_datatable_schema(self):
        ''' Stores schema for city table. '''
        schema = [
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("lon", "FLOAT64"),
            bigquery.SchemaField("lat", "FLOAT64"),
            bigquery.SchemaField("aqi", "FLOAT64"),
            bigquery.SchemaField("co", "FLOAT64"),
            bigquery.SchemaField('no', "FLOAT64"),
            bigquery.SchemaField("no2", "FLOAT64"),
            bigquery.SchemaField("o3", "FLOAT64"),
            bigquery.SchemaField("so2", "FLOAT64"),
            bigquery.SchemaField("pm2_5", "FLOAT64"),
            bigquery.SchemaField("pm10", "FLOAT64"),
            bigquery.SchemaField("nh3", "FLOAT64"),
            bigquery.SchemaField("timestamp", "TIMESTAMP")
        ]
        return schema

    def all_citity_data_schema(self):
        ''' Stores schema for cities air pollution table. '''
        schema = [
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("lon", "FLOAT64"),
            bigquery.SchemaField("lat", "FLOAT64"),
            bigquery.SchemaField("tag_name", "STRING"),
            bigquery.SchemaField("value", "FLOAT64"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            ]
        return schema
