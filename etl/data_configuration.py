import yaml
from google.cloud import bigquery


class DataConfigurator:

    def __init__(self) -> None:
        self.cities_yaml = 'conf/cities.yaml'
        self.city_datatable_schema = 'conf/city_table_schema.yaml'
        self.unified_city_datatable_schema = 'conf/unified_city_table_schema.yaml'

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

    def load_city_table_schema_from_yaml(self):
        ''' Extract data about city table schema for BigQuery table. '''
        try:
            with open(self.city_datatable_schema, 'r') as file:
                data = yaml.safe_load(file)
                details = data.get('fields', [])
            schema = [bigquery.SchemaField(detail['name'], detail['type']) for detail in details]
            return schema
        except FileNotFoundError:
            print(f"Error: File {self.cities_yaml} not found.")
        except PermissionError:
            print(f"Error: No permission to read the file {self.cities_yaml}.")
        except yaml.YAMLError as exc:
            print(f"Error parsing the YAML file: {exc}.")
        return []

    def load_unified_city_table_schema_from_yaml(self):
        ''' Extract data about unified city table schema for BigQuery table '''
        try:
            with open(self.unified_city_datatable_schema, 'r') as file:
                data = yaml.safe_load(file)
                details = data.get('fields', [])
            schema = [bigquery.SchemaField(detail['name'], detail['type']) for detail in details]
            return schema
        except FileNotFoundError:
            print(f"Error: File {self.unified_city_datatable_schema} not found.")
        except PermissionError:
            print(f"Error: No permission to read the file {self.unified_city_datatable_schema}.")
        except yaml.YAMLError as exc:
            print(f"Error parsng the YAML file: {exc}.")
        return []