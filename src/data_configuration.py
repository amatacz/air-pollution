import yaml


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
