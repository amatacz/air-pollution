import pandas as pd
import json


class OpenWeatherHistoricalDataTransformator:
    def __init__(self) -> None:
        pass

    def load_json(self, path_to_file: str) -> dict:
        '''
        Loads data from .json file in provided path.
        '''
        try:
            with open(path_to_file, 'r') as file:
                json_data = json.load(file)

            return json_data
        except json.JSONDecodeError:
            return None

    def save_history_data_to_dict(self, json_file: dict) -> dict:
        '''
        Loops through all cities from json_file and save loaded data to dictionary.
        Return None if no json.
        '''
        # Flatten nested structures and store each entry in a list
        entries = [
            { 
                'city': city,
                'lon': data['lon'],
                'lat': data['lat'],
                'aqi': item['aqi'],
                'co': item['air_components']['co'],
                'no': item['air_components']['no'],
                'no2': item['air_components']['no2'],
                'o3': item['air_components']['o3'],
                'so2': item['air_components']['so2'],
                'pm2_5': item['air_components']['pm2_5'],
                'pm10': item['air_components']['pm10'],
                'nh3': item['air_components']['nh3'],
                'timestamp': item['datetime']
            }
            for city, data in json_file.items()
            for key, item in data['history_air_pollution'].items()
        ]

        # Convert the list of entries into a dictionary with sequential keys
        return {index: entry for index, entry in enumerate(entries)}

    def save_dict_to_df(self, dict_file: dict) -> pd.DataFrame:
        '''
        Save dictionary to pandas DataFrame object.
        Returns None if dictionary is empty.
        '''
        try:
            return pd.DataFrame.from_dict(dict_file, orient='index')
        except Exception as e:
            return None

    def data_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        # TU NIE WIEM W SUMIE JAK TO ZROBIĆ LOGICZNIE, A OBIEKTOWO, WIĘC NA RAZIE WYPRINTOWAŁAM OUTPUTY

        # prints information about DatFrame - CZY TO POTRZEBNE?
        print("DATAFRAME INFORMATION: ", df.info())

        # replace "ń" with "n" in city names
        df['city'] = df['city'].str.replace("ń", "n")

        # convert timestamp column data type to timestamp
        df['timestamp'] = df['timestamp'].astype('datetime64[s]')

        # confirm conversion - CZY TO POTRZEBNE?
        print("CONVERTED DATATYPES: ", df.dtypes)
        return df

    def melt_all_cities_data_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Melts all_cities_data_frame and returns new dataframe.
        '''
        df_transformed = df.melt(id_vars=['city', 'lon', 'lat', 'timestamp'],
                                 value_vars=['aqi', 'co', 'no', 'no2', 'o3',
                                             'so2', 'pm2_5', 'pm10', 'nh3'],
                                 var_name=['tag_name'])
        df_transformed = df_transformed.sort_values(by=['timestamp', 'tag_name'])
        return df_transformed

    def save_city_data_to_city_dataframe(self, city, df: pd.DataFrame) -> pd.DataFrame:
        ''' Creates separate DataFrame for each city. '''
        return df[df['city'] == city]

    def historic_data_transform(self, data_dict):
        all_city_history_dict = self.save_history_data_to_dict(data_dict)
        all_city_history_data_frame = self.save_dict_to_df(all_city_history_dict)
        all_city_history_data_frame = self.data_cleaning(all_city_history_data_frame)

        return all_city_history_data_frame


x = OpenWeatherHistoricalDataTransformator()
file = x.load_json("C:\\Users\\matacza\\Desktop\\Projekty\\DE\\Pobieranie Danych (API)\\code\\data\\all_city_data.json")
print(x.save_history_data_to_dict(file))