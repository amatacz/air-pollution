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

    def save_data_to_dict(self, json_file: dict) -> dict:
        '''
        Loops through all cities from json_file and save loaded data to dictionary.
        Return None if no json.
        '''
        dict_data = {}  # data placeholder
        counter = 0  # counter created to get unique dict_data[key], as "i" value in the loop repeats and data is overwritten
        for key in json_file.keys():
            for i in range(0, len(json_file[key]['history_air_pollution'])):
                dict_data[counter] = {
                    'city': key,
                    'lon': json_file[key]['lon'],
                    'lat': json_file[key]['lat'],
                    'co': json_file[key]['history_air_pollution'][i]['air_components']['co'],
                    'no': json_file[key]['history_air_pollution'][i]['air_components']['no'],
                    'no2': json_file[key]['history_air_pollution'][i]['air_components']['no2'],
                    'o3': json_file[key]['history_air_pollution'][i]['air_components']['o3'],
                    'so2': json_file[key]['history_air_pollution'][i]['air_components']['so2'],
                    'pm2_5': json_file[key]['history_air_pollution'][i]['air_components']['pm2_5'],
                    'pm10': json_file[key]['history_air_pollution'][i]['air_components']['pm10'],
                    'nh3': json_file[key]['history_air_pollution'][i]['air_components']['nh3'],
                    'timestamp': json_file[key]['history_air_pollution'][i]['datetime']
                }

                counter += 1  # increment counter
        return dict_data

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

    def save_city_data_to_city_dataframe(self, city, df: pd.DataFrame) -> pd.DataFrame:
        ''' Creates separate DataFrame for each city. '''
        return df[df['city'] == city]
