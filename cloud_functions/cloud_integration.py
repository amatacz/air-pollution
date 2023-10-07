import os
from dotenv import load_dotenv
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict
import json


class CloudIntegration:

    def __init__(self):
        load_dotenv('secrets/.env')
        self.cloud_key = os.environ['GCLOUD_JSON_KEY_LOCATION']
        self.project_id = None

        '''
        Retrieve project id from .json key file for google cloud project.
        '''
        if os.path.exists(self.cloud_key):
            # open json file
            with open(self.cloud_key, 'r') as file:
                try:
                    # retrieve project id from json
                    self.project_id = json.load(file).get('project_id', None)
                except json.JSONDecodeError:
                    pass

    def get_openweather_api_key(self) -> str:
        ''' return an openweather api key from .env file '''
        return self.openweather_api_key

    def get_google_cloud_project_id(self) -> str:
        ''' return a cloud project id'''
        return self.project_id

    def _get_google_cloud_client(self) -> storage.client.Client:
        '''
        Return a client to manage google cloud service from provided .json key file.
        '''
        try:
            return storage.client.Client.from_service_account_json(self.cloud_key) # return client if there is a api key provided
        except Exception as e:
            return None # if there is no api key provided

    def _get_google_cloud_bigquery_client(self) -> bigquery.Client:
        '''
        Return a client to manage google cloud Big Quert from provided .json key file.
        '''
        try:
            credentials = service_account.Credentials.from_service_account_file(self.cloud_key)
            return bigquery.Client(credentials=credentials,
                                   project=self.project_id)
        except Exception as e:
            return None # if there is no api key provided

    def upload_data_to_cloud_from_file(self, bucket_name, data_to_upload, blob_name):
        ''' Uploads files with api data to GCP buckets. '''
        bucket = self._get_google_cloud_client().bucket(bucket_name) # connect to bucket
        blob = bucket.blob(blob_name)  # create a blob
        with open(data_to_upload, "rb") as file:
            blob.upload_from_file(file)  # upload data to blob

    def upload_data_to_cloud_from_dict(self, bucket_name, data_dict, blob_name):
        ''' Uploads data from a dictionary to GCP bucket. '''
        bucket = self._get_google_cloud_client().bucket(bucket_name)  # connect to bucket
        blob = bucket.blob(blob_name)  # create a blob
        data_str = json.dumps(data_dict)  # convert dict to string
        blob.upload_from_string(data_str)  # upload data to blob

    def download_data_from_cloud_to_file(self, bucket_name, source_blob_name, destination_file_name):
        ''' Downloads data from a GCP bucket. '''
        bucket = self._get_google_cloud_client().bucket(bucket_name)  # connect to bucket
        blob = bucket.blob(source_blob_name)  # get blob name
        blob.download_to_filename(destination_file_name)  # download file to provided location and under provided name

    def download_data_from_cloud_to_dict(self, bucket_name, source_blob_name):
        ''' Downloads data from a GCP bucket as string. '''
        bucket = self._get_google_cloud_client().bucket(bucket_name)  # connect to bucket
        blob = bucket.blob(source_blob_name)  # get blob name
        contents = blob.download_as_string()  # download file as string

        return contents

    def _create_bigquery_dataset(self, dataset_name):
        ''' Creates new dataset in BigQuery project.'''
        client = self._get_google_cloud_bigquery_client()  # connect to BigQuery
        dataset = bigquery.Dataset(f"{client.project}.{dataset_name}")  # create dataset
        try:
            dataset = client.create_dataset(dataset, timeout=30)  # make API call
        except Conflict:
            print(f"Dataset {dataset_name} already exists.")
            pass
        except Exception as e:
            print(f"Error occured: {e}")
            pass

    def _create_bigquery_table(self, dataset_name, table_name, schema):
        ''' Creates new tables in BigQuery project and dataset. '''
        table_id = f"{self.project_id}.{dataset_name}.{table_name}"  # create table_id
        table = bigquery.Table(table_id, schema=schema)  # create table
        table.clustering_fields = ["city"]
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='timestamp'
        )
        try:
            table = self._get_google_cloud_bigquery_client().create_table(table)  # make API call
        except Conflict:
            print(f"Table {table} already exists.")
            pass
        except Exception as e:
            print(f"Error occured: {e}")
            pass

    def _insert_data_from_df_to_bigquery_table(self, dataframe, dataset_name, table_name, schema):
        ''' Inserts data from DataFrame to BigQuery table '''

        table_id = f"{self.project_id}.{dataset_name}.{table_name}"  # choose the destination table
        job_config = bigquery.LoadJobConfig(schema=schema)  # choose table schema
        try:
            job = self._get_google_cloud_bigquery_client().load_table_from_dataframe(
                dataframe, table_id, job_config=job_config)  # Upload the contents of a table from a DataFrame
            job.result()  # Start the job and wait for it to complete and get the result
        except Exception as e:
            print("Error occured: ", e)

    def create_dataset_table_and_insert_data(self, dataset_name, table_name, schema, data):
        # create BigQuery dataset
        self._create_bigquery_dataset(dataset_name)
        # create BigQueryTable
        self._create_bigquery_table(dataset_name, table_name, schema=schema)
        # populate table with data
        self._insert_data_from_df_to_bigquery_table(data, dataset_name, table_name, schema=schema)
