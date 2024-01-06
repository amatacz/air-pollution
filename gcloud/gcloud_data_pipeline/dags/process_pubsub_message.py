from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import pubsub_v1, bigquery
from google.oauth2 import service_account
import os
import pandas as pd


def retrieve_pubsub_messages_to_xcom(
        ti,
        project_id: str,
        subscription_id: str,
        max_messages: int
):
    """
    This method pulls message from Pub/Sub topic and redirect it to
    `ti` message manager in airflow.

    Args:
        project_id ->
        subscription_id ->
        max_messages ->

    """

    # Path to your service account key file
    service_account_path = 'dags/credentials.json'

    # Explicitly use service account credentials by specifying the private key file
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path
    )

    # Create a subscriber client with the credentials
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": max_messages},
        timeout=100
    )

    messages = []
    ack_ids = []
    for received_message in response.received_messages:
        messages.append(received_message.message.data.decode("utf-8"))
        ack_ids.append(received_message.ack_id)

    if ack_ids:
        subscriber.acknowledge(request={"subscription": subscription_path,
                                        "ack_ids": ack_ids})

    # Push messages to XCom (internal airflow message manager) for the next task to consume
    ti.xcom_push(key='messages', value=messages)


def send_message_to_bigquery(ti):
    # Retrieve messages from the previous task using XCom
    messages = ti.xcom_pull(task_ids='retrieve_messages', key='messages')


    return None


def process_messages_with_pandas(ti):

    # Retrieve messages from the previous task using XCom
    messages = ti.xcom_pull(task_ids='retrieve_messages', key='messages')

    if not messages:
        print("No messages to process")
        return

    # Assuming messages are in a format that can be converted to a DataFrame
    df = pd.DataFrame({'messages': messages}, orient='index')

    # replace "ń" with "n" in city names
    df['city'] = df['city'].str.replace("ń", "n")

    # convert timestamp column data type to timestamp
    df['timestamp'] = df['timestamp'].astype('datetime64[s]')

    df_transformed = df.melt(id_vars=['city', 'lon', 'lat', 'timestamp'],
                             value_vars=['aqi', 'co', 'no', 'no2', 'o3',
                                         'so2', 'pm2_5', 'pm10', 'nh3'],
                             var_name=['tag_name'])
    df_transformed = df_transformed.sort_values(by=['timestamp', 'tag_name'])

    return df_transformed


''' AIRFLOW DAG IMPLEMENTATION '''

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pubsub_pandas_dag',
    default_args=default_args,
    description='A DAG to retrieve messages from Pub/Sub and process with Pandas',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

retrieve_messages = PythonOperator(
    task_id='retrieve_messages',
    python_callable=retrieve_pubsub_messages_to_xcom,
    op_kwargs={
        'project_id': 'phonic-vortex-398110',
        'subscription_id': 'pull-get-openweather-data-subscrption',
        'max_messages': 5
    },
    provide_context=True,
    dag=dag,
)

process_messages = PythonOperator(
    task_id='process_messages',
    python_callable=process_messages_with_pandas,
    provide_context=True,
    dag=dag,
)

retrieve_messages >> process_messages
