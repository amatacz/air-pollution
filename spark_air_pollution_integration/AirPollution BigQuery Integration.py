# Databricks notebook source
import json

from google.cloud import pubsub_v1
from google.oauth2 import service_account

# Retrieve data from Pub/Sub message manager

try:
    cred_path = "dbfs:/FileStore/credentials.json"
    credentials = service_account.Credentials.from_service_account_info(
        credentials_json
    )
    print("Credentials done: ", credentials)
except Exception as e:
    print(f"Something went wrong with credentials: {e}")

try:
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    print(f"Subscriber done: {subscriber}")
except Exception as e:
    print(f"Something went wrong with subscriber: {e}")

try:
    subscription_id = 'pull-get-openweather-data-subscrption'
    print(f"Subscription ID done: {subscription_id}")
except Exception as e:
    print(f"Something went wrong with subscription id: {e}")

try:
    subscription_path = subscriber.subscription_path(credentials_json['project_id'], subscription_id)
    print(f"Subscription path done: {subscription_path}")
except Exception as e:
    print(f"Something went wrong with subscription_path: {e}")

response = subscriber.pull(
    request = {"subscription": subscription_path, "max_messages": 5},
    timeout = 100
)

# COMMAND ----------

    messages = []
    ack_ids = []
    for received_message in response.received_messages:
        messages.append(eval(received_message.message.data.decode("utf-8")))
        ack_ids.append(received_message.ack_id)

    if ack_ids:
        subscriber.acknowledge(request={"subscription": subscription_path,
                                        "ack_ids": ack_ids})

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# DATA PROCESSING
entries = [{
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
        } for city, data in messages[0].items()
        for key, item in data['history_air_pollution'].items()]

# COMMAND ----------

entries_indexed = {index: entry for index, entry in enumerate(entries)}

# COMMAND ----------

df = pd.DataFrame.from_dict(entries_indexed, orient='index')

# COMMAND ----------

spark_df = spark.createDataFrame(df)

# COMMAND ----------

spark_df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

spark_df.dtypes

# COMMAND ----------

spark_df.city = spark_df.withColumn("city", regexp_replace(spark_df.city, "ń", "n"))

# COMMAND ----------

spark_df_transformed = spark_df.pandas_api().melt(id_vars=['city', 'lon', 'lat', 'timestamp'],
                             value_vars=['aqi', 'co', 'no', 'no2', 'o3',
                                         'so2', 'pm2_5', 'pm10', 'nh3'],
                             var_name=['tag_name']).to_spark()

# COMMAND ----------

spark_df_transformed.write \
  .format("com.google.cloud.spark.bigquery") \
  .option("credentials", credentials_base64) \
  .option("parentProject", "phonic-vortex-398110") \
  .option("temporaryGcsBucket", "air-pollution-bigquery-temp") \
  .mode("append") \
  .save("phonic-vortex-398110.air_pollution_dataset_unified.unified_city_data")
