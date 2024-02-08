# Databricks notebook source
import json

# COMMAND ----------

# credentials_json = {
#   "type": "service_account",
#   "project_id": "phonic-vortex-398110",
#   "private_key_id": "992e8c8f8f07345ae5539187d8841b70f203131c",
#   "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDZJateZmuZSHqt\nkl4SwGc/ueUQYnJIN8EVSXgQVFDjszvC3augUfSFhrZIWKA/p3dqeLLvzOlBdo0L\nif2Ebn884AXk6MKj6yCY5GGhyTKvv9OrEspgJhkqwxxv18TMxBf4tRp3ZyqUnxGB\n5slqkXOHSLg0Le+QLOfRvgJADbHp56UtbwQcTSLMJKXMme366NBma8aVqHajZOFv\n1HUzViEwHMcDbt1V/YI5N2QDSPPfVoG7lRxy0f4PK1zNvsbibagjE0Yz56tXTfuz\nK9mFgJtZwe6/mrHCchLTZZlVhgYgbCeB7lUjWN4nHQXJMlWIZcuaLPANtZm/p9Cc\n3zK7f6nrAgMBAAECggEAVeU0uI7DGUs2W9p+I+S/8tFR7v1CP5buZcE+IRd5N9j6\nVsbg0PYaR4dWK6ADI6rW7eL4xSrFSgPfHyVihzQ+WJI1Qqtf8v9bUZ9lj13LEwJK\ndG1maEopwmSTpxou2uDmqpbY9Up16XxSrJPiMKmAjY8SbnGNRfa9bJZAxZ2lNuST\n5n0lK4wd0a/Ke+b6wHMDW7DNFbH9EJViXItEPt61tyBy1UyfnKAxWla8M4cTglid\n/koR5FdO7VeiiBbl9sb27UDULipYyxXAtLUdK1aMUC1nAlWAoKbogTkmydhIAMj6\nqXZEtMn/jIZP/oMnuY4G3914Xk98nN5LboaK9l5LwQKBgQD7NQCZZCiNe4Gf/sKu\nMNCbJkXVIMQuzr3AqmmazKYQBFHIX7abbri1Okm5z5Lhiv4xQ01qU4gI7Y3JG7qS\nprvXYbjwPSkuGS9qC/DRCAGbsJF3TWKFern8eH+U8aJBXt9fulBvLBfZsqQVmGpn\n93rEqQyUa0VuR7QjnwJ8YjpL+QKBgQDdSk39nP5fiZ6Y043K1dkg14LM2tm/DlaZ\nB6SYLYqdfyf/fcP63rL8h2Z4cvU94k6YDgYz72cnw5R+P+L21WFWQ23VrXJNyfbL\nYMizj4LLS9+J+WxSxX3uiaZ2Hu6IqnjguD34gCyNkwbvlwP8SugZkTeAsBEuyHa7\n6iHSnyx2AwKBgCQsLKrp1kERQMgmKihlFRTSPQoEjttin8ZHg69rupnh1VjyF1xo\nZCaMfyNHdZIOrq2vUE14O8V1V45W9NW7Np88DiJh3DUsITy2/XVMHtcpZLj2JrKr\nym5Fd7Gd3cxl+epR7NUlV0XcyG3npaLU6xO4d/xDOhs4LgNXGUeJ6XUZAoGBAIPw\ngK29BEGsSR5vzHPqqyoUtM01k2yBvQpC71U4mhQrs4p2zvnt/CPrCyK30BcUrq2k\ndvhSPlwMFaZsqwNm2EFZtMwZPTS6PWanjyLYZzCVTXPzfi2fyOUHt3NoH+rmUWoT\nEAfxTB8hUZpanSq48sgDuGcHE0ekiH6kK4RsGVNHAoGBAN9bDAnKY5lGB5LApWqO\n8qFZZT5rBvkYkt/qf2SzTJV58tUodUpQe4mMJ62z881oiLpe6r4+X1q/xL/YZT/Q\nYq4XfFBR79ND0IgbCb6dpG1NWqbqTqNw0jbOF56m/blAbIxTCZNQ1FuL6BZ3ppgE\nQ/Tr+D9GYtmuUqYMYYyuQ7Tp\n-----END PRIVATE KEY-----\n",
#   "client_email": "air-pollution-admin@phonic-vortex-398110.iam.gserviceaccount.com",
#   "client_id": "103161065146756868567",
#   "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#   "token_uri": "https://oauth2.googleapis.com/token",
#   "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#   "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/air-pollution-admin%40phonic-vortex-398110.iam.gserviceaccount.com",
#   "universe_domain": "googleapis.com"
# }

# COMMAND ----------

# credentials_str = json.dumps(credentials_json) 

# COMMAND ----------

# import base64
# # Encode the JSON string in base64
# credentials_base64 = base64.b64encode(credentials_str.encode()).decode()

# COMMAND ----------

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
