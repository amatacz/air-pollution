FROM python:3.8

# Allow statements and log messages to immediately appear in the Cloud Run logs
ENV PYTHONUNBUFFERED True

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

#copy dag code to container image
ENV WORK_DIRECTORY temp_workdir
WORKDIR $WORK_DIRECTORY

COPY . ./
RUN cd gcloud

RUN pwd && ls

# CMD python ./gcloud/gcloud_data_pipeline/gcp_scripts/upload_dags_to_composer.py \
#         --dags_directory=./gcloud/gcloud_data_pipeline/dags/ \
#         --dags_bucket=air_pollution_composer_bucket_amatacz \
#         --bucket_folder_name=dags && \
#     python ./gcloud/gcloud_data_pipeline/gcp_scripts/upload_dags_to_composer.py \
#         --dags_directory=./gcloud/gcloud_data_pipeline/plugins/ \
#         --dags_bucket=air_pollution_composer_bucket_amatacz \
#         --bucket_folder_name=dags/plugins && \
#     python ./gcloud/gcloud_data_pipeline/gcp_scripts/upload_dags_to_composer.py \
#         --dags_directory=./gcloud/gcloud_data_pipeline/pyspark/ \
#         --dags_bucket=air_pollution_composer_bucket_amatacz \
#         --bucket_folder_name=pyspark 

# CMD python $WORK_DIRECTORY/gcp_scripts/upload_dags_to_composer.py \
#         --dags_directory=$WORK_DIRECTORY/dags/ \
#         --dags_bucket=air_pollution_composer_bucket_amatacz \
#         --bucket_folder_name=dags && \
#     python $WORK_DIRECTORY/gcp_scripts/upload_dags_to_composer.py \
#         --dags_directory=$WORK_DIRECTORY/plugins/ \
#         --dags_bucket=air_pollution_composer_bucket_amatacz \
#         --bucket_folder_name=dags/plugins && \
#     python $WORK_DIRECTORY/gcp_scripts/upload_dags_to_composer.py \
#         --dags_directory=$WORK_DIRECTORY/pyspark/ \
#         --dags_bucket=air_pollution_composer_bucket_amatacz \
#         --bucket_folder_name=pyspark 