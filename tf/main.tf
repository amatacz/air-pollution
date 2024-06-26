# Set up Cloud Function for air-pollution API data extraction
resource "google_cloudfunctions_function" "function-get-openweather-data" {
  name = "function-get-openweather-data"
  description = "Function to retrieve data from OpenWeather API"
  runtime = "python311"
  available_memory_mb = 512
  source_repository {
      url = "https://source.developers.google.com/projects/${var.gcp_project}/repos/${var.repository_name}/moveable-aliases/${var.branch_name}/paths/${var.source_directory}"
    }
  trigger_http = true
  entry_point = "gcloud_get_openweather_data_function"
}

# Set up Cloud Function for air-pollution API data extraction
resource "google_cloudfunctions_function" "function-transform-openweather-data" {
  name = "function-transform-openweather-data"
  description = "Function to transform data from OpenWeather API"
  runtime = "python311"
  available_memory_mb = 512
  source_repository {
      url = "https://source.developers.google.com/projects/${var.gcp_project}/repos/${var.repository_name}/moveable-aliases/${var.branch_name}/paths/${var.source_directory}"
    }
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource = "projects/${var.gcp_project}/topics/air-pollution-topic"

  }
  entry_point = "gcloud_transform_api_message"
}


# Create Pub/Sub topic
resource "google_pubsub_topic" "air-pollution-topic" {
  name = "air-pollution-topic"
}

# Create Pub/Sub subscription
resource "google_pubsub_subscription" "pull-get-openweather-data-subscrption" {
  topic = google_pubsub_topic.air-pollution-topic.name
  name = "pull-get-openweather-data-subscrption"

  # 10 min
  ack_deadline_seconds = 600

  # 7 days
  message_retention_duration = "604800s"
  retain_acked_messages = true

  enable_message_ordering    = false
}

# Create Workflow to manage data flow from Cloud Function to Pub/Sub

resource "google_workflows_workflow" "workflow-air-pollution" {
  name = "workflow-air-pollution"
  region = var.gcp_region

  source_contents = file("${path.module}/../workflow.yaml")
}

# Create BigQuery dataset
resource "google_bigquery_dataset" "air_pollution_dataset_unified" {
    dataset_id = "air_pollution_dataset_unified"
    description = "Dataset for data processed from OpenWeather API."
    location = "EU"
}

# Create BigQuery table
resource "google_bigquery_table" "unified_city_data" {
  dataset_id = google_bigquery_dataset.air_pollution_dataset_unified.dataset_id
  table_id = "unified_city_data"
  time_partitioning {
    type = "DAY"
  }
  # schema może być wskazana z pliku patrz: workflow
  schema = <<EOF
[
  {
    "name": "city",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "lat",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "lon",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "tag_name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "value",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  }
]
EOF
    
}