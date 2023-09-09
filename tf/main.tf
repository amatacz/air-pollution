provider "google" {
  credentials = file("C:\\Users\\matacza\\Desktop\\Projekty\\DE\\Pobieranie Danych (API)\\code\\secrets\\google_cloud_service_key.json")
  project     = "phonic-vortex-398110"
  region      = "us-central1"
}

resource "google_storage_bucket" "amatacz_air_pollution_bucket" {
  name     = "air_pollution_bucket_amatacz"
  location = "EU"

}

resource "google_storage_bucket" "amatacz_openweather_bucket" {
  name     = "openweather_bucket_amatacz"
  location = "EU"

}