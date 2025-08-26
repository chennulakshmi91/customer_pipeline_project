
provider "google" {
  project = "customer-transactions-pipeline"
  region  = "us-central1"
}

resource "google_storage_bucket" "data_bucket" {
  name          = "customer-transactions-data1"
  location      = "US"
  force_destroy = true
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics1"
  location   = "US"
}
