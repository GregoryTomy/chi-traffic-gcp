variable "credentials" {
    description = "GCP credentials"
    default = "keys/terraform-sa-key.json"
}

variable "project_name" {
    description = "Project name"
    default = "chi-traffic-gcp"
}

variable "project_number" {
  description = "Project number"
  default     = 450286238433
}

variable "gcs_bucket_location" {
    description = "Project location"
    default = "US"
}

variable "gcs_bucket_name" {
    description = "GCS bucket name for Chicago traffic data"
    default = "chi-traffic-gcp-bucket"
}
