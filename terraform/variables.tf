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

variable "region" {
    description = "Project region"
    default = "us-central1"
}

variable "gcs_bucket_name" {
    description = "GCS bucket name for Chicago traffic data"
    default = "chi-traffic-gcp-bucket"
}

variable "github_pat" {
    type = string
    sensitive = true
}

variable "github_repo_name" {
    description = "Name of GitHub repo connected to cloud run"
    default = "chi-traffic-gcp"
}

variable "github_repo_uri" {
    description = "URI of GitHub repo connected to cloud run"
    default ="https://github.com/GregoryTomy/chi-traffic-gcp.git"
}
