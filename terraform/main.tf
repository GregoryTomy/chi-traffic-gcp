terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.3.0"
    }
  }
}

provider "google" {
  # Configuration options
  credentials = file(var.credentials)
  project = var.project_name
  region  = "us-central1"
}


###########################################################################
# Google Cloud Storage Bucket
###########################################################################
resource "google_storage_bucket" "chi-traffic-bucket" {
  name          = var.gcs_bucket_name
  location      = var.gcs_bucket_location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

###########################################################################
# Google Cloud Composer
###########################################################################
# Create a Google Cloud service account
resource "google_service_account" "composer_service_account" {
    account_id="composer-service-account"
    display_name = "Google Cloud Composer service account"
}

# Assign IAM role to the service account
resource "google_project_iam_member" "composer_sa_roles" {
    project = var.project_name
    member = "serviceAccount:${google_service_account.composer_service_account.email}"
    role = "roles/composer.worker"
}

# Assign role to the cloud composer service agennt
resource "google_service_account_iam_member" "composer_sa_roles" {
    service_account_id = google_service_account.composer_service_account.name
    role = "roles/composer.ServiceAgentV2Ext"
    member = "serviceAccount:service-${var.project_number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

# Provision a Cloud Composer environement
resource "google_composer_environment" "composer_environment" {
    name = "chi-composer-environment"
    config {
      software_config {
        image_version = "composer-2.9.7-airflow-2.9.3"
        pypi_packages = {
            apache-airflow-providers-google=">=10.24.0"
            apache-airflow-providers-docker=">=3.14.0"
            pyarrow=">=14.0.1"
            requests=">=2.27.0"
        }
      }
      node_config {
        service_account = google_service_account.composer_service_account.email
      }
    }
}

###########################################################################
# GitHub Cloud Build Set Up
###########################################################################
# Create a secret containing the personal access token and grant permissions
# to the Service Agent
resource "google_secret_manager_secret" "github_token_secret" {
    project = var.project_name
    secret_id = "github-token"

    replication {
      auto {}
    }
}

resource "google_secret_manager_secret_version" "github_token_secret_version" {
    secret = google_secret_manager_secret.github_token_secret.id
    secret_data = var.github_pat
}

data "google_iam_policy" "serviceagent_secretAccessor" {
    binding {
        role = "roles/secretmanager.secretAccessor"
        members = ["serviceAccount:service-${var.project_number}@gcp-sa-cloudbuild.iam.gserviceaccount.com"]
    }
}

resource "google_secret_manager_secret_iam_policy" "policy" {
  project = google_secret_manager_secret.github_token_secret.project
  secret_id = google_secret_manager_secret.github_token_secret.secret_id
  policy_data = data.google_iam_policy.serviceagent_secretAccessor.policy_data
}

# Create the GitHub connection
resource "google_cloudbuildv2_connection" "my_connection" {
    project = var.project_name
    location = var.region
    name = "github-integration"

    github_config {
        app_installation_id = 56360622
        authorizer_credential {
            oauth_token_secret_version = google_secret_manager_secret_version.github_token_secret_version.id
        }
    }

    depends_on = [google_secret_manager_secret_iam_policy.policy]
}

# Link repository
resource "google_cloudbuildv2_repository" "my_repository" {
    project = var.project_name
    location = var.region
    name = var.github_repo_name
    parent_connection = google_cloudbuildv2_connection.my_connection.name
    remote_uri = var.github_repo_uri
}

###########################################################################
# Google Artifact Registry
###########################################################################
resource "google_artifact_registry_repository" "dbt_repository" {
    location = var.region
    repository_id = "dbt-images"
    description = "Repository for dbt project docker images"
    format = "DOCKER"
}

###########################################################################
# Google Cloud Run Job
###########################################################################
resource "google_cloud_run_v2_job" "dbt_cloud_run_job" {
    name = "dbt-cloud-run-job"
    location = var.region

    template {
        template {
          containers {
            image ="${var.region}-docker.pkg.dev/${var.project_name}/${google_artifact_registry_repository.dbt_repository.repository_id}/dbt-image:latest"
          }
        }
    }
}

###########################################################################
# Google Big Query
###########################################################################

# Google Big Query Dataset
resource "google_bigquery_dataset" "chi-traffic-dataset" {
  dataset_id                 = var.bq_dataset_name
  delete_contents_on_destroy = true
}
