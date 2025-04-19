terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.14.0"
    }
  }
}

provider "google" {
  # project ID and region fetched from GCP project dashboard
  project = var.project_id
  region  = var.region
  credentials = "/Users/gdq/git_repos/pipeline-performance-analysis/1_extract_load/pipeline-analysis-446021-e6b585f9b41d.json"
}

# bucket for data
resource "google_storage_bucket" "taxi-data-extract" {
  name          = "taxi-data-extract"
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      # number in days
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
  public_access_prevention = "enforced"
}

# bucket for logs
resource "google_storage_bucket" "mage-run-logs" {
  name          = "mage-run-logs"
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      # number in days
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
  public_access_prevention = "enforced"
}

# Enable Cloud Run API
resource "google_project_service" "cloudrun" {
  service            = "run.googleapis.com"
  disable_on_destroy = true
}

# Create the Cloud Run service (for mage docker container)
resource "google_cloud_run_service" "run_service" {
  name     = var.app_name
  location = var.region

  template {
    spec {
      containers {
        image = var.docker_image
        ports {
          container_port = 6789
        }
        resources {
          limits = {
            cpu    = var.container_cpu
            memory = var.container_memory
          }
        }
        env {
          name  = "PROJECT_NAME"
          value = "extract_load"
        }
        env {
          name = "path_to_keyfile"
          value = "/home/src/gcp_credentials"
        }
        volume_mounts {
          mount_path = "/secrets/bigquery"
          name       = "mage-extract-load"
        }
      }
      volumes {
        name = "mage-extract-load"
        secret {
          secret_name  = "mage-extract-load"
          items {
            key  = "latest"
            path = "projects/1023261528910/secrets/"
          }
        }
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }
  autogenerate_revision_name = true

    # Waits for the Cloud Run API to be enabled
    depends_on = [google_project_service.cloudrun]
}