variable "project_id" {
  type        = string
  description = "pipeline-analysis"
  default     = "pipeline-analysis-446021"
}

variable "region" {
  type        = string
  description = "The default compute region"
  default     = "eu-west10"
}

variable "zone" {
  type        = string
  description = "The default compute zone"
  default     = "eu-west10-a"
}

variable "location" {
  type        = string
  description = "The default compute location"
  default     = "EU"
}

variable "app_name" {
  type        = string
  description = "Application Name"
  default     = "mage-export-load"
}

variable "container_cpu" {
  description = "Container cpu"
  default     = "2000m"
}

variable "container_memory" {
  description = "Container memory"
  default     = "2G"
}

variable "docker_image" {
  type        = string
  description = "The docker image to deploy to Cloud Run."
  default     = "mageai/mageai:latest"
}