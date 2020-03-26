variable "load_config_file" {
  description = "Load kubeconfig file"
  default     = false
}

variable "gcp_project" {
  description = "The GCP project id"
}

variable "region" {
  description = "GCP default region"
  default     = "europe-west1"
}

variable "zone" {
  description = "GCP default zone"
  default     = "europe-west1-d"
}

variable "labels" {
  description = "Labels used in all resources"
  type        = map(string)
  default = {
    manager = "terraform"
    team    = "ror"
    slack   = "talk-ror"
    app     = "kishar"
  }
}

variable "network_name" {
  default = "default-network"
}

variable "prevent_destroy" {
  description = "Prevent destruction of the infrastructure?"
  default     = false
}

variable "kubernetes_namespace" {
  description = "Your kubernetes namespace"
}

variable "redis_reserved_ip_range" {
  description = "IP range for Redis, check Confluence `IP addressing scheme`"
}