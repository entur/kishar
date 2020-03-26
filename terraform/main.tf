provider "kubernetes" {
  load_config_file = var.load_config_file
}

provider "google" {
  project = var.gcp_project
  region  = var.region
  zone    = var.zone
}

module "redis" {
  source               = "github.com/entur/terraform//modules/redis"
  gcp_project          = var.gcp_project
  labels               = var.labels
  kubernetes_namespace = var.kubernetes_namespace
  zone                 = var.zone
  reserved_ip_range    = var.redis_reserved_ip_range
  prevent_destroy      = var.prevent_destroy
}