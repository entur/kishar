terraform {
  required_version = ">= 0.12.12"
  required_providers {
    google      = "~> 2.19"
    google-beta = "~> 2.19"
    kubernetes  = "~> 1.9"
    random      = "~> 2.2"
  }
}
