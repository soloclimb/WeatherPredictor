terraform {
    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "5.35.0"
        }
    }
     
    cloud {
        organization = "skyisthelimit"

        workspaces {
        name = "WeatherPredictor"
        }
    }
}


provider "aws" {
    region     = "us-east-1"
    access_key = var.AWS_ACCESS_KEY_ID
    secret_key = var.AWS_SECRET_ACCESS_KEY
}

module "etl" {
  source = "./etl_services"
  tasks_bucket_name  = "extraction-tasks"
}
