terraform {
    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "5.44.0"
        }
    }
     
    cloud { 
        organization = "skyisthelimit" 
        workspaces { 
            name = "weather-predictor-dev-workspace"
        } 
    } 
}

provider "aws" {
    region = "us-east-1"
}
module "etl" {
  source = "./etl_services"
  tasks_bucket_name  = "extraction_tasks"
}
