resource "aws_s3_bucket" "tasks_bucket_name" {
  bucket = var.tasks_bucket_name

  tags = {
    Name        = "Raw weather data"
    Environment = "Dev"
  }
}

