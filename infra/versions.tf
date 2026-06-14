terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # State LOCAL (simple, suffisant pour un projet solo).
  # En equipe / prod -> backend distant verrouille :
  # backend "s3" {
  #   bucket         = "mon-bucket-tfstate"
  #   key            = "crypto-realtime/terraform.tfstate"
  #   region         = "eu-west-3"
  #   dynamodb_table = "tf-locks"   # verrou concurrentiel
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region
}
