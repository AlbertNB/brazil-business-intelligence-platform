terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.81.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

module "aws_datalake" {
  source       = "./modules/aws_datalake"
  project_name = var.project_name
  environment  = var.environment
  aws_region   = var.aws_region
}

module "databricks_uc" {
  source = "./modules/databricks_uc"

  project_name                     = var.project_name
  environment                      = var.environment
  aws_region                       = var.aws_region
  databricks_host                  = var.databricks_host
  databricks_token                 = var.databricks_token
  databricks_metastore_id          = var.databricks_metastore_id
  databricks_uc_external_id        = var.databricks_uc_external_id
  databricks_aws_account_id        = var.databricks_aws_account_id
  databricks_uc_master_role_name   = var.databricks_uc_master_role_name
  databricks_principal             = var.databricks_principal
  catalog_name                     = var.catalog_name
  storage_credential_name          = var.storage_credential_name
  external_location_prefix         = var.external_location_prefix

  landing_bucket_name              = module.aws_datalake.landing_bucket_name
  bronze_bucket_name               = module.aws_datalake.bronze_bucket_name
  silver_bucket_name               = module.aws_datalake.silver_bucket_name
  gold_bucket_name                 = module.aws_datalake.gold_bucket_name
  metadata_bucket_name             = module.aws_datalake.metadata_bucket_name
}