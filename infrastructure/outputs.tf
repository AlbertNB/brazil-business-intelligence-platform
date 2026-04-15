output "landing_bucket_name" {
  value = module.aws_datalake.landing_bucket_name
}

output "bronze_bucket_name" {
  value = module.aws_datalake.bronze_bucket_name
}

output "silver_bucket_name" {
  value = module.aws_datalake.silver_bucket_name
}

output "gold_bucket_name" {
  value = module.aws_datalake.gold_bucket_name
}

output "metadata_bucket_name" {
  value = module.aws_datalake.metadata_bucket_name
}

output "databricks_role_arn" {
  value = module.aws_datalake.databricks_role_arn
}

output "landing_writer_role_arn" {
  value = module.aws_datalake.landing_writer_role_arn
}

output "landing_writer_user_name" {
  value = module.aws_datalake.landing_writer_user_name
}

output "landing_writer_credentials_secret_arn" {
  value = module.aws_datalake.landing_writer_credentials_secret_arn
}

output "uc_storage_credential_name" {
  value = module.databricks_uc.storage_credential_name
}

output "uc_external_locations" {
  value = module.databricks_uc.external_locations
}

output "uc_catalog_name" {
  value = module.databricks_uc.catalog_name
}

output "uc_schemas" {
  value = module.databricks_uc.schemas
}

output "uc_role_arn" {
  value = module.databricks_uc.uc_role_arn
}