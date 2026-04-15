variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "databricks_host" {
  type      = string
  sensitive = true
}

variable "databricks_token" {
  type      = string
  sensitive = true
}

variable "databricks_metastore_id" {
  type = string
}

variable "databricks_uc_external_id" {
  type = string
}

variable "databricks_aws_account_id" {
  type = string
}

variable "databricks_uc_master_role_name" {
  type = string
}

variable "landing_bucket_name" {
  type = string
}

variable "bronze_bucket_name" {
  type = string
}

variable "silver_bucket_name" {
  type = string
}

variable "gold_bucket_name" {
  type = string
}

variable "metadata_bucket_name" {
  type = string
}

variable "catalog_name" {
  type = string
}

variable "storage_credential_name" {
  type = string
}

variable "external_location_prefix" {
  type = string
}

variable "databricks_principal" {
  type = string
}