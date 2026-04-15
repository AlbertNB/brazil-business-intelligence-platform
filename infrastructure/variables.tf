variable "project_name" {
  type        = string
  description = "Project prefix, for example bbip"
}

variable "environment" {
  type        = string
  description = "Environment name, for example dev or prod"
}

variable "aws_region" {
  type        = string
  description = "AWS region"
}

variable "databricks_host" {
  type        = string
  sensitive   = true
  description = "Databricks workspace host"
}

variable "databricks_token" {
  type        = string
  sensitive   = true
  description = "Databricks token"
}

variable "databricks_metastore_id" {
  type        = string
  description = "Unity Catalog metastore id"
}

variable "databricks_uc_external_id" {
  type        = string
  description = "External ID used by Databricks Unity Catalog when assuming cross-account roles"
}

variable "databricks_aws_account_id" {
  type        = string
  description = "Databricks AWS account id for Unity Catalog cross-account assume-role"
}

variable "databricks_uc_master_role_name" {
  type        = string
  description = "Databricks Unity Catalog master role name"
}

variable "databricks_principal" {
  type        = string
  description = "Databricks principal (user or group) that will receive catalog and schema grants"
}

variable "catalog_name" {
  type        = string
  description = "Unity Catalog name"
}

variable "storage_credential_name" {
  type        = string
  description = "Unity Catalog storage credential name"
}

variable "external_location_prefix" {
  type        = string
  description = "Prefix for external location names"
}