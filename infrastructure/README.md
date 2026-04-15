# BBIP Terraform - AWS + Databricks Unity Catalog

This folder provisions AWS data lake resources and Databricks Unity Catalog resources with Terraform.

## What it creates

### AWS
- S3 buckets: landing, bronze, silver, gold, metadata
- Bucket protections and baseline controls:
	- versioning
	- SSE-S3 encryption
	- block public access
	- deny non-SSL requests
	- lifecycle rule to abort incomplete multipart uploads after 7 days
- Bucket tags:
	- env = dev or prod
	- layer = landing, bronze, silver, gold, metadata
- Databricks role for S3 access:
	- landing read-only
	- bronze, silver, gold, metadata read/write
- Landing writer role and user with write access to landing
- Secrets Manager secret container for landing writer credentials

### Databricks / Unity Catalog
- AWS IAM role for Unity Catalog cross-account access (with self-assume support)
- Metastore data access
- Storage credential
- External locations for all buckets (landing read-only)
- Catalog with storage root in silver
- Schemas:
	- bronze
	- silver
	- gold
- Managed location per schema using the corresponding bucket
- Grants:
	- USE_CATALOG on the catalog
	- USE_SCHEMA, CREATE_TABLE, CREATE_VOLUME on bronze/silver/gold schemas

## Structure

- main.tf: root orchestration
- variables.tf: root input variables
- outputs.tf: root outputs
- modules/aws_datalake: AWS resources
- modules/databricks_uc: Unity Catalog resources

## Required variables

The root module expects values in terraform.tfvars (or environment TF_VAR_ variables), including:

- project_name
- environment
- aws_region
- databricks_host
- databricks_token
- databricks_metastore_id
- databricks_uc_external_id
- databricks_aws_account_id
- databricks_uc_master_role_name
- databricks_principal
- catalog_name
- storage_credential_name
- external_location_prefix

See terraform.tfvars.example for a template.

## Notes

- Terraform lock file should be versioned: .terraform.lock.hcl
- Local state files and tfvars files should not be committed
- landing external location is configured as read-only
- catalog storage root points to the silver bucket location
- SELECT and MODIFY are not granted at schema level; they should be granted on tables/views/volumes when needed

## Commands

terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars

Optional destroy flow:

terraform plan -destroy -var-file=terraform.tfvars
terraform destroy -var-file=terraform.tfvars