# BBIP Terraform - AWS + Databricks Unity Catalog

This folder provisions AWS data lake resources and Databricks Unity Catalog resources with Terraform.

## storage_mode: managed vs external

The `storage_mode` variable controls the data lake architecture:

- `managed` (default): only the landing bucket is created in AWS. Bronze, silver and gold live as Unity Catalog **managed tables**, using the metastore's default managed storage instead of a bucket this project owns. There is no metadata bucket; Auto Loader schema/checkpoint state lives in a UC managed volume (`/Volumes/<catalog>/bronze/autoloader`) instead.
- `external`: the original architecture. Landing, bronze, silver, gold and metadata are each a dedicated S3 bucket, wired into Unity Catalog as external locations, with the catalog/schemas rooted at those buckets.

Switching `storage_mode` on an **existing** deployment is a destructive migration, not a toggle:
- Bronze/silver/gold/metadata buckets are destroyed (`external` -> `managed`) or created fresh (`managed` -> `external`).
- The Unity Catalog `storage_root` on the catalog changes, which the Databricks provider treats as a forcing change — the catalog (and everything under it: schemas, tables, grants) gets destroyed and recreated.
- Nothing here migrates existing data between the two storage backends. After switching, rerun the Auto Loader ingestion job (against the corresponding `bronze_ingestion*.py` script, see `databricks/`) and `dbt run` to rebuild bronze/silver/gold from the landing bucket, which is unaffected by the switch.

Run `terraform plan` and read the diff carefully before applying a `storage_mode` change against a real environment.

## What it creates

### AWS
- S3 buckets:
	- `managed` mode: landing only
	- `external` mode: landing, bronze, silver, gold, metadata
- Bucket protections and baseline controls (all created buckets):
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
	- `external` mode only: bronze, silver, gold, metadata read/write
- Landing writer role and user with write access to landing
- Secrets Manager secret container for landing writer credentials

### Databricks / Unity Catalog
- AWS IAM role for Unity Catalog cross-account access (with self-assume support)
- Metastore data access
- Storage credential
- External locations:
	- `managed` mode: landing only (read-only)
	- `external` mode: all buckets (landing read-only)
- Catalog:
	- `managed` mode: no storage_root (UC default managed storage)
	- `external` mode: storage root in silver
- Schemas:
	- bronze
	- silver
	- gold
- Managed location per schema using the corresponding bucket (`external` mode only)
- `managed` mode only: a managed volume (`autoloader`) in the bronze schema for Auto Loader schema/checkpoint state
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

Optional:
- storage_mode ("managed" or "external", default "managed")

See terraform.tfvars.example for a template.

## Notes

- Terraform lock file should be versioned: .terraform.lock.hcl
- Local state files and tfvars files should not be committed
- landing external location is configured as read-only
- catalog storage root points to the silver bucket location
- SELECT and MODIFY are not granted at schema level; they should be granted on tables/views/volumes when needed

## Commands

```bash
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

Optional destroy flow:

```bash
terraform plan -destroy -var-file=terraform.tfvars
terraform destroy -var-file=terraform.tfvars
```