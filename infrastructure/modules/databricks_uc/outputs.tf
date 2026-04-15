output "storage_credential_name" {
  value = databricks_storage_credential.this.name
}

output "external_locations" {
  value = {
    for k, v in databricks_external_location.buckets : k => {
      name = v.name
      url  = v.url
    }
  }
}

output "catalog_name" {
  value = databricks_catalog.this.name
}

output "schemas" {
  value = {
    for k, v in databricks_schema.layers : k => v.name
  }
}

output "uc_role_arn" {
  value = aws_iam_role.databricks_uc_role.arn
}