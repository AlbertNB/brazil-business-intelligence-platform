output "landing_bucket_name" {
  value = aws_s3_bucket.landing.bucket
}

output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.bucket
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.bucket
}

output "gold_bucket_name" {
  value = aws_s3_bucket.gold.bucket
}

output "metadata_bucket_name" {
  value = aws_s3_bucket.metadata.bucket
}

output "databricks_role_arn" {
  value = aws_iam_role.databricks_role.arn
}

output "landing_writer_role_arn" {
  value = aws_iam_role.landing_writer_role.arn
}

output "landing_writer_user_name" {
  value = aws_iam_user.landing_writer_user.name
}

output "landing_writer_credentials_secret_arn" {
  value = aws_secretsmanager_secret.landing_writer_credentials.arn
}