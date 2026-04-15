terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
    time = {
      source = "hashicorp/time"
    }
  }
}

locals {
  buckets = {
    landing  = var.landing_bucket_name
    bronze   = var.bronze_bucket_name
    silver   = var.silver_bucket_name
    gold     = var.gold_bucket_name
    metadata = var.metadata_bucket_name
  }
  schema_layers = toset(["bronze", "silver", "gold"])

  uc_role_name = "${var.project_name}-${var.environment}-databricks-uc-role"
}

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "databricks_uc_role" {
  name = local.uc_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksAssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.databricks_aws_account_id}:role/${var.databricks_uc_master_role_name}"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_uc_external_id
          }
        }
      },
      {
        Sid    = "SelfAssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_uc_external_id
          }
          ArnEquals = {
            "aws:PrincipalArn" = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.uc_role_name}"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "databricks_uc_self_assume" {
  name = "${local.uc_role_name}-self-assume"
  role = aws_iam_role.databricks_uc_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowSelfAssume"
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Resource = aws_iam_role.databricks_uc_role.arn
      }
    ]
  })
}

resource "aws_iam_policy" "databricks_uc_s3" {
  name = "${var.project_name}-${var.environment}-databricks-uc-s3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = flatten([
      [
        for bucket_name in values(local.buckets) : {
          Sid      = "List${replace(title(bucket_name), "-", "")}"
          Effect   = "Allow"
          Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
          Resource = "arn:aws:s3:::${bucket_name}"
        }
      ],
      [
        {
          Sid    = "ObjectsLandingReadOnly"
          Effect = "Allow"
          Action = [
            "s3:GetObject"
          ]
          Resource = "arn:aws:s3:::${local.buckets.landing}/*"
        }
      ],
      [
        for bucket_key, bucket_name in local.buckets : {
          Sid    = "Objects${replace(title(bucket_name), "-", "")}" 
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts"
          ]
          Resource = "arn:aws:s3:::${bucket_name}/*"
        }
        if bucket_key != "landing"
      ]
    ])
  })
}

resource "aws_iam_role_policy_attachment" "databricks_uc_s3" {
  role       = aws_iam_role.databricks_uc_role.name
  policy_arn = aws_iam_policy.databricks_uc_s3.arn
}

resource "time_sleep" "wait_for_iam_propagation" {
  depends_on = [
    aws_iam_role_policy_attachment.databricks_uc_s3,
    aws_iam_role_policy.databricks_uc_self_assume
  ]

  create_duration = "30s"
}

resource "databricks_metastore_data_access" "this" {
  metastore_id = var.databricks_metastore_id
  name         = "${var.project_name}-${var.environment}-root"

  aws_iam_role {
    role_arn = aws_iam_role.databricks_uc_role.arn
  }

  is_default = false

  depends_on = [
    time_sleep.wait_for_iam_propagation
  ]
}

resource "databricks_storage_credential" "this" {
  name    = var.storage_credential_name
  comment = "Storage credential for ${var.project_name}-${var.environment}"

  aws_iam_role {
    role_arn = aws_iam_role.databricks_uc_role.arn
  }

  depends_on = [
    databricks_metastore_data_access.this
  ]
}

resource "databricks_external_location" "buckets" {
  for_each        = local.buckets
  name            = "${var.external_location_prefix}_${each.key}"
  url             = "s3://${each.value}"
  credential_name = databricks_storage_credential.this.name
  comment         = "External location for ${each.key} bucket"
  read_only       = each.key == "landing"
  force_destroy   = true
}

resource "databricks_catalog" "this" {
  name         = var.catalog_name
  comment      = "Catalog for ${var.project_name}-${var.environment}"
  storage_root = "${databricks_external_location.buckets["silver"].url}/"
}

resource "databricks_schema" "layers" {
  for_each = local.schema_layers

  catalog_name = databricks_catalog.this.name
  name         = each.value
  comment      = "${title(each.value)} layer schema for ${var.project_name}-${var.environment}"
  storage_root = "s3://${local.buckets[each.value]}/"

  depends_on = [
    databricks_external_location.buckets
  ]
}

resource "databricks_grants" "catalog_usage" {
  catalog = databricks_catalog.this.name

  grant {
    principal  = var.databricks_principal
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "schema_usage" {
  for_each = databricks_schema.layers

  schema = "${databricks_catalog.this.name}.${each.value.name}"

  grant {
    principal  = var.databricks_principal
    privileges = [
      "USE_SCHEMA",
      "CREATE_TABLE",
      "CREATE_VOLUME"
    ]
  }
}