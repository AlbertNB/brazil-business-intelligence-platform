data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  bucket_names = {
    landing  = "${var.project_name}-landing-${var.environment}-${local.account_id}-${var.aws_region}"
    bronze   = "${var.project_name}-bronze-${var.environment}-${local.account_id}-${var.aws_region}"
    silver   = "${var.project_name}-silver-${var.environment}-${local.account_id}-${var.aws_region}"
    gold     = "${var.project_name}-gold-${var.environment}-${local.account_id}-${var.aws_region}"
    metadata = "${var.project_name}-metadata-${var.environment}-${local.account_id}-${var.aws_region}"
  }
}

resource "aws_s3_bucket" "landing" {
  bucket = local.bucket_names.landing
  tags = {
    env   = var.environment
    layer = "landing"
  }
}

resource "aws_s3_bucket" "bronze" {
  bucket = local.bucket_names.bronze
  tags = {
    env   = var.environment
    layer = "bronze"
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = local.bucket_names.silver
  tags = {
    env   = var.environment
    layer = "silver"
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = local.bucket_names.gold
  tags = {
    env   = var.environment
    layer = "gold"
  }
}

resource "aws_s3_bucket" "metadata" {
  bucket = local.bucket_names.metadata
  tags = {
    env   = var.environment
    layer = "metadata"
  }
}

locals {
  bucket_resources = {
    landing  = aws_s3_bucket.landing
    bronze   = aws_s3_bucket.bronze
    silver   = aws_s3_bucket.silver
    gold     = aws_s3_bucket.gold
    metadata = aws_s3_bucket.metadata
  }
}

resource "aws_s3_bucket_versioning" "all" {
  for_each = local.bucket_resources

  bucket = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = local.bucket_resources

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each = local.bucket_resources

  bucket                  = each.value.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "all" {
  for_each = local.bucket_resources

  bucket = each.value.id

  rule {
    id     = "abort-incomplete-multipart-7-days"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    filter {
      prefix = ""
    }
  }
}

data "aws_iam_policy_document" "bucket_ssl_deny" {
  for_each = local.bucket_resources

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    resources = [
      each.value.arn,
      "${each.value.arn}/*"
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "all" {
  for_each = local.bucket_resources

  bucket = each.value.id
  policy = data.aws_iam_policy_document.bucket_ssl_deny[each.key].json
}

data "aws_iam_policy_document" "databricks_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "databricks_role" {
  name                 = "${var.project_name}-databricks-role"
  description          = "Role intended for Databricks access to the project data lake on S3."
  assume_role_policy   = data.aws_iam_policy_document.databricks_assume_role.json
  max_session_duration = 43200
}

resource "aws_iam_role_policy_attachment" "cloudwatch_agent" {
  role       = aws_iam_role.databricks_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

data "aws_iam_policy_document" "databricks_role_policy" {
  statement {
    sid     = "AllowListBuckets"
    effect  = "Allow"
    actions = ["s3:ListAllMyBuckets", "s3:GetBucketLocation"]
    resources = ["*"]
  }

  statement {
    sid     = "AllowLandingBucketAccess"
    effect  = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.landing.arn]
  }

  statement {
    sid     = "AllowLandingObjectReadOnly"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.landing.arn}/*"]
  }

  statement {
    sid     = "AllowWritableBucketList"
    effect  = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.bronze.arn,
      aws_s3_bucket.silver.arn,
      aws_s3_bucket.gold.arn,
      aws_s3_bucket.metadata.arn
    ]
  }

  statement {
    sid     = "AllowWritableObjectAccess"
    effect  = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = [
      "${aws_s3_bucket.bronze.arn}/*",
      "${aws_s3_bucket.silver.arn}/*",
      "${aws_s3_bucket.gold.arn}/*",
      "${aws_s3_bucket.metadata.arn}/*"
    ]
  }
}

resource "aws_iam_role_policy" "databricks_role_policy" {
  name   = "${var.project_name}-databricks-s3-policy"
  role   = aws_iam_role.databricks_role.id
  policy = data.aws_iam_policy_document.databricks_role_policy.json
}

data "aws_iam_policy_document" "landing_writer_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "landing_writer_role" {
  name                 = "${var.project_name}-landing-writer-role"
  description          = "Role for external ingestion services that write raw files to the landing bucket."
  assume_role_policy   = data.aws_iam_policy_document.landing_writer_assume_role.json
  max_session_duration = 43200
}

data "aws_iam_policy_document" "landing_writer_role_policy" {
  statement {
    sid     = "AllowLandingBucketList"
    effect  = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.landing.arn]
  }

  statement {
    sid     = "AllowLandingObjectWrite"
    effect  = "Allow"
    actions = ["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["${aws_s3_bucket.landing.arn}/*"]
  }
}

resource "aws_iam_role_policy" "landing_writer_role_policy" {
  name   = "${var.project_name}-landing-writer-s3-policy"
  role   = aws_iam_role.landing_writer_role.id
  policy = data.aws_iam_policy_document.landing_writer_role_policy.json
}

resource "aws_iam_user" "landing_writer_user" {
  name = "${var.project_name}-landing-writer"
}

data "aws_iam_policy_document" "landing_writer_user_policy" {
  statement {
    sid     = "AllowLandingBucketListForUser"
    effect  = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.landing.arn]
  }

  statement {
    sid     = "AllowLandingObjectWriteForUser"
    effect  = "Allow"
    actions = ["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["${aws_s3_bucket.landing.arn}/*"]
  }
}

resource "aws_iam_user_policy" "landing_writer_user_policy" {
  name   = "${var.project_name}-landing-writer-user-s3-policy"
  user   = aws_iam_user.landing_writer_user.name
  policy = data.aws_iam_policy_document.landing_writer_user_policy.json
}

resource "aws_secretsmanager_secret" "landing_writer_credentials" {
  name        = "${var.project_name}/landing-writer-credentials"
  description = "Container for landing writer credentials. Populate the value out-of-band."
}