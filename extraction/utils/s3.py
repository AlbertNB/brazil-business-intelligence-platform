import logging
import os
from typing import IO, List, Union

import boto3
from botocore.exceptions import ClientError

from extraction.utils.helpers import parse_s3_uri

logger = logging.getLogger(__name__)


class S3Writer:
    """Utility class for writing text content to Amazon S3."""

    def __init__(self) -> None:
        session = boto3.session.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
            region_name=os.getenv("AWS_REGION"),
        )
        self.client = session.client("s3")

    def put_text(
        self,
        s3_uri: str,
        data: Union[str, IO[str]],
        content_type: str,
    ) -> None:
        """Upload text content to S3."""
        bucket, key = parse_s3_uri(s3_uri)

        if hasattr(data, "getvalue"):
            body_str = data.getvalue()
        else:
            body_str = data

        body_bytes = body_str.encode("utf-8")

        try:
            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body_bytes,
                ContentType=content_type,
            )
        except ClientError as exc:
            logger.exception("Failed to write file to S3: %s", s3_uri)
            raise RuntimeError(f"Failed to write file to S3: {s3_uri}") from exc

    def list_keys(self, bucket: str, prefix: str) -> List[str]:
        """List all object keys for a bucket/prefix pair."""
        keys: List[str] = []
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix.strip("/")):
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                if key:
                    keys.append(key)

        return keys
