from datetime import datetime, timezone
from typing import Tuple


def utc_now_iso() -> str:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def s3_join(*parts: str) -> str:
    """Join path segments into a normalized S3 key."""
    return "/".join(p.strip("/").replace("\\", "/") for p in parts if p)


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """Parse an S3 URI and return its bucket and key."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    bucket_and_key = s3_uri[5:]
    bucket, _, key = bucket_and_key.partition("/")

    if not bucket:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    return bucket, key
