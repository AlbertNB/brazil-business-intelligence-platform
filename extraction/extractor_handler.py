import logging
import os
import sys

from dotenv import load_dotenv

from ibge.extractor import IbgeExtractor
from rfb.extractor import RfbCnpjExtractor


logger = logging.getLogger(__name__)


def get_arg(flag: str):
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return None


def main() -> None:
    load_dotenv("landing.env")

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    source = get_arg("--source")
    stream_arg = get_arg("--stream")

    if not source:
        raise ValueError("Missing --source")

    source = source.lower()

    streams = None
    if stream_arg:
        streams = [
            s.strip().lower()
            for s in stream_arg.split(",")
            if s.strip()
        ]

    if source == "ibge":
        extractor = IbgeExtractor(
            s3_bucket=os.getenv("S3_BUCKET"),
            s3_base_prefix="ibge",
        )
    elif source == "rfb":
        extractor = RfbCnpjExtractor(
            s3_bucket=os.getenv("S3_BUCKET"),
            s3_base_prefix="rfb/cnpjs",
        )
    else:
        raise ValueError(f"Unknown source: {source}")

    result = extractor.run(streams=streams)

    print("Saved:", result)


if __name__ == "__main__":
    main()
