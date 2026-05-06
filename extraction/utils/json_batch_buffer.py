import json
import logging
from typing import Any, Callable, Dict, List

from utils.helpers import s3_join
from utils.s3 import S3Handler

logger = logging.getLogger(__name__)


class JsonBatchBuffer:
    """Buffer records and flush them in batches to S3 as JSON files."""

    def __init__(
        self,
        s3_handler: S3Handler,
        s3_bucket: str,
        s3_base_prefix: str,
        stream_name: str,
        extraction_ts: str,
        flush_threshold_bytes: int = 10 * 1024 * 1024,
        file_name_builder: Callable[[str, str, int], str] | None = None,
    ) -> None:
        self.s3_handler = s3_handler
        self.s3_bucket = s3_bucket
        self.s3_base_prefix = s3_base_prefix
        self.stream_name = stream_name
        self.extraction_ts = extraction_ts
        self.flush_threshold_bytes = flush_threshold_bytes
        self.file_name_builder = file_name_builder or self._default_file_name_builder

        self.items: List[Dict[str, Any]] = []
        self.current_size_bytes = 0
        self.part_number = 1
        self.written_files: List[str] = []

    def add(self, item: Dict[str, Any]) -> None:
        """Add an item to the buffer and flush when the threshold is reached."""
        item_size = len(json.dumps(item, ensure_ascii=False).encode("utf-8"))
        self.items.append(item)
        self.current_size_bytes += item_size
        logger.debug(
            "%s current size, flush threshold %s",
            self.current_size_bytes,
            self.flush_threshold_bytes,
        )

        if self.current_size_bytes >= self.flush_threshold_bytes:
            self.flush()

    def flush(self) -> None:
        """Flush all buffered items as a JSON file to S3."""
        if not self.items:
            return

        s3_uri = self._build_s3_uri()
        payload = json.dumps(self.items, ensure_ascii=False)
        logger.info("Flushing JSON batch into %s", s3_uri)

        self.s3_handler.put_text(
            s3_uri=s3_uri,
            data=payload,
            content_type="application/json; charset=utf-8",
        )

        self.written_files.append(s3_uri)
        self.items = []
        self.current_size_bytes = 0
        self.part_number += 1

    def _build_s3_uri(self) -> str:
        """Build the destination S3 URI for the current batch file."""
        file_name = self.file_name_builder(
            self.stream_name,
            self.extraction_ts,
            self.part_number,
        )

        key = s3_join(
            self.s3_base_prefix,
            self.stream_name,
            f"_extraction_ts={self.extraction_ts}",
            file_name,
        )
        return f"s3://{self.s3_bucket}/{key}"

    @staticmethod
    def _default_file_name_builder(
        stream_name: str,
        extraction_ts: str,
        part_number: int,
    ) -> str:
        """Build the default batch file name for S3."""
        return f"{stream_name}_{extraction_ts}_part_{part_number:05d}.json"
