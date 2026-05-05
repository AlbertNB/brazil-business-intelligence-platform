import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Set

import requests

logger = logging.getLogger(__name__)


class HttpClient:
    """HTTP client with retry support and optional custom headers."""

    def __init__(
        self,
        timeout_sec: int = 60,
        max_retries: int = 5,
        backoff_sec: float = 2.0,
        user_agent: Optional[str] = None,
        acceptable_status_codes: Optional[Set[int]] = None,
    ) -> None:
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.backoff_sec = backoff_sec
        self.user_agent = user_agent
        self.acceptable_status_codes = acceptable_status_codes or {200}

        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        if self.user_agent:
            session.headers.update({"User-Agent": self.user_agent})
        return session

    def _reset_session(self) -> None:
        try:
            self.session.close()
        finally:
            self.session = self._build_session()

    def request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        log_progress: bool = False,
        progress_chunk_size: int = 1024 * 1024,
    ) -> requests.Response:
        """Send an HTTP request with retry behavior for transient errors.

        When ``log_progress=True`` the response is streamed and download progress
        is logged every ``progress_chunk_size`` bytes (default 1 MB).
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug("Attempt %s, %s - %s", attempt, method.upper(), url)
                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    params=params or {},
                    json=json_body,
                    headers=headers or {},
                    timeout=self.timeout_sec,
                    stream=log_progress,
                )

                if response.status_code not in self.acceptable_status_codes:
                    raise requests.HTTPError(
                        f"Unexpected HTTP status {response.status_code}",
                        response=response,
                    )

                if log_progress:
                    total: Optional[int] = None
                    content_length = response.headers.get("Content-Length")
                    if content_length and content_length.isdigit():
                        total = int(content_length)

                    downloaded = 0
                    chunks = []
                    for chunk in response.iter_content(chunk_size=progress_chunk_size):
                        if not chunk:
                            continue
                        chunks.append(chunk)
                        downloaded += len(chunk)
                        if total:
                            logger.info(
                                "Download progress | url=%s | %.1f MB / %.1f MB (%.0f%%)",
                                url,
                                downloaded / 1024 / 1024,
                                total / 1024 / 1024,
                                downloaded / total * 100,
                            )
                        else:
                            logger.info(
                                "Download progress | url=%s | %.1f MB downloaded",
                                url,
                                downloaded / 1024 / 1024,
                            )
                    response._content = b"".join(chunks)

                return response

            except requests.RequestException as exc:
                logger.warning(
                    "HTTP request failed (attempt %s/%s) for %s %s: %s",
                    attempt,
                    self.max_retries,
                    method.upper(),
                    url,
                    exc,
                )

                if isinstance(exc, requests.exceptions.SSLError):
                    # Force a fresh TLS connection when pooled socket becomes invalid.
                    self._reset_session()

                if attempt == self.max_retries:
                    raise RuntimeError(f"Request failed for URL: {url}") from exc

                time.sleep(self.backoff_sec)

    def download_to_file(
        self,
        url: str,
        target_path: Path,
        chunk_size: int,
        connect_timeout_sec: int = 30,
        expected_status_codes: Optional[Set[int]] = None,
        progress_callback: Optional[Callable[[int, Optional[int]], None]] = None,
    ) -> int:
        """Download a URL to a local file with retries and optional progress callback.

        The progress callback receives `(downloaded_bytes, expected_total_bytes)` where
        total may be `None` when the server does not provide Content-Length.
        """
        accepted_status_codes = expected_status_codes or {200}

        for attempt in range(1, self.max_retries + 1):
            try:
                downloaded = 0

                with self.session.get(
                    url,
                    stream=True,
                    headers={"Connection": "close"},
                    timeout=(connect_timeout_sec, self.timeout_sec),
                ) as response:
                    if response.status_code not in accepted_status_codes:
                        raise requests.HTTPError(
                            f"Unexpected HTTP status {response.status_code}",
                            response=response,
                        )

                    expected_size: Optional[int] = None
                    content_length_header = response.headers.get("Content-Length")
                    if content_length_header and content_length_header.isdigit():
                        expected_size = int(content_length_header)

                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(target_path, "wb") as output_file:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if not chunk:
                                continue

                            output_file.write(chunk)
                            downloaded += len(chunk)

                            if progress_callback:
                                progress_callback(downloaded, expected_size)

                return downloaded

            except requests.RequestException as exc:
                logger.warning(
                    "Download failed (attempt %s/%s) for %s: %s",
                    attempt,
                    self.max_retries,
                    url,
                    exc,
                )

                if isinstance(exc, requests.exceptions.SSLError):
                    # Force a fresh TLS connection when pooled socket becomes invalid.
                    self._reset_session()

                if attempt == self.max_retries:
                    raise RuntimeError(f"Download failed for URL: {url}") from exc

                time.sleep(self.backoff_sec)
