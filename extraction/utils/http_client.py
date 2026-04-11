import logging
import time
from typing import Any, Dict, Optional, Set

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

        self.session = requests.Session()
        if self.user_agent:
            self.session.headers.update({"User-Agent": self.user_agent})

    def request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """Send an HTTP request with retry behavior for transient errors."""
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
                )

                if response.status_code not in self.acceptable_status_codes:
                    raise requests.HTTPError(
                        f"Unexpected HTTP status {response.status_code}",
                        response=response,
                    )

                return response

            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    raise RuntimeError(f"Request failed for URL: {url}") from exc

                time.sleep(self.backoff_sec)
