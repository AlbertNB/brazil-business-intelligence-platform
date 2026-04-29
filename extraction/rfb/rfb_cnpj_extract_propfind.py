import logging
import os
import re
import shutil
import tempfile
import threading
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urljoin, urlparse

from dotenv import load_dotenv

from utils.compression_tasks import iter_zip_member_bytes, write_gzip_file
from utils.helpers import s3_join, utc_now_iso
from utils.http_client import HttpClient
from utils.s3 import S3Handler

logger = logging.getLogger(__name__)


@dataclass
class DownloadTask:
    zip_name: str
    zip_url: str


@dataclass
class PrepareTask:
    zip_name: str
    local_zip_path: Path


@dataclass
class UploadTask:
    stream_name: str
    child_filename: str
    local_gz_path: Path


class RfbCnpjPropfindExtractor:
    """CNPJ extractor for Receita Federal using PROPFIND listing and a concurrent pipeline."""

    BASE_URL = "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/"

    DEFAULT_STREAM = "cnpjs"
    DEFAULT_TEMP_DIR = "/tmp/rfb_cnpjs"
    DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024

    DOWNLOAD_WORKERS = 4
    PREPARE_WORKERS = 2
    UPLOAD_WORKERS = 4

    def __init__(
        self,
        s3_bucket: str,
        s3_base_prefix: str,
        env_file: Optional[str] = None,
    ) -> None:
        if env_file:
            load_dotenv(env_file)

        if not s3_bucket:
            raise ValueError("s3_bucket is required")

        self.s3_bucket = s3_bucket
        self.s3_base_prefix = s3_base_prefix
        self.temp_dir = os.getenv("RFB_TEMP_DIR", self.DEFAULT_TEMP_DIR)

        self.http = HttpClient(
            timeout_sec=int(os.getenv("RFB_HTTP_TIMEOUT_SEC", "1800")),
            max_retries=int(os.getenv("RFB_HTTP_MAX_RETRIES", "3")),
            backoff_sec=float(os.getenv("RFB_HTTP_BACKOFF_SEC", "5")),
            acceptable_status_codes={200, 207},
            user_agent=os.getenv(
                "RFB_HTTP_USER_AGENT",
                (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            ),
        )
        self.s3 = S3Handler()

        self.stream_handler = {
            self.DEFAULT_STREAM: lambda extraction_ts: self.extract_latest_month(
                extraction_ts=extraction_ts,
            )
        }

    def run(
        self,
        streams: Optional[List[str]] = None,
        extraction_ts: Optional[str] = None,
    ) -> Dict[str, Any]:
        streams = streams or [self.DEFAULT_STREAM]
        extraction_ts = extraction_ts or utc_now_iso()

        results: Dict[str, Any] = {}

        for data_stream in streams:
            handler = self.stream_handler.get(data_stream)

            if handler is None:
                logger.warning("Unknown stream '%s'. Skipping.", data_stream)
                continue

            logger.info("Starting extraction for stream '%s'...", data_stream)
            results[data_stream] = handler(extraction_ts)
            logger.info("Finished extraction for stream '%s'.", data_stream)

        return results

    def _propfind(self, url: str) -> str:
        response = self.http.request(
            method="PROPFIND",
            url=url,
            headers={"Depth": "1"},
        )
        return response.text

    @staticmethod
    def _extract_hrefs(xml_text: str) -> List[str]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            raise RuntimeError("Invalid XML returned from PROPFIND request") from exc

        hrefs: List[str] = []
        for elem in root.iter():
            if elem.tag.endswith("}href") and elem.text:
                hrefs.append(elem.text.strip())
        return hrefs

    @staticmethod
    def _to_absolute_url(base_url: str, href: str) -> str:
        if href.startswith("http://") or href.startswith("https://"):
            return href

        normalized_base = base_url if base_url.endswith("/") else f"{base_url}/"
        return urljoin(normalized_base, href.lstrip("/"))

    def _find_latest_competence(self) -> Tuple[str, str]:
        logger.info("Listing competencies with PROPFIND: %s", self.BASE_URL)
        xml_text = self._propfind(self.BASE_URL)
        hrefs = self._extract_hrefs(xml_text)

        month_pattern = re.compile(r"(\d{4}-\d{2})/?$")
        candidates: set[str] = set()

        for href in hrefs:
            parsed_path = unquote(urlparse(href).path)
            match = month_pattern.search(parsed_path)
            if match:
                candidates.add(match.group(1))

        if not candidates:
            raise RuntimeError("Could not find any competence in YYYY-MM format")

        latest = max(candidates)
        competence_url = urljoin(self.BASE_URL, f"{latest}/")
        logger.info("Latest competence identified: %s", latest)
        return latest, competence_url

    def _list_competence_zip_files(self, competence_url: str) -> List[DownloadTask]:
        logger.info("Listing zip files for competence: %s", competence_url)
        xml_text = self._propfind(competence_url)
        hrefs = self._extract_hrefs(xml_text)

        tasks: List[DownloadTask] = []
        seen_names: set[str] = set()

        for href in hrefs:
            absolute_url = self._to_absolute_url(competence_url, href)
            path_name = Path(unquote(urlparse(absolute_url).path)).name
            if not path_name:
                continue

            if not path_name.lower().endswith(".zip"):
                continue

            if path_name in seen_names:
                continue

            seen_names.add(path_name)
            tasks.append(DownloadTask(zip_name=path_name, zip_url=absolute_url))

        tasks.sort(key=lambda item: item.zip_name)

        if not tasks:
            raise RuntimeError(f"No .zip files found for competence URL: {competence_url}")

        logger.info("Found %s zip files for competence", len(tasks))
        return tasks

    @staticmethod
    def _resolve_stream_name(filename: str) -> str:
        lower = Path(filename).name.lower()

        if lower.startswith("empresas"):
            return "empresas"
        if lower.startswith("estabelecimentos"):
            return "estabelecimentos"
        if lower.startswith("socios"):
            return "socios"
        if lower.startswith("simples"):
            return "simples"
        if lower.startswith("cnaes"):
            return "cnaes"
        if lower.startswith("motivos"):
            return "motivos"
        if lower.startswith("municipios"):
            return "municipios"
        if lower.startswith("naturezas"):
            return "naturezas"
        if lower.startswith("paises"):
            return "paises"
        if lower.startswith("qualificacoes"):
            return "qualificacoes"

        return "others"

    def _download_zip(self, task: DownloadTask, download_dir: Path) -> Path:
        local_zip_path = download_dir / task.zip_name
        local_zip_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info("[download] start: %s", task.zip_name)

        next_progress_pct = 20

        def on_progress(downloaded: int, expected_total: Optional[int]) -> None:
            nonlocal next_progress_pct

            if expected_total is None or expected_total <= 0:
                return

            progress_pct = int((downloaded * 100) / expected_total)
            while progress_pct >= next_progress_pct and next_progress_pct <= 100:
                logger.info("[download] progress: %s %s%%", task.zip_name, next_progress_pct)
                next_progress_pct += 20

        self.http.download_to_file(
            url=task.zip_url,
            target_path=local_zip_path,
            chunk_size=self.DOWNLOAD_CHUNK_SIZE,
            connect_timeout_sec=30,
            expected_status_codes={200},
            progress_callback=on_progress,
        )

        if not local_zip_path.exists() or local_zip_path.stat().st_size <= 0:
            raise RuntimeError(f"Downloaded file is missing or empty: {local_zip_path}")

        logger.info("[download] end: %s", task.zip_name)
        return local_zip_path

    def _prepare_zip(
        self,
        task: PrepareTask,
        prepared_dir: Path,
        failures: List[Dict[str, str]],
        failures_lock: threading.Lock,
    ) -> List[UploadTask]:
        logger.info("[prepare] start: %s", task.zip_name)

        if not task.local_zip_path.exists() or task.local_zip_path.stat().st_size <= 0:
            raise RuntimeError(f"Input zip does not exist or is empty: {task.local_zip_path}")

        stream_name = self._resolve_stream_name(task.zip_name)
        zip_target_dir = prepared_dir / Path(task.zip_name).stem
        zip_target_dir.mkdir(parents=True, exist_ok=True)

        upload_tasks: List[UploadTask] = []

        for child_filename, raw_bytes in iter_zip_member_bytes(task.local_zip_path):
            gz_path = zip_target_dir / f"{child_filename}.gz"

            try:
                write_gzip_file(content=raw_bytes, target_path=gz_path)

                upload_tasks.append(
                    UploadTask(
                        stream_name=stream_name,
                        child_filename=child_filename,
                        local_gz_path=gz_path,
                    )
                )
            except Exception as exc:
                logger.exception(
                    "[prepare] failed child file %s from %s",
                    child_filename,
                    task.zip_name,
                )
                with failures_lock:
                    failures.append(
                        {
                            "stage": "prepare",
                            "file": f"{task.zip_name}:{child_filename}",
                            "error": str(exc),
                        }
                    )

        logger.info("[prepare] end: %s", task.zip_name)
        return upload_tasks

    def _upload_gz(self, task: UploadTask, reference_month: str) -> str:
        target_key = s3_join(
            self.s3_base_prefix,
            task.stream_name,
            f"reference_month={reference_month}",
            task.local_gz_path.name,
        )

        logger.info("[upload] start: %s", task.local_gz_path.name)

        with open(task.local_gz_path, "rb") as fobj:
            self.s3.client.put_object(
                Bucket=self.s3_bucket,
                Key=target_key,
                Body=fobj.read(),
                ContentType="application/gzip",
            )

        if task.local_gz_path.exists():
            task.local_gz_path.unlink()

        parent_dir = task.local_gz_path.parent
        if parent_dir.exists() and not any(parent_dir.iterdir()):
            parent_dir.rmdir()

        logger.info("[upload] end: %s", task.local_gz_path.name)
        return f"s3://{self.s3_bucket}/{target_key}"

    @staticmethod
    def _append_failure(
        stage: str,
        file_name: str,
        exc: Exception,
        failures: List[Dict[str, str]],
        failures_lock: threading.Lock,
    ) -> None:
        with failures_lock:
            failures.append(
                {
                    "stage": stage,
                    "file": file_name,
                    "error": str(exc),
                }
            )

    @staticmethod
    def _worker_loop(
        queue: Queue,
        process_task: Callable[[Any], None],
        on_error: Callable[[Any, Exception], None],
        on_finally: Optional[Callable[[Any], None]] = None,
    ) -> None:
        while True:
            task = queue.get()
            try:
                if task is None:
                    return

                try:
                    process_task(task)
                except Exception as exc:
                    on_error(task, exc)
                finally:
                    if on_finally is not None:
                        on_finally(task)
            finally:
                queue.task_done()

    def _download_worker(
        self,
        download_queue: Queue,
        prepare_queue: Queue,
        download_dir: Path,
        failures: List[Dict[str, str]],
        failures_lock: threading.Lock,
    ) -> None:
        def process_task(task: DownloadTask) -> None:
            local_zip_path = self._download_zip(task, download_dir)
            prepare_queue.put(PrepareTask(zip_name=task.zip_name, local_zip_path=local_zip_path))

        def on_error(task: DownloadTask, exc: Exception) -> None:
            logger.exception("[download] failed: %s", task.zip_name)
            self._append_failure("download", task.zip_name, exc, failures, failures_lock)

        self._worker_loop(download_queue, process_task, on_error)

    def _prepare_worker(
        self,
        prepare_queue: Queue,
        upload_queue: Queue,
        prepared_dir: Path,
        failures: List[Dict[str, str]],
        failures_lock: threading.Lock,
    ) -> None:
        def process_task(task: PrepareTask) -> None:
            upload_tasks = self._prepare_zip(
                task=task,
                prepared_dir=prepared_dir,
                failures=failures,
                failures_lock=failures_lock,
            )
            for upload_task in upload_tasks:
                upload_queue.put(upload_task)

        def on_error(task: PrepareTask, exc: Exception) -> None:
            logger.exception("[prepare] failed: %s", task.zip_name)
            self._append_failure("prepare", task.zip_name, exc, failures, failures_lock)

        def on_finally(task: PrepareTask) -> None:
            if task.local_zip_path.exists():
                task.local_zip_path.unlink(missing_ok=True)

        self._worker_loop(prepare_queue, process_task, on_error, on_finally)

    def _upload_worker(
        self,
        upload_queue: Queue,
        reference_month: str,
        uploaded_by_stream: Dict[str, List[str]],
        uploaded_lock: threading.Lock,
        failures: List[Dict[str, str]],
        failures_lock: threading.Lock,
    ) -> None:
        def process_task(task: UploadTask) -> None:
            s3_uri = self._upload_gz(task=task, reference_month=reference_month)
            with uploaded_lock:
                uploaded_by_stream.setdefault(task.stream_name, []).append(s3_uri)

        def on_error(task: UploadTask, exc: Exception) -> None:
            logger.exception("[upload] failed: %s", task.local_gz_path.name)
            self._append_failure("upload", task.local_gz_path.name, exc, failures, failures_lock)

        self._worker_loop(upload_queue, process_task, on_error)

    @staticmethod
    def _close_stage(queue: Queue, workers: int) -> None:
        for _ in range(workers):
            queue.put(None)
        queue.join()

    @staticmethod
    def _start_workers(target, amount: int, args: Iterable[Any]) -> List[threading.Thread]:
        workers: List[threading.Thread] = []
        for idx in range(amount):
            thread = threading.Thread(
                target=target,
                args=tuple(args),
                name=f"{target.__name__}-{idx + 1}",
                daemon=True,
            )
            thread.start()
            workers.append(thread)
        return workers

    def extract_latest_month(self, extraction_ts: Optional[str] = None) -> Dict[str, Any]:
        extraction_ts = extraction_ts or utc_now_iso()

        reference_month, competence_url = self._find_latest_competence()
        download_tasks = self._list_competence_zip_files(competence_url)

        temp_root = Path(self.temp_dir)
        temp_root.mkdir(parents=True, exist_ok=True)
        runtime_dir = Path(tempfile.mkdtemp(prefix="rfb_pipeline_", dir=temp_root))
        download_dir = runtime_dir / "downloads"
        prepared_dir = runtime_dir / "prepared"
        download_dir.mkdir(parents=True, exist_ok=True)
        prepared_dir.mkdir(parents=True, exist_ok=True)

        download_queue: Queue = Queue()
        prepare_queue: Queue = Queue()
        upload_queue: Queue = Queue()

        uploaded_by_stream: Dict[str, List[str]] = {}
        failures: List[Dict[str, str]] = []

        uploaded_lock = threading.Lock()
        failures_lock = threading.Lock()

        try:
            download_workers = self._start_workers(
                target=self._download_worker,
                amount=self.DOWNLOAD_WORKERS,
                args=(
                    download_queue,
                    prepare_queue,
                    download_dir,
                    failures,
                    failures_lock,
                ),
            )
            prepare_workers = self._start_workers(
                target=self._prepare_worker,
                amount=self.PREPARE_WORKERS,
                args=(
                    prepare_queue,
                    upload_queue,
                    prepared_dir,
                    failures,
                    failures_lock,
                ),
            )
            upload_workers = self._start_workers(
                target=self._upload_worker,
                amount=self.UPLOAD_WORKERS,
                args=(
                    upload_queue,
                    reference_month,
                    uploaded_by_stream,
                    uploaded_lock,
                    failures,
                    failures_lock,
                ),
            )

            for task in download_tasks:
                download_queue.put(task)

            self._close_stage(download_queue, self.DOWNLOAD_WORKERS)
            self._close_stage(prepare_queue, self.PREPARE_WORKERS)
            self._close_stage(upload_queue, self.UPLOAD_WORKERS)

            for thread in download_workers + prepare_workers + upload_workers:
                thread.join(timeout=2)

            files_uploaded = sum(len(files) for files in uploaded_by_stream.values())

            return {
                "reference_month": reference_month,
                "competence_url": competence_url,
                "extraction_ts": extraction_ts,
                "zip_files_found": len(download_tasks),
                "streams": sorted(uploaded_by_stream.keys()),
                "files_uploaded": files_uploaded,
                "uploaded_files_by_stream": uploaded_by_stream,
                "failures": failures,
            }
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)


def main() -> None:
    load_dotenv("landing.env")

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    extractor = RfbCnpjPropfindExtractor(
        s3_bucket=os.getenv("S3_BUCKET"),
        s3_base_prefix="rfb/cnpjs",
    )

    result = extractor.run()
    print("Saved:", result)


if __name__ == "__main__":
    main()
