import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from dotenv import load_dotenv

from utils.http_client import HttpClient
from utils.json_batch_buffer import JsonBatchBuffer
from utils.helpers import s3_join, utc_now_iso
from utils.s3 import S3Handler

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExpandedRequest:
    """A prepared HTTP request with metadata for extraction."""

    url: str
    params: Dict[str, Any]
    metadata: Dict[str, Any]


class IbgeExtractor:
    """Extractor implementation for IBGE data sources.

    This extractor provides three data streams:

    1. **estados** - Brazilian states metadata
       - Endpoint: GET /api/v1/localidades/estados
       - Returns: List of all 27 states with region information
       - Output: s3://bucket/ibge/estados/_extraction={TIMESTAMP}/estados_{TIMESTAMP}.json

    2. **municipios** - Brazilian municipalities metadata
       - Endpoint: GET /api/v1/localidades/municipios
       - Returns: List of all ~5,570 municipalities with hierarchical location data
       - Output: s3://bucket/ibge/municipios/_extraction={TIMESTAMP}/municipios_{TIMESTAMP}.json

    3. **resultados** - Socioeconomic indicators (population, area)
       - Endpoints: GET /api/v1/pesquisas/indicadores/{IDS}/resultados/{LOCATION_ID}
       - Indicators: Population (2022 census), Estimated population (2025), Area
       - Requests: ~5,600+ (one per municipality + one per state)
       - Output: s3://bucket/ibge/resultados/_extraction={TIMESTAMP}/resultados_{TIMESTAMP}_batch_{N}.json

    4. **cnaes** - CNAE subclasses
       - Endpoint: GET /api/v2/cnae/subclasses
       - Returns: Full list of CNAE subclasses with codes and descriptions
       - Output: s3://bucket/ibge/cnaes/_extraction={TIMESTAMP}/cnaes_{TIMESTAMP}.json

     5. **geolocation** - GeoJSON geometry files
         - Endpoint: GET /api/v4/malhas/paises/BR (with different intrarregiao params)
         - Returns: GeoJSON geometries for country, all states and all municipalities
         - Output: s3://bucket/ibge/geolocation/_extraction={TIMESTAMP}/geo_level={TYPE}/geolocation_{TIMESTAMP}_{TYPE}.geojson

    Usage:
        extractor = IbgeExtractor(
            s3_bucket="my-bucket",
            s3_base_prefix="ibge",
        )

        # Extract all streams
        result = extractor.run()

        # Extract specific streams
        result = extractor.run(streams=["estados", "municipios"])

        # Extract with custom timestamp
        result = extractor.run(
            streams=["resultados"],
            extraction_ts="2026-04-11T12:00:00Z"
        )
    """

    BASE_URL = "https://servicodados.ibge.gov.br"
    VIEW_PARAMS = {"view": "nivelado"}

    def __init__(
        self,
        s3_bucket: str,
        s3_base_prefix: str,
        env_file: Optional[str] = None,
    ) -> None:
        if env_file:
            load_dotenv(env_file)

        self.s3_bucket = s3_bucket
        self.s3_base_prefix = s3_base_prefix

        self.http = HttpClient(
            acceptable_status_codes={200},
            backoff_sec=2.0,
        )
        self.http_geo = HttpClient(
            acceptable_status_codes={200},
            backoff_sec=5.0,
            timeout_sec=300,
        )
        self.s3 = S3Handler()

        self.stream_handler = {
            "estados": lambda extraction_ts: self.extract_location(
                location_type="estados",
                extraction_ts=extraction_ts,
            ),
            "municipios": lambda extraction_ts: self.extract_location(
                location_type="municipios",
                extraction_ts=extraction_ts,
            ),
            "resultados": lambda extraction_ts: self.extract_results(
                extraction_ts=extraction_ts,
            ),
            "cnaes": lambda extraction_ts: self.extract_cnaes(
                extraction_ts=extraction_ts,
            ),
            "geolocation": lambda extraction_ts: self.extract_geolocation(
                extraction_ts=extraction_ts,
            ),
        }

    def run(
        self,
        streams: Optional[List[str]] = None,
        extraction_ts: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run extraction for the configured data streams."""
        streams = streams or ["estados", "municipios", "resultados"]
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

    def _build_s3_uri(self, stream_name: str, extraction_ts: str) -> str:
        """Build the S3 URI for a JSON export file."""
        key = s3_join(
            self.s3_base_prefix,
            stream_name,
            f"_extraction={extraction_ts}",
            f"{stream_name}_{extraction_ts}.json",
        )
        return f"s3://{self.s3_bucket}/{key}"

    def _get_location(self, location_type: str) -> List[Dict[str, Any]]:
        """Fetch embedded location data from the IBGE service."""
        if location_type not in {"estados", "municipios"}:
            raise ValueError("location_type must be either 'estados' or 'municipios'")

        response = self.http.request(
            method="GET",
            url=f"{self.BASE_URL}/api/v1/localidades/{location_type}",
            params=self.VIEW_PARAMS,
        )
        return response.json()

    def extract_location(
        self,
        location_type: str,
        extraction_ts: Optional[str] = None,
    ) -> str:
        """Extract location metadata and upload it to S3."""
        extraction_ts = extraction_ts or utc_now_iso()
        payload = self._get_location(location_type)

        s3_uri = self._build_s3_uri(location_type, extraction_ts)

        self.s3.put_text(
            s3_uri=s3_uri,
            data=json.dumps(payload, ensure_ascii=False),
            content_type="application/json; charset=utf-8",
        )
        return s3_uri

    def build_result_requests(
        self,
        indicators_ids: Iterable[int | str],
        location_ids: Iterable[int | str],
    ) -> List[ExpandedRequest]:
        """Build the request objects used to fetch result data from IBGE."""
        indicators_str = "|".join(str(indicator_id) for indicator_id in indicators_ids)

        return [
            ExpandedRequest(
                url=f"{self.BASE_URL}/api/v1/pesquisas/indicadores/{indicators_str}/resultados/{location_id}",
                params={},
                metadata={
                    "location_id": location_id,
                    "indicators_ids": list(indicators_ids),
                },
            )
            for location_id in location_ids
        ]

    def extract_results(
        self,
        extraction_ts: Optional[str] = None,
        flush_threshold_mb: int = 10,
    ) -> List[str]:
        """Extract result indicators and flush them to S3 in batched JSON files."""
        extraction_ts = extraction_ts or utc_now_iso()

        buffer = JsonBatchBuffer(
            s3_handler=self.s3,
            s3_bucket=self.s3_bucket,
            s3_base_prefix=self.s3_base_prefix,
            stream_name="resultados",
            extraction_ts=extraction_ts,
            flush_threshold_bytes=flush_threshold_mb * 1024 * 1024,
        )

        municipality_indicator_ids = [
            "96385",  # Population census 2022 - municipalities
            "29171",  # Population estimated 2025 - municipalities
            "29167",  # Area - municipalities
        ]

        state_indicator_ids = [
            "96385",  # Population census 2022 - states
            "29171",  # Population estimated 2025 - states
            "48980",  # Area - states
        ]

        municipality_ids = [
            city["municipio-id"]
            for city in self._get_location("municipios")
        ]

        state_ids = [
            state["UF-id"]
            for state in self._get_location("estados")
        ]

        requests_list = [
            *self.build_result_requests(municipality_indicator_ids, municipality_ids),
            *self.build_result_requests(state_indicator_ids, state_ids),
        ]

        total = len(requests_list)
        logger.info("%s | starting result extraction | total_requests=%s", utc_now_iso(), total)

        for index, request_item in enumerate(requests_list, start=1):
            location_id = request_item.metadata["location_id"]
            indicator_ids = request_item.metadata["indicators_ids"]

            try:
                response = self.http.request(
                    method="GET",
                    url=request_item.url,
                    params=request_item.params,
                )

                result = {
                    "location_id": location_id,
                    "indicators_ids": indicator_ids,
                    "extraction_ts": extraction_ts,
                    "payload": response.json(),
                }
                buffer.add(result)

            except Exception as exc:
                logger.exception(
                    "%s | failed to process location_id=%s | indicators_ids=%s",
                    utc_now_iso(),
                    location_id,
                    indicator_ids,
                )
                raise RuntimeError(
                    f"Failed to process location_id={location_id} | indicators_ids={indicator_ids}"
                ) from exc

            if index % 100 == 0 or index == total:
                logger.info("%s | result progress: %s/%s", utc_now_iso(), index, total)

        buffer.flush()

        logger.info(
            "%s | result extraction finished | written_files=%s",
            utc_now_iso(),
            len(buffer.written_files),
        )

        return buffer.written_files

    def extract_cnaes(
        self,
        extraction_ts: Optional[str] = None,
    ) -> str:
        """Extract CNAE subclasses and upload to S3."""
        extraction_ts = extraction_ts or utc_now_iso()

        response = self.http.request(
            method="GET",
            url=f"{self.BASE_URL}/api/v2/cnae/subclasses",
        )

        s3_uri = self._build_s3_uri("cnaes", extraction_ts)

        self.s3.put_text(
            s3_uri=s3_uri,
            data=json.dumps(response.json(), ensure_ascii=False),
            content_type="application/json; charset=utf-8",
        )
        return s3_uri

    def _build_geolocation_s3_uri(self, geo_level: str, extraction_ts: str) -> str:
        """Build the S3 URI for a geolocation GeoJSON file."""
        key = s3_join(
            self.s3_base_prefix,
            "geolocation",
            f"_extraction={extraction_ts}",
            f"geo_level={geo_level}",
            f"geolocation_{extraction_ts}_{geo_level}.geojson",
        )
        return f"s3://{self.s3_bucket}/{key}"

    def extract_geolocation(
        self,
        extraction_ts: Optional[str] = None,
    ) -> List[str]:
        """Extract GeoJSON geometry files for country, states and municipalities."""
        extraction_ts = extraction_ts or utc_now_iso()

        geolocation_url = f"{self.BASE_URL}/api/v4/malhas/paises/BR"
        endpoints = [
            {
                "geo_level": "municipality",
                "params": {"intrarregiao": "municipio", "formato": "application/vnd.geo+json"},
            },
            {
                "geo_level": "state",
                "params": {"intrarregiao": "UF", "formato": "application/vnd.geo+json"},
            },
            {
                "geo_level": "country",
                "params": {"formato": "application/vnd.geo+json"},
            },
        ]

        written_files: List[str] = []

        for endpoint in endpoints:
            geo_level = endpoint["geo_level"]
            logger.info("Fetching geolocation | geo_level=%s", geo_level)

            response = self.http_geo.request(
                method="GET",
                url=geolocation_url,
                params=endpoint["params"],
                log_progress=True,
                progress_chunk_size=5 * 1024 * 1024
            )

            s3_uri = self._build_geolocation_s3_uri(geo_level, extraction_ts)

            self.s3.put_text(
                s3_uri=s3_uri,
                data=response.text,
                content_type="application/vnd.geo+json; charset=utf-8",
            )
            written_files.append(s3_uri)
            logger.info("Geolocation uploaded | geo_level=%s | s3_uri=%s", geo_level, s3_uri)

        return written_files
