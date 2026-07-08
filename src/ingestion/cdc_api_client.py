"""
Client API Socrata pour récupérer les datasets CDC en JSON/CSV.
Documentation : https://dev.socrata.com/docs/queries/
"""

import csv
import io
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

from utils.config import Config, get_datasets_config, get_mode_limits, get_socrata_config
from utils.logger import get_logger

logger = get_logger(__name__)

RETRYABLE_STATUS_CODES = {429, 502, 503, 504}


class CDCApiClient:
    """Client pour l'API Socrata CDC Open Data."""

    def __init__(self, app_token: Optional[str] = None) -> None:
        self.base_url = Config.CDC_API_BASE_URL
        self.app_token = app_token or Config.CDC_APP_TOKEN
        self.socrata_cfg = get_socrata_config()
        self.session = requests.Session()
        if self.app_token:
            self.session.headers["X-App-Token"] = self.app_token
        self.session.headers["Accept"] = "application/json"

    def _page_size(self, limit: Optional[int] = None) -> int:
        page_size = int(self.socrata_cfg.get("page_size", 50000))
        page_size = min(page_size, 50000)
        if limit is not None:
            return min(page_size, limit)
        return page_size

    def _request_with_retry(self, url: str, params: dict[str, Any]) -> requests.Response:
        timeout = int(self.socrata_cfg.get("request_timeout", 120))
        max_retries = int(self.socrata_cfg.get("max_retries", 5))
        backoff = float(self.socrata_cfg.get("retry_backoff", 2.0))

        last_error: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                if response.status_code in RETRYABLE_STATUS_CODES:
                    wait = backoff ** attempt
                    logger.warning(
                        "API %s — HTTP %d, retry %d/%d dans %.1fs",
                        url,
                        response.status_code,
                        attempt + 1,
                        max_retries,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc
                wait = backoff ** attempt
                logger.warning(
                    "API %s — %s, retry %d/%d dans %.1fs",
                    url,
                    type(exc).__name__,
                    attempt + 1,
                    max_retries,
                    wait,
                )
                time.sleep(wait)

        if last_error:
            raise last_error
        raise requests.HTTPError(f"Échec après {max_retries} tentatives : {url}")

    def _rate_limit_sleep(self) -> None:
        time.sleep(float(self.socrata_cfg.get("rate_limit_delay", 0.5)))

    def _pagination_strategy(self) -> str:
        return str(self.socrata_cfg.get("pagination_strategy", "offset"))

    def _build_pagination_params(
        self,
        page_size: int,
        *,
        offset: int = 0,
        last_key: Optional[str] = None,
        order_column: Optional[str] = None,
        order: Optional[str] = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"$limit": page_size}
        strategy = self._pagination_strategy()
        order_col = order_column or str(self.socrata_cfg.get("order_column", ":id"))

        if order:
            params["$order"] = order
        elif strategy == "keyset":
            params["$order"] = order_col

        if strategy == "keyset":
            if last_key is not None:
                params["$where"] = f"{order_col} > '{last_key}'"
        else:
            params["$offset"] = offset

        return params

    @staticmethod
    def _extract_keyset_value(record: dict[str, Any], order_column: str) -> str:
        if order_column in record:
            return str(record[order_column])
        bare = order_column.lstrip(":")
        if bare in record:
            return str(record[bare])
        if ":id" in record:
            return str(record[":id"])
        raise KeyError(f"Colonne de pagination introuvable : {order_column}")

    @staticmethod
    def _extract_keyset_from_csv(csv_text: str, order_column: str) -> Optional[str]:
        reader = csv.reader(io.StringIO(csv_text))
        rows = list(reader)
        if len(rows) < 2:
            return None

        header = rows[0]
        bare = order_column.lstrip(":")
        col_index = None
        for idx, col in enumerate(header):
            if col == order_column or col == bare or col == f":{bare}":
                col_index = idx
                break
        if col_index is None:
            raise KeyError(f"Colonne {order_column} absente du CSV")

        last_row = rows[-1]
        if col_index >= len(last_row):
            return None
        return last_row[col_index]

    def fetch_dataset(
        self,
        dataset_id: str,
        limit: Optional[int] = None,
        offset: int = 0,
        order: Optional[str] = None,
        order_column: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Récupère un dataset CDC via l'API Socrata (JSON).
        Mode sample : pagination offset, chargement en mémoire.
        """
        url = f"{self.base_url}/{dataset_id}.json"
        all_records: list[dict[str, Any]] = []
        current_offset = offset
        last_key: Optional[str] = None
        order_col = order_column or str(self.socrata_cfg.get("order_column", ":id"))

        while True:
            remaining = None if limit is None else max(limit - len(all_records), 0)
            if limit is not None and remaining == 0:
                break

            page_size = self._page_size(remaining)
            params = self._build_pagination_params(
                page_size,
                offset=current_offset,
                last_key=last_key,
                order_column=order_col,
                order=order,
            )

            logger.info(
                "API JSON %s — page_size=%d, offset=%d, keyset=%s",
                dataset_id,
                page_size,
                current_offset,
                last_key,
            )
            response = self._request_with_retry(url, params)
            batch = response.json()

            if not batch:
                break

            all_records.extend(batch)
            logger.info("Récupéré %d enregistrements (total: %d)", len(batch), len(all_records))

            if limit and len(all_records) >= limit:
                all_records = all_records[:limit]
                break

            if len(batch) < page_size:
                break

            if self._pagination_strategy() == "keyset":
                last_key = self._extract_keyset_value(batch[-1], order_col)
            else:
                current_offset += len(batch)
                max_offset = int(self.socrata_cfg.get("max_offset", 50000))
                if current_offset >= max_offset:
                    logger.warning(
                        "Limite offset Socrata atteinte (%d) pour %s — bascule keyset",
                        max_offset,
                        dataset_id,
                    )
                    last_key = self._extract_keyset_value(batch[-1], order_col)
                    self.socrata_cfg["pagination_strategy"] = "keyset"
                    current_offset = 0

            self._rate_limit_sleep()

        return all_records

    def fetch_dataset_to_file(
        self,
        dataset_id: str,
        output_path: Path,
        limit: Optional[int] = None,
        order_column: Optional[str] = None,
    ) -> int:
        """
        Télécharge un dataset JSON en streaming (NDJSON) vers un fichier.
        Évite l'accumulation en mémoire pour le mode full.
        """
        url = f"{self.base_url}/{dataset_id}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        total_rows = 0
        current_offset = 0
        last_key: Optional[str] = None
        order_col = order_column or str(self.socrata_cfg.get("order_column", ":id"))

        with output_path.open("w", encoding="utf-8") as handle:
            while True:
                remaining = None if limit is None else max(limit - total_rows, 0)
                if limit is not None and remaining == 0:
                    break

                page_size = self._page_size(remaining)
                params = self._build_pagination_params(
                    page_size,
                    offset=current_offset,
                    last_key=last_key,
                    order_column=order_col,
                )

                logger.info(
                    "API JSON stream %s → %s (total=%d, keyset=%s)",
                    dataset_id,
                    output_path.name,
                    total_rows,
                    last_key,
                )
                response = self._request_with_retry(url, params)
                batch = response.json()

                if not batch:
                    break

                for record in batch:
                    handle.write(json.dumps(record, ensure_ascii=False))
                    handle.write("\n")
                    total_rows += 1
                    if limit and total_rows >= limit:
                        break

                logger.info("Page JSON : %d lignes (total: %d)", len(batch), total_rows)

                if limit and total_rows >= limit:
                    break
                if len(batch) < page_size:
                    break

                if self._pagination_strategy() == "keyset":
                    last_key = self._extract_keyset_value(batch[-1], order_col)
                else:
                    current_offset += len(batch)

                self._rate_limit_sleep()

        return total_rows

    def fetch_as_csv(self, dataset_id: str, limit: Optional[int] = None) -> str:
        """Récupère un dataset au format CSV (mode sample, en mémoire)."""
        buffer = io.StringIO()
        self.fetch_as_csv_to_file(dataset_id, buffer, limit=limit)
        return buffer.getvalue()

    def fetch_as_csv_to_file(
        self,
        dataset_id: str,
        output: Path | io.StringIO,
        limit: Optional[int] = None,
        order_column: Optional[str] = None,
    ) -> int:
        """
        Télécharge un dataset CSV avec pagination complète.
        Écrit en streaming (fichier ou buffer) pour supporter 5GB+.
        """
        url = f"{self.base_url}/{dataset_id}.csv"
        total_rows = 0
        current_offset = 0
        last_key: Optional[str] = None
        first_page = True
        order_col = order_column or str(self.socrata_cfg.get("order_column", ":id"))

        if isinstance(output, Path):
            output.parent.mkdir(parents=True, exist_ok=True)
            file_handle = output.open("w", encoding="utf-8", newline="")
        else:
            file_handle = output

        try:
            while True:
                remaining = None if limit is None else max(limit - total_rows, 0)
                if limit is not None and remaining == 0:
                    break

                page_size = self._page_size(remaining)
                params = self._build_pagination_params(
                    page_size,
                    offset=current_offset,
                    last_key=last_key,
                    order_column=order_col,
                )

                logger.info(
                    "API CSV %s — page_size=%d, total=%d, keyset=%s",
                    dataset_id,
                    page_size,
                    total_rows,
                    last_key,
                )
                response = self._request_with_retry(url, params)
                content = response.text

                if not content.strip():
                    break

                lines = content.splitlines()
                data_rows = max(len(lines) - 1, 0)

                if first_page:
                    file_handle.write(content)
                    if not content.endswith("\n"):
                        file_handle.write("\n")
                    first_page = False
                elif data_rows > 0:
                    body = "\n".join(lines[1:])
                    file_handle.write(body)
                    if not body.endswith("\n"):
                        file_handle.write("\n")

                total_rows += data_rows
                logger.info("Page CSV : %d lignes (total: %d)", data_rows, total_rows)

                if limit and total_rows >= limit:
                    break
                if data_rows < page_size:
                    break

                if self._pagination_strategy() == "keyset":
                    last_key = self._extract_keyset_from_csv(content, order_col)
                    if not last_key:
                        break
                else:
                    current_offset += data_rows
                    max_offset = int(self.socrata_cfg.get("max_offset", 50000))
                    if current_offset >= max_offset:
                        logger.warning(
                            "Limite offset Socrata atteinte (%d) pour %s — bascule keyset",
                            max_offset,
                            dataset_id,
                        )
                        last_key = self._extract_keyset_from_csv(content, order_col)
                        self.socrata_cfg["pagination_strategy"] = "keyset"
                        current_offset = 0
                        if not last_key:
                            break

                self._rate_limit_sleep()
        finally:
            if isinstance(output, Path):
                file_handle.close()

        return total_rows

    def fetch_all_configured_datasets(self) -> dict[str, list[dict[str, Any]]]:
        """Récupère tous les datasets configurés dans datasets.yaml."""
        config = get_datasets_config()
        limits = get_mode_limits()
        max_rows = limits.get("max_rows_per_dataset")

        results = {}
        for key, dataset_cfg in config.get("datasets", {}).items():
            dataset_id = dataset_cfg["id"]
            order_column = dataset_cfg.get("order_column")
            try:
                records = self.fetch_dataset(
                    dataset_id,
                    limit=max_rows,
                    order_column=order_column,
                )
                results[key] = records
                logger.info("Dataset %s : %d enregistrements", key, len(records))
            except (requests.RequestException, KeyError) as e:
                logger.error("Erreur dataset %s : %s", key, e)
                results[key] = []

        return results

    @staticmethod
    def records_to_csv_string(records: list[dict[str, Any]]) -> str:
        """Convertit une liste de dicts en chaîne CSV."""
        if not records:
            return ""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
        return output.getvalue()

    @staticmethod
    def get_ingestion_metadata(
        source: str,
        filename: str,
        file_size: int,
        fmt: str,
        status: str = "success",
    ) -> dict[str, Any]:
        """Génère les métadonnées d'ingestion standard."""
        return {
            "source": source,
            "filename": filename,
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "file_size": file_size,
            "format": fmt,
            "status": status,
        }
