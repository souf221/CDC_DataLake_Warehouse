"""
Téléchargement de fichiers CSV depuis l'API CDC/Socrata.
Supporte la pagination complète en mode full (5GB+, streaming).
"""

from pathlib import Path
from typing import Any

from ingestion.cdc_api_client import CDCApiClient
from utils.config import Config, get_datasets_config, get_mode_limits, is_full_mode, to_project_relative
from utils.logger import get_logger

logger = get_logger(__name__)


class CDCCsvDownloader:
    """Télécharge et sauvegarde les datasets CDC en CSV local."""

    def __init__(self, output_dir: Path | None = None) -> None:
        self.output_dir = output_dir or Config.RAW_DIR / "csv"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.api_client = CDCApiClient()

    def download_all(self) -> list[dict[str, Any]]:
        """Télécharge tous les datasets configurés en CSV."""
        config = get_datasets_config()
        limits = get_mode_limits()
        max_rows = limits.get("max_rows_per_dataset")
        results: list[dict[str, Any]] = []

        for key, dataset_cfg in config.get("datasets", {}).items():
            results.append(self.download_dataset(key, dataset_cfg, max_rows))

        return results

    def download_dataset(
        self,
        dataset_key: str | None = None,
        dataset_cfg: dict[str, Any] | None = None,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        """Télécharge un dataset spécifique par sa clé de config."""
        config = get_datasets_config()
        if dataset_key is None:
            raise ValueError("dataset_key requis")

        dataset_cfg = dataset_cfg or config.get("datasets", {}).get(dataset_key)
        if not dataset_cfg:
            raise ValueError(f"Dataset inconnu : {dataset_key}")

        if max_rows is None:
            limits = get_mode_limits()
            max_rows = limits.get("max_rows_per_dataset")

        dataset_id = dataset_cfg["id"]
        order_column = dataset_cfg.get("order_column")
        filename = f"{dataset_key}_{dataset_id}.csv"
        filepath = self.output_dir / filename

        try:
            if is_full_mode():
                record_count = self.api_client.fetch_as_csv_to_file(
                    dataset_id,
                    filepath,
                    limit=max_rows,
                    order_column=order_column,
                )
                file_size = filepath.stat().st_size
            else:
                csv_content = self.api_client.fetch_as_csv(
                    dataset_id,
                    limit=max_rows,
                )
                filepath.write_text(csv_content, encoding="utf-8")
                file_size = len(csv_content.encode("utf-8"))
                record_count = max(csv_content.count("\n") - 1, 0)

            metadata = CDCApiClient.get_ingestion_metadata(
                source=f"socrata://{dataset_id}",
                filename=filename,
                file_size=file_size,
                fmt="csv",
            )
            metadata["dataset_key"] = dataset_key
            metadata["local_path"] = to_project_relative(filepath)
            metadata["record_count"] = record_count
            metadata["pagination"] = "keyset" if is_full_mode() else "offset"
            logger.info(
                "CSV téléchargé : %s (%d bytes, %d lignes)",
                filename,
                file_size,
                record_count,
            )
            return metadata
        except Exception as e:
            logger.error("Erreur téléchargement CSV %s : %s", dataset_key, e)
            return CDCApiClient.get_ingestion_metadata(
                source=f"socrata://{dataset_id}",
                filename=filename,
                file_size=0,
                fmt="csv",
                status=f"error: {e}",
            )
