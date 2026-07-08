"""
Point d'entrée principal pour l'extraction (Extract) des données CDC.
Orchestre API JSON, CSV et PDF → stockage local + MinIO Bronze.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingestion.cdc_api_client import CDCApiClient
from ingestion.cdc_csv_downloader import CDCCsvDownloader
from ingestion.cdc_pdf_downloader import CDCPdfDownloader
from utils.config import Config, get_data_mode, get_datasets_config, is_full_mode, resolve_project_path, to_project_relative
from utils.logger import get_logger
from utils.minio_client import MinioClient

logger = get_logger(__name__)


class CDCExtractor:
    """Orchestrateur d'extraction ELT - phase Extract."""

    def __init__(self) -> None:
        self.api_client = CDCApiClient()
        self.csv_downloader = CDCCsvDownloader()
        self.pdf_downloader = CDCPdfDownloader()
        self.minio = MinioClient()
        self.manifest_path = Config.RAW_DIR / "ingestion_manifest.json"

    def extract_json_datasets(self) -> list[dict[str, Any]]:
        """Extrait les datasets JSON via API Socrata (mode sample uniquement)."""
        if is_full_mode():
            logger.info("Mode full : extraction JSON ignorée (CSV paginé utilisé)")
            return []

        datasets = self.api_client.fetch_all_configured_datasets()
        results: list[dict[str, Any]] = []
        json_dir = Config.RAW_DIR / "json"
        json_dir.mkdir(parents=True, exist_ok=True)

        for key, records in datasets.items():
            if not records:
                continue
            filename = f"{key}.json"
            filepath = json_dir / filename
            filepath.write_text(
                json.dumps(records, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            metadata = CDCApiClient.get_ingestion_metadata(
                source=f"socrata://{get_datasets_config()['datasets'][key]['id']}",
                filename=filename,
                file_size=filepath.stat().st_size,
                fmt="json",
            )
            metadata["dataset_key"] = key
            metadata["record_count"] = len(records)
            metadata["local_path"] = to_project_relative(filepath)
            results.append(metadata)

        return results

    def extract_csv_datasets(self) -> list[dict[str, Any]]:
        """Extrait les datasets en format CSV."""
        return self.csv_downloader.download_all()

    def extract_pdf_reports(self) -> list[dict[str, Any]]:
        """Extrait les rapports PDF CDC/MMWR."""
        return self.pdf_downloader.download_all_configured()

    def upload_to_bronze(self, metadata_list: list[dict[str, Any]], layer: str = "bronze") -> list[dict[str, Any]]:
        """Upload les fichiers locaux vers MinIO (couche Bronze)."""
        uploaded = []
        for meta in metadata_list:
            resolved = resolve_project_path(meta.get("local_path", ""))
            if resolved is None:
                logger.warning("Fichier local introuvable : %s", meta.get("local_path"))
                continue
            local_path = str(resolved)

            fmt = meta.get("format", "unknown")
            dataset_key = meta.get("dataset_key", Path(local_path).stem)
            object_name = f"{layer}/{fmt}/{dataset_key}/{Path(local_path).name}"

            try:
                upload_meta = self.minio.upload_file(local_path, object_name)
                upload_meta.update(meta)
                upload_meta["local_path"] = to_project_relative(resolved)
                uploaded.append(upload_meta)
            except Exception as e:
                logger.error("Erreur upload %s : %s", local_path, e)
                meta["status"] = f"upload_error: {e}"
                uploaded.append(meta)

        return uploaded

    def run(self) -> dict[str, Any]:
        """Exécute l'extraction complète."""
        start = datetime.now(timezone.utc)
        logger.info("=== Démarrage extraction CDC (mode: %s) ===", get_data_mode())

        all_metadata: list[dict[str, Any]] = []

        # 1. JSON via API
        logger.info("--- Extraction JSON (API Socrata) ---")
        json_meta = self.extract_json_datasets()
        all_metadata.extend(json_meta)

        # 2. CSV
        logger.info("--- Extraction CSV ---")
        csv_meta = self.extract_csv_datasets()
        all_metadata.extend(csv_meta)

        # 3. PDF
        logger.info("--- Extraction PDF (MMWR) ---")
        pdf_meta = self.extract_pdf_reports()
        all_metadata.extend(pdf_meta)

        # 4. Upload vers MinIO Bronze
        logger.info("--- Upload vers MinIO (Bronze) ---")
        uploaded = self.upload_to_bronze(all_metadata)

        # Sauvegarder le manifeste
        manifest = {
            "extraction_time": start.isoformat(),
            "mode": get_data_mode(),
            "total_files": len(uploaded),
            "total_size_bytes": sum(m.get("file_size", 0) for m in uploaded),
            "files": uploaded,
        }
        self.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(
            "=== Extraction terminée : %d fichiers, %d bytes en %.1fs ===",
            manifest["total_files"],
            manifest["total_size_bytes"],
            duration,
        )
        return manifest


def main() -> None:
    """Point d'entrée CLI."""
    extractor = CDCExtractor()
    manifest = extractor.run()
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
