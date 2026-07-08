"""
Jobs PySpark pour la couche Bronze.
Lit les fichiers bruts (CSV, JSON, PDF) et les enregistre en Delta Lake sans transformation métier.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from bronze.metadata import BronzeMetadataManager
from utils.config import Config, resolve_project_path
from utils.logger import get_logger
from utils.spark_session import create_spark_session

logger = get_logger(__name__)


class BronzeLoader:
    """Charge les données brutes en couche Bronze (format Delta Lake)."""

    def __init__(self, spark: SparkSession | None = None) -> None:
        self.spark = spark or create_spark_session("bronze-loader")
        self.metadata_mgr = BronzeMetadataManager(self.spark)

    def load_csv(self, path: str, dataset_key: str, metadata: dict[str, Any]) -> DataFrame:
        """Lit un CSV et l'enregistre en Bronze Delta."""
        logger.info("Chargement CSV Bronze : %s", path)
        df = (
            self.spark.read.option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .csv(path)
        )
        df = self.metadata_mgr.enrich_dataframe(df, metadata)
        output_path = f"{Config.BRONZE_PATH}/structured/{dataset_key}"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
        logger.info("Bronze CSV sauvegardé : %s (%d colonnes)", output_path, len(df.columns))
        return df

    def load_json(self, path: str, dataset_key: str, metadata: dict[str, Any]) -> DataFrame:
        """Lit un JSON et l'enregistre en Bronze Delta."""
        logger.info("Chargement JSON Bronze : %s", path)
        df = self.spark.read.option("multiLine", "true").json(path)
        df = self.metadata_mgr.enrich_dataframe(df, metadata)
        output_path = f"{Config.BRONZE_PATH}/structured/{dataset_key}"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
        logger.info("Bronze JSON sauvegardé : %s", output_path)
        return df

    def load_json_from_local(self, local_path: str, dataset_key: str) -> DataFrame:
        """Charge un fichier JSON local en Bronze."""
        path = Path(local_path)
        metadata = {
            "source": f"local://{path}",
            "filename": path.name,
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "format": "json",
        }
        return self.load_json(str(path), dataset_key, metadata)

    def register_pdf_metadata(self, pdf_metadata_list: list[dict[str, Any]]) -> DataFrame:
        """
        Enregistre les métadonnées PDF en Bronze (le contenu binaire reste dans MinIO).
        Les PDF ne sont pas transformés en Bronze.
        """
        rows = []
        for meta in pdf_metadata_list:
            rows.append({
                "source": meta.get("source", ""),
                "filename": meta.get("filename", ""),
                "file_size": meta.get("file_size", 0),
                "format": "pdf",
                "category": meta.get("category", "mmwr"),
                "local_path": meta.get("local_path", ""),
                "status": meta.get("status", "success"),
            })

        df = self.spark.createDataFrame(rows)
        output_path = f"{Config.BRONZE_PATH}/unstructured/pdf_registry"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
        logger.info("Registre PDF Bronze : %d fichiers", len(rows))
        return df

    def load_from_manifest(self, manifest_path: str | None = None) -> dict[str, Any]:
        """Charge tous les fichiers listés dans le manifeste d'ingestion."""
        manifest_path = manifest_path or str(Config.RAW_DIR / "ingestion_manifest.json")
        start = datetime.now(timezone.utc)

        if not Path(manifest_path).exists():
            logger.error("Manifeste introuvable : %s", manifest_path)
            return {"status": "error", "message": "manifest not found"}

        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8-sig"))
        files = manifest.get("files", [])

        # Un seul format par dataset : CSV prioritaire sur JSON (évite conflits de schéma Delta)
        files_by_key: dict[str, dict[str, Any]] = {}
        for file_meta in files:
            if file_meta.get("format") == "pdf":
                continue
            key = file_meta.get("dataset_key", "")
            if not key:
                continue
            existing = files_by_key.get(key)
            if existing is None or (existing.get("format") == "json" and file_meta.get("format") == "csv"):
                files_by_key[key] = file_meta

        structured_files = list(files_by_key.values())
        metadata_records = []
        loaded_datasets = []
        errors = []

        for file_meta in structured_files:
            fmt = file_meta.get("format", "")
            resolved = resolve_project_path(file_meta.get("local_path", ""))
            dataset_key = file_meta.get("dataset_key", Path(file_meta.get("local_path", "")).stem)

            if resolved is None:
                errors.append(f"Fichier introuvable : {file_meta.get('local_path')}")
                continue

            local_path = str(resolved)

            try:
                if fmt == "json":
                    self.load_json(local_path, dataset_key, file_meta)
                    loaded_datasets.append(dataset_key)
                elif fmt == "csv":
                    self.load_csv(local_path, dataset_key, file_meta)
                    loaded_datasets.append(dataset_key)
                elif fmt == "pdf":
                    continue  # PDF traités séparément
                metadata_records.append(file_meta)
            except Exception as e:
                logger.error("Erreur chargement %s : %s", local_path, e)
                errors.append(str(e))

        # PDF registry
        pdf_files = [f for f in files if f.get("format") == "pdf"]
        if pdf_files:
            self.register_pdf_metadata(pdf_files)
            metadata_records.extend(pdf_files)

        # Métadonnées
        self.metadata_mgr.append_metadata(metadata_records)

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        result = {
            "status": "success" if not errors else "partial",
            "loaded_datasets": loaded_datasets,
            "total_files": len(files),
            "errors": errors,
            "duration_seconds": duration,
        }
        logger.info("Bronze load terminé : %s", result)
        return result


def main() -> None:
    loader = BronzeLoader()
    result = loader.load_from_manifest()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
