"""
Transformations Silver : nettoyage, validation, sauvegarde Delta Lake.
"""

import json
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession

from silver.cleaners import clean_dataframe
from silver.pdf_extractor import PDFTextExtractor
from utils.config import Config, get_datasets_config
from utils.logger import get_logger
from utils.spark_session import create_spark_session

logger = get_logger(__name__)


class SilverTransformer:
    """Transforme les données Bronze en Silver."""

    def __init__(self, spark: SparkSession | None = None) -> None:
        self.spark = spark or create_spark_session("silver-transformer")

    def transform_dataset(self, dataset_key: str) -> dict[str, Any]:
        """Transforme un dataset Bronze → Silver."""
        bronze_path = f"{Config.BRONZE_PATH}/structured/{dataset_key}"
        silver_path = f"{Config.SILVER_PATH}/{dataset_key}"

        try:
            df = self.spark.read.format("delta").load(bronze_path)
            input_count = df.count()
            df_clean = clean_dataframe(df, dataset_key)
            output_count = df_clean.count()

            df_clean.write.format("delta").mode("overwrite").save(silver_path)
            logger.info(
                "Silver %s : %d -> %d lignes",
                dataset_key, input_count, output_count,
            )
            return {
                "dataset": dataset_key,
                "input_rows": input_count,
                "output_rows": output_count,
                "rejected_rows": input_count - output_count,
                "status": "success",
            }
        except Exception as e:
            logger.error("Erreur Silver %s : %s", dataset_key, e)
            return {
                "dataset": dataset_key,
                "status": "error",
                "error": str(e),
            }

    def transform_pdf_text(self) -> dict[str, Any]:
        """Extrait et sauvegarde le texte PDF en Silver."""
        extractor = PDFTextExtractor()
        extractions = extractor.extract_all_from_directory()

        if not extractions:
            return {"status": "no_pdf", "count": 0}

        records = extractor.to_spark_records(extractions)
        df = self.spark.createDataFrame(records)
        silver_path = f"{Config.SILVER_PATH}/pdf_extracted_text"
        df.write.format("delta").mode("overwrite").save(silver_path)

        return {
            "status": "success",
            "pdf_count": len(extractions),
            "total_characters": sum(e["total_characters"] for e in extractions),
        }

    def run_all(self) -> dict[str, Any]:
        """Transforme tous les datasets configurés."""
        start = datetime.now(timezone.utc)
        config = get_datasets_config()
        results = []

        for dataset_key in config.get("datasets", {}):
            result = self.transform_dataset(dataset_key)
            results.append(result)

        pdf_result = self.transform_pdf_text()
        results.append({"dataset": "pdf_extracted_text", **pdf_result})

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        summary = {
            "status": "success",
            "transformations": results,
            "duration_seconds": duration,
            "total_output_rows": sum(r.get("output_rows", 0) for r in results),
        }
        logger.info("Silver terminé en %.1fs", duration)
        return summary


def main() -> None:
    transformer = SilverTransformer()
    result = transformer.run_all()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
