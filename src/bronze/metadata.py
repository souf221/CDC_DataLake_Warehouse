"""
Gestion des métadonnées d'ingestion pour la couche Bronze.
Enregistre source, filename, ingestion_time, file_size, format, status.
"""

from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from utils.config import Config
from utils.logger import get_logger

logger = get_logger(__name__)

BRONZE_METADATA_SCHEMA = StructType([
    StructField("source", StringType(), False),
    StructField("filename", StringType(), False),
    StructField("ingestion_time", TimestampType(), False),
    StructField("file_size", LongType(), True),
    StructField("format", StringType(), False),
    StructField("status", StringType(), False),
    StructField("dataset_key", StringType(), True),
    StructField("record_count", LongType(), True),
    StructField("object_name", StringType(), True),
])


class BronzeMetadataManager:
    """Gère la table de métadonnées Bronze."""

    METADATA_TABLE = f"{Config.BRONZE_PATH}/_metadata/ingestion_log"

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def create_metadata_df(self, records: list[dict[str, Any]]) -> DataFrame:
        """Crée un DataFrame à partir des métadonnées d'ingestion."""
        rows = []
        for rec in records:
            ingestion_time = rec.get("ingestion_time")
            if isinstance(ingestion_time, str):
                ingestion_time = datetime.fromisoformat(
                    ingestion_time.replace("Z", "+00:00")
                )
            elif ingestion_time is None:
                ingestion_time = datetime.now(timezone.utc)

            rows.append({
                "source": rec.get("source", "unknown"),
                "filename": rec.get("filename", "unknown"),
                "ingestion_time": ingestion_time,
                "file_size": rec.get("file_size", 0),
                "format": rec.get("format", "unknown"),
                "status": rec.get("status", "unknown"),
                "dataset_key": rec.get("dataset_key"),
                "record_count": rec.get("record_count"),
                "object_name": rec.get("object_name"),
            })

        return self.spark.createDataFrame(rows, schema=BRONZE_METADATA_SCHEMA)

    def append_metadata(self, records: list[dict[str, Any]]) -> int:
        """Ajoute des métadonnées à la table Delta Bronze."""
        if not records:
            return 0

        df = self.create_metadata_df(records)
        count = df.count()

        df.write.format("delta").mode("append").save(self.METADATA_TABLE)
        logger.info("Métadonnées Bronze : %d enregistrements ajoutés", count)
        return count

    def read_metadata(self) -> DataFrame:
        """Lit la table de métadonnées."""
        return self.spark.read.format("delta").load(self.METADATA_TABLE)

    @staticmethod
    def enrich_dataframe(df: DataFrame, metadata: dict[str, Any]) -> DataFrame:
        """Enrichit un DataFrame Bronze avec colonnes de métadonnées (sans modifier les données métier)."""
        ingestion_raw = metadata.get("ingestion_time", "")
        return (
            df.withColumn("_bronze_source", F.lit(metadata.get("source", "")))
            .withColumn("_bronze_filename", F.lit(metadata.get("filename", "")))
            .withColumn(
                "_bronze_ingestion_time",
                F.to_timestamp(F.lit(str(ingestion_raw))) if ingestion_raw else F.current_timestamp(),
            )
            .withColumn("_bronze_format", F.lit(metadata.get("format", "")))
        )
