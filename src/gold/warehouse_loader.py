"""
Chargement des tables Gold Delta Lake vers PostgreSQL Data Warehouse.
"""

import json
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text

from utils.config import Config
from utils.logger import get_logger
from utils.spark_session import create_spark_session

logger = get_logger(__name__)

GOLD_TABLES = [
    "kpi_incidence_by_region",
    "kpi_cases_per_day",
    "kpi_deaths_per_day",
    "kpi_hospitalizations_by_region",
    "kpi_vaccination_vs_cases",
    "kpi_temporal_evolution_by_state",
]


class WarehouseLoader:
    """Charge les KPIs Gold dans PostgreSQL."""

    def __init__(self, spark: SparkSession | None = None) -> None:
        self.spark = spark or create_spark_session("warehouse-loader")
        self.engine = create_engine(Config.postgres_url())

    def load_table(self, table_name: str, mode: str = "overwrite") -> dict[str, Any]:
        """Charge une table Gold vers PostgreSQL."""
        gold_path = f"{Config.GOLD_PATH}/{table_name}"

        try:
            df = self.spark.read.format("delta").load(gold_path)
            row_count = df.count()

            # Convertir en Pandas pour chargement PostgreSQL
            pdf = df.toPandas()

            # Normaliser les timestamps pour PostgreSQL
            for col in pdf.select_dtypes(include=["datetime64", "datetimetz"]).columns:
                pdf[col] = pdf[col].dt.tz_localize(None)

            # Vider la table sans supprimer les vues SQL dépendantes
            with self.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE gold.{table_name} RESTART IDENTITY"))

            pdf.to_sql(
                table_name,
                self.engine,
                schema="gold",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            logger.info("Warehouse %s : %d lignes chargées", table_name, row_count)
            return {"table": table_name, "rows": row_count, "status": "success"}

        except Exception as e:
            logger.error("Erreur warehouse %s : %s", table_name, e)
            return {"table": table_name, "status": "error", "error": str(e)}

    def log_pipeline_metadata(self, layer: str, job_name: str, stats: dict[str, Any]) -> None:
        """Enregistre les métadonnées d'exécution du pipeline."""
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO gold.pipeline_metadata
                    (layer, job_name, source, records_processed, records_rejected, duration_seconds, status)
                    VALUES (:layer, :job_name, :source, :records_processed, :records_rejected, :duration, :status)
                """),
                {
                    "layer": layer,
                    "job_name": job_name,
                    "source": stats.get("source", "cdc_pipeline"),
                    "records_processed": stats.get("records_processed", 0),
                    "records_rejected": stats.get("records_rejected", 0),
                    "duration": stats.get("duration_seconds", 0),
                    "status": stats.get("status", "success"),
                },
            )
            conn.commit()

    def load_all(self) -> dict[str, Any]:
        """Charge toutes les tables Gold."""
        start = datetime.now(timezone.utc)
        results = []

        for table_name in GOLD_TABLES:
            result = self.load_table(table_name)
            results.append(result)

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        total_rows = sum(r.get("rows", 0) for r in results)

        summary = {
            "status": "success",
            "tables": results,
            "total_rows": total_rows,
            "duration_seconds": duration,
        }

        self.log_pipeline_metadata("gold", "load_warehouse", {
            "records_processed": total_rows,
            "duration_seconds": duration,
            "status": "success",
        })

        return summary


def main() -> None:
    loader = WarehouseLoader()
    result = loader.load_all()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
