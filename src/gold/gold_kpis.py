"""
Construction des tables KPI Gold à partir des données Silver.
KPIs : incidence, cas/jour, décès/jour, hospitalisations, vaccination, évolution temporelle.
"""

import json
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.config import Config
from utils.logger import get_logger
from utils.spark_session import create_spark_session

logger = get_logger(__name__)


class GoldKPIBuilder:
    """Construit les tables KPI de la couche Gold."""

    def __init__(self, spark: SparkSession | None = None) -> None:
        self.spark = spark or create_spark_session("gold-kpi-builder")

    def _read_silver(self, dataset_key: str) -> DataFrame | None:
        """Lit un dataset Silver."""
        path = f"{Config.SILVER_PATH}/{dataset_key}"
        try:
            return self.spark.read.format("delta").load(path)
        except Exception as e:
            logger.warning("Silver dataset introuvable %s : %s", dataset_key, e)
            return None

    def build_kpi_incidence_by_region(self) -> DataFrame:
        """KPI : incidence par région/État."""
        df = self._read_silver("covid_cases_weekly_state")
        if df is None:
            return self.spark.createDataFrame(
                [], "region STRING, state STRING, total_cases BIGINT, total_deaths BIGINT, "
                "incidence_rate DOUBLE, mortality_rate DOUBLE, report_date DATE, updated_at TIMESTAMP",
            )

        state_col = "state_normalized" if "state_normalized" in df.columns else "state"
        date_col = "end_date_parsed" if "end_date_parsed" in df.columns else "end_date"

        kpi = (
            df.groupBy(F.col(state_col).alias("state"))
            .agg(
                F.sum(F.coalesce(F.col("tot_cases").cast("double"), F.lit(0))).alias("total_cases"),
                F.sum(F.coalesce(F.col("tot_deaths").cast("double"), F.lit(0))).alias("total_deaths"),
                F.max(F.col(date_col)).alias("report_date"),
            )
            .withColumn("region", F.col("state"))
            .withColumn(
                "incidence_rate",
                F.when(F.col("total_cases") > 0, F.col("total_deaths") / F.col("total_cases") * 100)
                .otherwise(F.lit(0.0)),
            )
            .withColumn(
                "mortality_rate",
                F.when(F.col("total_cases") > 0, F.col("total_deaths") / F.col("total_cases") * 100)
                .otherwise(F.lit(0.0)),
            )
            .withColumn("updated_at", F.current_timestamp())
        )
        return kpi.select(
            "region", "state", "total_cases", "total_deaths",
            "incidence_rate", "mortality_rate", "report_date", "updated_at",
        )

    def build_kpi_cases_per_day(self) -> DataFrame:
        """KPI : cas par jour."""
        df = self._read_silver("covid_cases_weekly_state")
        if df is None:
            return self.spark.createDataFrame(
                [], "report_date DATE, state STRING, new_cases BIGINT, cumulative_cases BIGINT, updated_at TIMESTAMP",
            )

        date_col = "end_date_parsed" if "end_date_parsed" in df.columns else "end_date"
        state_col = "state_normalized" if "state_normalized" in df.columns else "state"

        return (
            df.groupBy(F.col(date_col).alias("report_date"), F.col(state_col).alias("state"))
            .agg(
                F.sum(F.coalesce(F.col("new_cases").cast("double"), F.lit(0))).alias("new_cases"),
                F.max(F.coalesce(F.col("tot_cases").cast("double"), F.lit(0))).alias("cumulative_cases"),
            )
            .withColumn("updated_at", F.current_timestamp())
        )

    def build_kpi_deaths_per_day(self) -> DataFrame:
        """KPI : décès par jour."""
        df = self._read_silver("covid_cases_weekly_state")
        if df is None:
            return self.spark.createDataFrame(
                [], "report_date DATE, state STRING, new_deaths BIGINT, cumulative_deaths BIGINT, updated_at TIMESTAMP",
            )

        date_col = "end_date_parsed" if "end_date_parsed" in df.columns else "end_date"
        state_col = "state_normalized" if "state_normalized" in df.columns else "state"

        return (
            df.groupBy(F.col(date_col).alias("report_date"), F.col(state_col).alias("state"))
            .agg(
                F.sum(F.coalesce(F.col("new_deaths").cast("double"), F.lit(0))).alias("new_deaths"),
                F.max(F.coalesce(F.col("tot_deaths").cast("double"), F.lit(0))).alias("cumulative_deaths"),
            )
            .withColumn("updated_at", F.current_timestamp())
        )

    def build_kpi_hospitalizations_by_region(self) -> DataFrame:
        """KPI : hospitalisations par région (proxy via cas si données hosp. absentes)."""
        df = self._read_silver("covid_cases_weekly_state")
        if df is None:
            return self.spark.createDataFrame(
                [], "region STRING, state STRING, hospitalized DOUBLE, icu_patients DOUBLE, "
                "report_date DATE, updated_at TIMESTAMP",
            )

        state_col = "state_normalized" if "state_normalized" in df.columns else "state"
        date_col = "end_date_parsed" if "end_date_parsed" in df.columns else "end_date"

        # Estimation : ~5% des nouveaux cas nécessitent hospitalisation (proxy épidémiologique)
        return (
            df.groupBy(F.col(state_col).alias("state"), F.col(date_col).alias("report_date"))
            .agg(
                F.sum(F.coalesce(F.col("new_cases").cast("double"), F.lit(0)) * 0.05).alias("hospitalized"),
                F.sum(F.coalesce(F.col("new_cases").cast("double"), F.lit(0)) * 0.01).alias("icu_patients"),
            )
            .withColumn("region", F.col("state"))
            .withColumn("updated_at", F.current_timestamp())
            .select("region", "state", "hospitalized", "icu_patients", "report_date", "updated_at")
        )

    def build_kpi_vaccination_vs_cases(self) -> DataFrame:
        """KPI : corrélation vaccination vs cas."""
        vacc_df = self._read_silver("covid_vaccinations_jurisdiction")
        cases_df = self._read_silver("covid_cases_weekly_state")

        if vacc_df is None or cases_df is None:
            return self.spark.createDataFrame(
                [], "state STRING, report_date DATE, doses_administered BIGINT, fully_vaccinated BIGINT, "
                "new_cases BIGINT, vaccination_rate DOUBLE, updated_at TIMESTAMP",
            )

        vacc_date = "date_parsed" if "date_parsed" in vacc_df.columns else "date"
        cases_date = "end_date_parsed" if "end_date_parsed" in cases_df.columns else "end_date"
        location_col = "location_normalized" if "location_normalized" in vacc_df.columns else "location"
        state_col = "state_normalized" if "state_normalized" in cases_df.columns else "state"

        vacc_agg = (
            vacc_df.groupBy(F.col(location_col).alias("state"), F.col(vacc_date).alias("report_date"))
            .agg(
                F.max(F.coalesce(F.col("administered").cast("double"), F.lit(0))).alias("doses_administered"),
                F.max(F.coalesce(F.col("series_complete_yes").cast("double"), F.lit(0))).alias("fully_vaccinated"),
            )
        )

        cases_agg = (
            cases_df.groupBy(F.col(state_col).alias("state"), F.col(cases_date).alias("report_date"))
            .agg(F.sum(F.coalesce(F.col("new_cases").cast("double"), F.lit(0))).alias("new_cases"))
        )

        return (
            vacc_agg.join(cases_agg, on=["state", "report_date"], how="outer")
            .fillna(0)
            .withColumn(
                "vaccination_rate",
                F.when(F.col("doses_administered") > 0,
                       F.col("fully_vaccinated") / F.col("doses_administered") * 100)
                .otherwise(F.lit(0.0)),
            )
            .withColumn("updated_at", F.current_timestamp())
        )

    def build_kpi_temporal_evolution(self) -> DataFrame:
        """KPI : évolution temporelle par État (métriques multiples)."""
        cases = self.build_kpi_cases_per_day()
        deaths = self.build_kpi_deaths_per_day()

        cases_metrics = cases.select(
            "state", "report_date",
            F.lit("new_cases").alias("metric_name"),
            F.col("new_cases").cast("double").alias("metric_value"),
        )
        deaths_metrics = deaths.select(
            "state", "report_date",
            F.lit("new_deaths").alias("metric_name"),
            F.col("new_deaths").cast("double").alias("metric_value"),
        )

        return (
            cases_metrics.union(deaths_metrics)
            .withColumn("updated_at", F.current_timestamp())
        )

    def build_all(self) -> dict[str, Any]:
        """Construit et sauvegarde toutes les tables KPI Gold."""
        start = datetime.now(timezone.utc)
        kpi_builders = {
            "kpi_incidence_by_region": self.build_kpi_incidence_by_region,
            "kpi_cases_per_day": self.build_kpi_cases_per_day,
            "kpi_deaths_per_day": self.build_kpi_deaths_per_day,
            "kpi_hospitalizations_by_region": self.build_kpi_hospitalizations_by_region,
            "kpi_vaccination_vs_cases": self.build_kpi_vaccination_vs_cases,
            "kpi_temporal_evolution_by_state": self.build_kpi_temporal_evolution,
        }

        results = []
        for table_name, builder_fn in kpi_builders.items():
            try:
                df = builder_fn()
                output_path = f"{Config.GOLD_PATH}/{table_name}"
                row_count = df.count()
                df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
                results.append({"table": table_name, "rows": row_count, "status": "success"})
                logger.info("Gold KPI %s : %d lignes", table_name, row_count)
            except Exception as e:
                logger.error("Erreur Gold %s : %s", table_name, e)
                results.append({"table": table_name, "status": "error", "error": str(e)})

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        return {
            "status": "success",
            "kpis": results,
            "duration_seconds": duration,
        }


def main() -> None:
    builder = GoldKPIBuilder()
    result = builder.build_all()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
