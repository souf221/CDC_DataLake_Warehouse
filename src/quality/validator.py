"""
Validateur de qualité des données Silver avec Great Expectations.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from quality.expectations import get_expectations_for_dataset
from utils.config import Config, get_datasets_config
from utils.logger import get_logger

logger = get_logger(__name__)


class SilverQualityValidator:
    """Exécute les contrôles qualité sur les données Silver."""

    def __init__(self) -> None:
        self.results_dir = Config.DATA_DIR / "quality_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def validate_dataframe(self, df: pd.DataFrame, dataset_key: str) -> dict[str, Any]:
        """
        Valide un DataFrame Pandas avec les règles définies.
        Utilise une validation manuelle compatible sans contexte GE complet.
        """
        expectations = get_expectations_for_dataset(dataset_key)
        results = []
        passed = 0
        failed = 0

        for exp in expectations:
            exp_type = exp["expectation_type"]
            kwargs = exp.get("kwargs", {})
            success = False
            details = {}

            try:
                if exp_type == "expect_table_row_count_to_be_between":
                    count = len(df)
                    min_val = kwargs.get("min_value", 0)
                    max_val = kwargs.get("max_value", float("inf"))
                    success = min_val <= count <= max_val
                    details = {"observed_value": count}

                elif exp_type == "expect_column_to_exist":
                    col = kwargs["column"]
                    success = col in df.columns
                    details = {"column": col, "exists": success}

                elif exp_type == "expect_column_values_to_not_be_null":
                    col = kwargs["column"]
                    mostly = kwargs.get("mostly", 1.0)
                    if col in df.columns:
                        non_null_rate = df[col].notna().mean()
                        success = non_null_rate >= mostly
                        details = {"non_null_rate": non_null_rate, "threshold": mostly}
                    else:
                        success = False

                elif exp_type == "expect_column_values_to_be_between":
                    col = kwargs["column"]
                    min_val = kwargs.get("min_value", float("-inf"))
                    max_val = kwargs.get("max_value", float("inf"))
                    mostly = kwargs.get("mostly", 1.0)
                    if col in df.columns:
                        numeric = pd.to_numeric(df[col], errors="coerce")
                        valid_rate = ((numeric >= min_val) & (numeric <= max_val)).mean()
                        success = valid_rate >= mostly
                        details = {"valid_rate": valid_rate, "threshold": mostly}
                    else:
                        success = False

                else:
                    success = True
                    details = {"note": f"Unhandled expectation: {exp_type}"}

            except Exception as e:
                success = False
                details = {"error": str(e)}

            result_entry = {
                "expectation": exp_type,
                "kwargs": kwargs,
                "success": success,
                "details": details,
            }
            results.append(result_entry)
            if success:
                passed += 1
            else:
                failed += 1

        return {
            "dataset": dataset_key,
            "total_expectations": len(expectations),
            "passed": passed,
            "failed": failed,
            "success_rate": passed / len(expectations) if expectations else 1.0,
            "results": results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def validate_silver_delta(self, spark, dataset_key: str) -> dict[str, Any]:
        """Valide un dataset Silver Delta via Spark → Pandas."""
        silver_path = f"{Config.SILVER_PATH}/{dataset_key}"
        try:
            df_spark = spark.read.format("delta").load(silver_path)
            pdf = df_spark.toPandas()
            return self.validate_dataframe(pdf, dataset_key)
        except Exception as e:
            logger.error("Erreur validation %s : %s", dataset_key, e)
            return {
                "dataset": dataset_key,
                "status": "error",
                "error": str(e),
            }

    def run_all(self, spark=None) -> dict[str, Any]:
        """Exécute tous les contrôles qualité."""
        if spark is None:
            from utils.spark_session import create_spark_session
            spark = create_spark_session("quality-validator")

        config = get_datasets_config()
        all_results = []

        for dataset_key in config.get("datasets", {}):
            result = self.validate_silver_delta(spark, dataset_key)
            all_results.append(result)

        # Sauvegarder le rapport
        report_path = self.results_dir / f"quality_report_{datetime.now():%Y%m%d_%H%M%S}.json"
        summary = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "datasets_validated": len(all_results),
            "total_passed": sum(r.get("passed", 0) for r in all_results),
            "total_failed": sum(r.get("failed", 0) for r in all_results),
            "results": all_results,
        }
        report_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        logger.info("Rapport qualité sauvegardé : %s", report_path)

        return summary


def main() -> None:
    validator = SilverQualityValidator()
    result = validator.run_all()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
