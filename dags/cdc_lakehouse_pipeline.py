"""
DAG Airflow principal : Pipeline CDC Data Lakehouse Medallion.
Étapes : extract → bronze → silver → quality → gold → warehouse → metrics
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule

# Configuration DAG
DEFAULT_ARGS = {
    "owner": "cdc-lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

PROJECT_SRC = "/opt/airflow/project/src"


def publish_pipeline_metrics(**context):
    """Publie les métriques du pipeline vers Prometheus."""
    import sys
    sys.path.insert(0, PROJECT_SRC)

    from monitoring.metrics import PipelineMetrics
    from utils.config import get_data_mode

    ti = context["ti"]
    extract_result = ti.xcom_pull(task_ids="extract_cdc_data") or {}
    bronze_result = ti.xcom_pull(task_ids="load_bronze") or {}
    silver_result = ti.xcom_pull(task_ids="transform_silver") or {}

    PipelineMetrics.set_pipeline_info("1.0.0", get_data_mode())
    PipelineMetrics.record_file_read("bronze", "mixed", "success")
    PipelineMetrics.record_rows_inserted(
        "silver",
        "all",
        silver_result.get("total_output_rows", 0) if isinstance(silver_result, dict) else 0,
    )
    PipelineMetrics.set_freshness("gold", "kpi_cases_per_day", 0)
    return {"status": "metrics_published"}


def run_extract(**context):
    """Exécute l'extraction CDC."""
    import sys
    sys.path.insert(0, PROJECT_SRC)
    from ingestion.extract import CDCExtractor
    return CDCExtractor().run()


def run_bronze(**context):
    """Charge les données en Bronze."""
    import sys
    sys.path.insert(0, PROJECT_SRC)
    from bronze.bronze_loader import BronzeLoader
    return BronzeLoader().load_from_manifest()


def run_silver(**context):
    """Transforme les données en Silver."""
    import sys
    sys.path.insert(0, PROJECT_SRC)
    from silver.silver_transform import SilverTransformer
    return SilverTransformer().run_all()


def run_quality(**context):
    """Exécute les contrôles qualité."""
    import sys
    sys.path.insert(0, PROJECT_SRC)
    from quality.validator import SilverQualityValidator
    return SilverQualityValidator().run_all()


def run_gold(**context):
    """Construit les KPIs Gold."""
    import sys
    sys.path.insert(0, PROJECT_SRC)
    from gold.gold_kpis import GoldKPIBuilder
    return GoldKPIBuilder().build_all()


def run_warehouse(**context):
    """Charge le Data Warehouse PostgreSQL."""
    import sys
    sys.path.insert(0, PROJECT_SRC)
    from gold.warehouse_loader import WarehouseLoader
    return WarehouseLoader().load_all()


with DAG(
    dag_id="cdc_lakehouse_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline Medallion CDC : Bronze → Silver → Gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cdc", "lakehouse", "medallion", "production"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    extract_cdc_data = PythonOperator(
        task_id="extract_cdc_data",
        python_callable=run_extract,
        doc_md="Extraction des données CDC (API JSON, CSV, PDF) vers MinIO Bronze.",
    )

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=run_bronze,
        doc_md="Chargement PySpark des fichiers bruts en tables Delta Bronze.",
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver,
        doc_md="Nettoyage et transformation des données en couche Silver.",
    )

    run_quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality,
        doc_md="Validation qualité Great Expectations sur Silver.",
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=run_gold,
        doc_md="Construction des KPIs métier en couche Gold.",
    )

    load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=run_warehouse,
        doc_md="Chargement des tables Gold dans PostgreSQL.",
    )

    publish_metrics = PythonOperator(
        task_id="publish_metrics",
        python_callable=publish_pipeline_metrics,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Publication des métriques Prometheus.",
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # Dépendances du pipeline
    start >> extract_cdc_data >> load_bronze >> transform_silver
    transform_silver >> run_quality_checks >> build_gold >> load_warehouse
    load_warehouse >> publish_metrics >> end
