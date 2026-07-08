#!/bin/bash
# Lance le pipeline complet dans le conteneur Spark (environnement Docker cohérent)
# Usage : ./scripts/run_pipeline_docker.sh [sample|full] [step]

set -euo pipefail

MODE="${1:-sample}"
STEP="${2:-all}"
SERVICE="${PIPELINE_DOCKER_SERVICE:-spark-master}"

export DATA_MODE="$MODE"
export PYTHONPATH="/opt/bitnami/spark/project/src:/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/pyspark.zip:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip"
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
export POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
export SPARK_MASTER="${SPARK_MASTER:-local[*]}"

run_step() {
    docker compose exec \
        -e DATA_MODE \
        -e PYTHONPATH \
        -e MINIO_ENDPOINT \
        -e MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}" \
        -e MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}" \
        -e MINIO_BUCKET="${MINIO_BUCKET:-cdc-lakehouse}" \
        -e POSTGRES_HOST \
        -e POSTGRES_PORT="${POSTGRES_PORT:-5432}" \
        -e POSTGRES_DB="${POSTGRES_DB:-cdc_warehouse}" \
        -e POSTGRES_USER="${POSTGRES_USER:-cdc_user}" \
        -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-cdc_password}" \
        -e SPARK_MASTER \
        -e CDC_APP_TOKEN="${CDC_APP_TOKEN:-}" \
        "$SERVICE" \
        bash -c "pip3 install -q minio requests pyyaml python-dotenv boto3 pandas psycopg2-binary sqlalchemy pypdf pdfplumber prometheus-client great-expectations==0.18.12 delta-spark==3.1.0 2>/dev/null || true; python3 -m $1"
}

case "$STEP" in
    extract)   run_step ingestion.extract ;;
    bronze)    run_step bronze.bronze_loader ;;
    silver)    run_step silver.silver_transform ;;
    quality)   run_step quality.validator ;;
    gold)      run_step gold.gold_kpis ;;
    warehouse) run_step gold.warehouse_loader ;;
    all)
        run_step ingestion.extract
        run_step bronze.bronze_loader
        run_step silver.silver_transform
        run_step quality.validator
        run_step gold.gold_kpis
        run_step gold.warehouse_loader
        ;;
    *)
        echo "Étape inconnue: $STEP"
        exit 1
        ;;
esac

echo ">>> Pipeline Docker terminé avec succès !"
