#!/bin/bash
# =============================================================================
# Script de lancement du pipeline CDC Lakehouse étape par étape
# Usage : ./scripts/run_pipeline.sh [sample|full] [step]
# =============================================================================

set -euo pipefail

MODE="${1:-sample}"
STEP="${2:-all}"
export DATA_MODE="$MODE"
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"

echo "=============================================="
echo " CDC Data Lakehouse Pipeline"
echo " Mode: $MODE | Step: $STEP"
echo "=============================================="

run_extract() {
    echo ">>> [1/6] Extraction CDC..."
    python -m ingestion.extract
}

run_bronze() {
    echo ">>> [2/6] Chargement Bronze..."
    python -m bronze.bronze_loader
}

run_silver() {
    echo ">>> [3/6] Transformation Silver..."
    python -m silver.silver_transform
}

run_quality() {
    echo ">>> [4/6] Contrôles qualité..."
    python -m quality.validator
}

run_gold() {
    echo ">>> [5/6] Construction Gold KPIs..."
    python -m gold.gold_kpis
}

run_warehouse() {
    echo ">>> [6/6] Chargement Data Warehouse..."
    python -m gold.warehouse_loader
}

case "$STEP" in
    extract)   run_extract ;;
    bronze)    run_bronze ;;
    silver)    run_silver ;;
    quality)   run_quality ;;
    gold)      run_gold ;;
    warehouse) run_warehouse ;;
    all)
        run_extract
        run_bronze
        run_silver
        run_quality
        run_gold
        run_warehouse
        ;;
    *)
        echo "Étape inconnue: $STEP"
        echo "Usage: $0 [sample|full] [extract|bronze|silver|quality|gold|warehouse|all]"
        exit 1
        ;;
esac

echo ">>> Pipeline terminé avec succès !"
