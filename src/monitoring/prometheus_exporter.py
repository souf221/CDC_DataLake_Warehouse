"""
Serveur HTTP exposant les métriques Prometheus.
Endpoint : http://localhost:8000/metrics
"""

import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, start_http_server

from monitoring.metrics import JOB_DURATION, PipelineMetrics
from utils.config import get_data_mode
from utils.logger import get_logger

logger = get_logger(__name__)


def init_demo_metrics() -> None:
    """Initialise des métriques de démonstration pour le dashboard Grafana."""
    PipelineMetrics.set_pipeline_info("1.0.0", get_data_mode())

    layers = ["bronze", "silver", "gold"]
    datasets = [
        "covid_cases_weekly_state",
        "covid_vaccinations_jurisdiction",
        "provisional_flu_pneumonia_covid_deaths",
        "flu_vaccination_coverage",
    ]
    for layer in layers:
        PipelineMetrics.record_file_read(layer, "json", "success")
        PipelineMetrics.record_file_read(layer, "csv", "success")
        PipelineMetrics.record_bytes(layer, "read", 1_500_000)
        PipelineMetrics.record_bytes(layer, "write", 1_200_000)
        for dataset in datasets:
            PipelineMetrics.record_rows_inserted(layer, dataset, 926 if layer == "silver" else 1000)
            PipelineMetrics.set_freshness(layer, dataset, 1800)
        if layer == "silver":
            PipelineMetrics.record_rows_rejected(layer, datasets[0], 74, "quality")

    # Histogrammes / erreurs (sinon panneaux Grafana vides)
    for job in ("extract", "bronze", "silver", "gold", "warehouse"):
        JOB_DURATION.labels(layer="pipeline", job_name=job).observe(45.0 + len(job))
    PipelineMetrics.record_error("silver", "quality_check", "validation_warning")


def start_metrics_server(port: int | None = None) -> None:
    """Démarre le serveur de métriques Prometheus."""
    port = port or int(os.getenv("METRICS_PORT", "8000"))
    init_demo_metrics()
    start_http_server(port)
    logger.info("Serveur métriques Prometheus démarré sur le port %d", port)

    # Garder le processus actif
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Arrêt du serveur métriques")


if __name__ == "__main__":
    start_metrics_server()
