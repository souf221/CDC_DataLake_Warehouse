"""
Métriques Prometheus pour le pipeline CDC Lakehouse.
Métriques : fichiers lus, lignes insérées/rejetées, durée jobs, volume, erreurs, freshness.
"""

import time
from contextlib import contextmanager
from typing import Generator, Optional

from prometheus_client import Counter, Gauge, Histogram, Info

# Compteurs
FILES_READ = Counter(
    "cdc_files_read_total",
    "Nombre total de fichiers lus",
    ["layer", "format", "status"],
)

ROWS_INSERTED = Counter(
    "cdc_rows_inserted_total",
    "Nombre total de lignes insérées",
    ["layer", "dataset"],
)

ROWS_REJECTED = Counter(
    "cdc_rows_rejected_total",
    "Nombre total de lignes rejetées",
    ["layer", "dataset", "reason"],
)

ERRORS = Counter(
    "cdc_errors_total",
    "Nombre total d'erreurs par couche",
    ["layer", "job_name", "error_type"],
)

BYTES_PROCESSED = Counter(
    "cdc_bytes_processed_total",
    "Volume de données traitées en bytes",
    ["layer", "direction"],  # direction: read | write
)

# Histogrammes
JOB_DURATION = Histogram(
    "cdc_job_duration_seconds",
    "Durée des jobs en secondes",
    ["layer", "job_name"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800],
)

# Jauges
DATA_FRESHNESS = Gauge(
    "cdc_data_freshness_seconds",
    "Âge des données les plus récentes en secondes",
    ["layer", "dataset"],
)

ACTIVE_JOBS = Gauge(
    "cdc_active_jobs",
    "Nombre de jobs actuellement en cours",
    ["layer"],
)

PIPELINE_INFO = Info(
    "cdc_pipeline",
    "Informations sur le pipeline CDC Lakehouse",
)


class PipelineMetrics:
    """Helper pour enregistrer les métriques du pipeline."""

    @staticmethod
    def record_file_read(layer: str, fmt: str, status: str = "success") -> None:
        FILES_READ.labels(layer=layer, format=fmt, status=status).inc()

    @staticmethod
    def record_rows_inserted(layer: str, dataset: str, count: int) -> None:
        ROWS_INSERTED.labels(layer=layer, dataset=dataset).inc(count)

    @staticmethod
    def record_rows_rejected(layer: str, dataset: str, count: int, reason: str = "quality") -> None:
        ROWS_REJECTED.labels(layer=layer, dataset=dataset, reason=reason).inc(count)

    @staticmethod
    def record_error(layer: str, job_name: str, error_type: str = "unknown") -> None:
        ERRORS.labels(layer=layer, job_name=job_name, error_type=error_type).inc()

    @staticmethod
    def record_bytes(layer: str, direction: str, count: int) -> None:
        BYTES_PROCESSED.labels(layer=layer, direction=direction).inc(count)

    @staticmethod
    def set_freshness(layer: str, dataset: str, age_seconds: float) -> None:
        DATA_FRESHNESS.labels(layer=layer, dataset=dataset).set(age_seconds)

    @staticmethod
    @contextmanager
    def track_job(layer: str, job_name: str) -> Generator[None, None, None]:
        """Context manager pour mesurer la durée d'un job."""
        ACTIVE_JOBS.labels(layer=layer).inc()
        start = time.time()
        try:
            yield
        except Exception:
            PipelineMetrics.record_error(layer, job_name, "exception")
            raise
        finally:
            duration = time.time() - start
            JOB_DURATION.labels(layer=layer, job_name=job_name).observe(duration)
            ACTIVE_JOBS.labels(layer=layer).dec()

    @staticmethod
    def set_pipeline_info(version: str, mode: str) -> None:
        PIPELINE_INFO.info({"version": version, "mode": mode})
