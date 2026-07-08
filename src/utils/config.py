"""
Configuration centralisée du pipeline.
Charge les variables d'environnement et les fichiers YAML.
"""

import os
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv

# Charger .env depuis la racine du projet
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def get_env(key: str, default: Optional[str] = None) -> str:
    """Récupère une variable d'environnement."""
    return os.getenv(key, default or "")


def get_data_mode() -> str:
    """Retourne le mode de données : sample ou full."""
    return get_env("DATA_MODE", "sample").lower()


def is_sample_mode() -> bool:
    return get_data_mode() == "sample"


def load_yaml_config(filename: str) -> dict[str, Any]:
    """Charge un fichier YAML depuis configs/."""
    config_path = PROJECT_ROOT / "configs" / filename
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_datasets_config() -> dict[str, Any]:
    return load_yaml_config("datasets.yaml")


def get_settings_config() -> dict[str, Any]:
    return load_yaml_config("settings.yaml")


def get_mode_limits() -> dict[str, Any]:
    """Retourne les limites selon le mode sample/full."""
    datasets_cfg = get_datasets_config()
    mode = get_data_mode()
    return datasets_cfg.get("modes", {}).get(mode, {})


def get_socrata_config() -> dict[str, Any]:
    """Retourne la configuration Socrata (pagination, timeouts, retries)."""
    settings = get_settings_config()
    mode = get_data_mode()
    defaults: dict[str, Any] = {
        "page_size": 50000,
        "max_offset": 50000,
        "order_column": ":id",
        "pagination_strategy": "offset",
        "request_timeout": 120,
        "rate_limit_delay": 0.3,
        "max_retries": 5,
        "retry_backoff": 2.0,
    }
    socrata_cfg = settings.get("socrata", {})
    mode_overrides = socrata_cfg.get("modes", {}).get(mode, {})
    merged = {**defaults, **{k: v for k, v in socrata_cfg.items() if k != "modes"}, **mode_overrides}
    return merged


def is_full_mode() -> bool:
    return get_data_mode() == "full"


def to_project_relative(path: Path) -> str:
    """Chemin relatif à la racine du projet (portable Docker / local)."""
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def resolve_project_path(path_str: str) -> Path | None:
    """Résout un chemin relatif ou absolu (y compris anciens chemins Docker)."""
    if not path_str:
        return None

    path = Path(path_str)
    candidates: list[Path] = []

    if path.is_absolute():
        candidates.append(path)
        for prefix in ("/opt/airflow/project/", "/opt/bitnami/spark/project/"):
            if path_str.startswith(prefix):
                candidates.append(PROJECT_ROOT / path_str[len(prefix):])
    else:
        candidates.append(PROJECT_ROOT / path)
        candidates.append(PROJECT_ROOT / path_str.lstrip("/"))

    candidates.append(PROJECT_ROOT / "data" / "raw" / path.name)

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


class Config:
    """Configuration typée du pipeline."""

    # MinIO
    MINIO_ENDPOINT = get_env("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = get_env("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = get_env("MINIO_SECRET_KEY", "minioadmin")
    MINIO_BUCKET = get_env("MINIO_BUCKET", "cdc-lakehouse")
    MINIO_SECURE = get_env("MINIO_SECURE", "false").lower() == "true"

    # Spark / Delta
    SPARK_MASTER = get_env("SPARK_MASTER", "local[*]")
    SPARK_APP_NAME = get_env("SPARK_APP_NAME", "cdc-lakehouse")
    BRONZE_PATH = get_env("BRONZE_PATH", f"s3a://{MINIO_BUCKET}/bronze")
    SILVER_PATH = get_env("SILVER_PATH", f"s3a://{MINIO_BUCKET}/silver")
    GOLD_PATH = get_env("GOLD_PATH", f"s3a://{MINIO_BUCKET}/gold")

    # PostgreSQL
    POSTGRES_HOST = get_env("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(get_env("POSTGRES_PORT", "5432"))
    POSTGRES_DB = get_env("POSTGRES_DB", "cdc_warehouse")
    POSTGRES_USER = get_env("POSTGRES_USER", "cdc_user")
    POSTGRES_PASSWORD = get_env("POSTGRES_PASSWORD", "cdc_password")

    # CDC API
    CDC_API_BASE_URL = get_env("CDC_API_BASE_URL", "https://data.cdc.gov/resource")
    CDC_APP_TOKEN = get_env("CDC_APP_TOKEN", "")

    # Monitoring
    PROMETHEUS_PORT = int(get_env("PROMETHEUS_PORT", "8000"))
    METRICS_PUSHGATEWAY = get_env("METRICS_PUSHGATEWAY", "http://localhost:9091")

    # Chemins locaux
    DATA_DIR = PROJECT_ROOT / "data"
    RAW_DIR = DATA_DIR / "raw"
    LOCAL_BRONZE = DATA_DIR / "bronze"
    LOCAL_SILVER = DATA_DIR / "silver"
    LOCAL_GOLD = DATA_DIR / "gold"

    @classmethod
    def postgres_url(cls) -> str:
        return (
            f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}"
            f"@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
        )
