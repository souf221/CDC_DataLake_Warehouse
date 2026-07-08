"""
Client MinIO pour le stockage objet S3 local.
Gère l'upload/download des fichiers bruts en couche Bronze.
"""

import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

from minio import Minio
from minio.error import S3Error

from utils.config import Config
from utils.logger import get_logger

logger = get_logger(__name__)


class MinioClient:
    """Wrapper autour du client MinIO."""

    def __init__(self) -> None:
        endpoint = Config.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        self.client = Minio(
            endpoint,
            access_key=Config.MINIO_ACCESS_KEY,
            secret_key=Config.MINIO_SECRET_KEY,
            secure=Config.MINIO_SECURE,
        )
        self.bucket = Config.MINIO_BUCKET
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        """Crée le bucket s'il n'existe pas."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info("Bucket créé : %s", self.bucket)
        except S3Error as e:
            logger.error("Erreur création bucket : %s", e)
            raise

    def upload_file(
        self,
        local_path: Union[str, Path],
        object_name: str,
        content_type: Optional[str] = None,
    ) -> dict[str, Any]:
        """Upload un fichier local vers MinIO."""
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Fichier introuvable : {local_path}")

        self.client.fput_object(
            self.bucket,
            object_name,
            str(local_path),
            content_type=content_type,
        )
        stat = self.client.stat_object(self.bucket, object_name)
        metadata = {
            "source": str(local_path),
            "filename": local_path.name,
            "object_name": object_name,
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "file_size": stat.size,
            "format": local_path.suffix.lstrip("."),
            "status": "success",
        }
        logger.info("Upload OK : %s -> %s (%d bytes)", local_path.name, object_name, stat.size)
        return metadata

    def upload_bytes(
        self,
        data: bytes,
        object_name: str,
        content_type: str = "application/octet-stream",
    ) -> dict[str, Any]:
        """Upload des données binaires vers MinIO."""
        stream = io.BytesIO(data)
        self.client.put_object(
            self.bucket,
            object_name,
            stream,
            length=len(data),
            content_type=content_type,
        )
        metadata = {
            "filename": object_name.split("/")[-1],
            "object_name": object_name,
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "file_size": len(data),
            "format": object_name.split(".")[-1] if "." in object_name else "unknown",
            "status": "success",
        }
        logger.info("Upload bytes OK : %s (%d bytes)", object_name, len(data))
        return metadata

    def upload_json(self, data: Any, object_name: str) -> dict[str, Any]:
        """Upload des données JSON vers MinIO."""
        json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        return self.upload_bytes(json_bytes, object_name, "application/json")

    def download_file(self, object_name: str, local_path: Union[str, Path]) -> Path:
        """Télécharge un objet MinIO vers le disque local."""
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.client.fget_object(self.bucket, object_name, str(local_path))
        logger.info("Download OK : %s -> %s", object_name, local_path)
        return local_path

    def list_objects(self, prefix: str = "") -> list[str]:
        """Liste les objets dans le bucket avec un préfixe."""
        objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]

    def get_s3a_path(self, layer: str, dataset: str) -> str:
        """Retourne le chemin S3A pour Spark."""
        return f"s3a://{self.bucket}/{layer}/{dataset}"
