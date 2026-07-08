"""
Téléchargement de rapports PDF CDC/MMWR.
Données non structurées pour extraction texte en couche Silver.
"""

from pathlib import Path
from typing import Any

import requests

from utils.config import Config, get_datasets_config, get_mode_limits, to_project_relative
from utils.logger import get_logger

logger = get_logger(__name__)


class CDCPdfDownloader:
    """Télécharge les rapports PDF publics CDC/MMWR."""

    def __init__(self, output_dir: Path | None = None) -> None:
        self.output_dir = output_dir or Config.RAW_DIR / "pdf"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "CDC-Lakehouse-ETL/1.0 (Educational Project)"

    def download_pdf(self, url: str, name: str) -> dict[str, Any]:
        """Télécharge un PDF depuis une URL CDC."""
        filename = f"{name}.pdf"
        filepath = self.output_dir / filename

        logger.info("Téléchargement PDF : %s", url)
        response = self.session.get(url, timeout=120, stream=True)
        response.raise_for_status()

        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        file_size = filepath.stat().st_size
        metadata = {
            "source": url,
            "filename": filename,
            "local_path": to_project_relative(filepath),
            "file_size": file_size,
            "format": "pdf",
            "status": "success",
        }
        logger.info("PDF téléchargé : %s (%d bytes)", filename, file_size)
        return metadata

    def download_all_configured(self) -> list[dict[str, Any]]:
        """Télécharge tous les PDF configurés dans datasets.yaml."""
        config = get_datasets_config()
        results: list[dict[str, Any]] = []

        for pdf_cfg in config.get("pdf_reports", []):
            try:
                metadata = self.download_pdf(pdf_cfg["url"], pdf_cfg["name"])
                metadata["category"] = pdf_cfg.get("category", "mmwr")
                results.append(metadata)
            except requests.RequestException as e:
                logger.error("Erreur PDF %s : %s", pdf_cfg["name"], e)
                results.append({
                    "source": pdf_cfg["url"],
                    "filename": f"{pdf_cfg['name']}.pdf",
                    "file_size": 0,
                    "format": "pdf",
                    "status": f"error: {e}",
                })

        return results
