"""
Extraction de texte depuis les rapports PDF CDC/MMWR.
Produit du texte structuré JSON pour la couche Silver.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pdfplumber

from utils.config import Config, get_mode_limits
from utils.logger import get_logger

logger = get_logger(__name__)


class PDFTextExtractor:
    """Extrait le texte des PDF CDC pour analyse en Silver."""

    def __init__(self, output_dir: Path | None = None) -> None:
        self.output_dir = output_dir or Config.LOCAL_SILVER / "pdf_text"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_pages = get_mode_limits().get("max_pdf_pages")

    def extract_text(self, pdf_path: str | Path) -> dict[str, Any]:
        """Extrait le texte d'un PDF page par page."""
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF introuvable : {pdf_path}")

        pages_text = []
        total_chars = 0

        with pdfplumber.open(pdf_path) as pdf:
            max_p = self.max_pages or len(pdf.pages)
            for i, page in enumerate(pdf.pages[:max_p]):
                text = page.extract_text() or ""
                pages_text.append({
                    "page_number": i + 1,
                    "text": text,
                    "char_count": len(text),
                })
                total_chars += len(text)

        result = {
            "source_file": pdf_path.name,
            "source_path": str(pdf_path),
            "total_pages": len(pages_text),
            "total_characters": total_chars,
            "extraction_time": datetime.now(timezone.utc).isoformat(),
            "pages": pages_text,
            "full_text": "\n\n".join(p["text"] for p in pages_text),
        }
        logger.info(
            "PDF extrait : %s (%d pages, %d chars)",
            pdf_path.name, len(pages_text), total_chars,
        )
        return result

    def extract_all_from_directory(self, pdf_dir: Path | None = None) -> list[dict[str, Any]]:
        """Extrait le texte de tous les PDF d'un répertoire."""
        pdf_dir = pdf_dir or Config.RAW_DIR / "pdf"
        results = []

        for pdf_file in pdf_dir.glob("*.pdf"):
            try:
                extracted = self.extract_text(pdf_file)
                output_file = self.output_dir / f"{pdf_file.stem}.json"
                output_file.write_text(
                    json.dumps(extracted, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                extracted["output_path"] = str(output_file)
                results.append(extracted)
            except Exception as e:
                logger.error("Erreur extraction %s : %s", pdf_file.name, e)

        return results

    def to_spark_records(self, extractions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convertit les extractions en enregistrements plats pour Spark."""
        records = []
        for ext in extractions:
            records.append({
                "source_file": ext["source_file"],
                "total_pages": ext["total_pages"],
                "total_characters": ext["total_characters"],
                "extraction_time": ext["extraction_time"],
                "full_text": ext["full_text"][:10000],  # Tronquer pour Spark
                "text_preview": ext["full_text"][:500],
            })
        return records
