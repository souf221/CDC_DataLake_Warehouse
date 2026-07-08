"""
API + frontend statique pour visualiser les KPIs Gold (PostgreSQL).
"""

from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text

from utils.config import Config
from utils.logger import get_logger

logger = get_logger(__name__)

FRONTEND_DIR = Path(__file__).resolve().parents[2] / "frontend"
engine = create_engine(Config.postgres_url())

app = FastAPI(title="CDC Lakehouse KPI Dashboard", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _query_df(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    out = df.copy()
    for col in out.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        out[col] = out[col].astype(str)
    return out.where(pd.notnull(out), None).to_dict(orient="records")


@app.get("/api/health")
def health() -> dict[str, str]:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ok", "database": Config.POSTGRES_DB}


@app.get("/api/kpi/summary")
def kpi_summary() -> dict[str, Any]:
    cases = _query_df(
        """
        SELECT
            COALESCE(SUM(new_cases), 0) AS total_new_cases,
            COUNT(DISTINCT state) AS states_count,
            MAX(report_date) AS latest_report_date
        FROM gold.kpi_cases_per_day
        """
    )
    deaths = _query_df(
        "SELECT COALESCE(SUM(new_deaths), 0) AS total_new_deaths FROM gold.kpi_deaths_per_day"
    )
    # Les doses sont des cumuls historiques par date/état.
    # On affiche le total national US (évite double comptage États + US).
    vacc = _query_df(
        """
        SELECT
            doses_administered AS total_doses,
            fully_vaccinated AS total_fully_vaccinated,
            report_date AS vaccination_snapshot_date
        FROM gold.kpi_vaccination_vs_cases
        WHERE UPPER(state) IN ('US', 'UNITED STATES')
        ORDER BY report_date DESC
        LIMIT 1
        """
    )
    if vacc.empty:
        vacc = _query_df(
            """
            WITH latest AS (
                SELECT state, MAX(report_date) AS max_date
                FROM gold.kpi_vaccination_vs_cases
                WHERE UPPER(state) NOT IN ('US', 'UNITED STATES', 'USA')
                GROUP BY state
            )
            SELECT
                SUM(v.doses_administered) AS total_doses,
                SUM(v.fully_vaccinated) AS total_fully_vaccinated,
                MAX(v.report_date) AS vaccination_snapshot_date
            FROM gold.kpi_vaccination_vs_cases v
            JOIN latest l
                ON v.state = l.state
               AND v.report_date = l.max_date
            """
        )
    result: dict[str, Any] = {}
    if not cases.empty:
        result.update(cases.iloc[0].to_dict())
    if not deaths.empty:
        result.update(deaths.iloc[0].to_dict())
    if not vacc.empty:
        result.update(vacc.iloc[0].to_dict())
    return result


@app.get("/api/kpi/top-states")
def top_states(limit: int = Query(10, ge=1, le=50)) -> list[dict[str, Any]]:
    df = _query_df(
        """
        SELECT state, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths
        FROM gold.v_kpi_dashboard
        GROUP BY state
        ORDER BY total_cases DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )
    return _records(df)


@app.get("/api/kpi/timeline")
def timeline(metric: str = Query("cases", pattern="^(cases|deaths)$")) -> list[dict[str, Any]]:
    if metric == "cases":
        df = _query_df(
            """
            SELECT report_date, SUM(new_cases) AS value
            FROM gold.kpi_cases_per_day
            GROUP BY report_date
            ORDER BY report_date
            """
        )
    else:
        df = _query_df(
            """
            SELECT report_date, SUM(new_deaths) AS value
            FROM gold.kpi_deaths_per_day
            GROUP BY report_date
            ORDER BY report_date
            """
        )
    return _records(df)


@app.get("/api/kpi/regional")
def regional_summary(limit: int = Query(20, ge=1, le=100)) -> list[dict[str, Any]]:
    df = _query_df(
        """
        SELECT region, state, total_cases, total_deaths, avg_incidence_rate, last_report_date
        FROM gold.v_regional_summary
        ORDER BY total_cases DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )
    return _records(df)


@app.get("/api/kpi/dashboard")
def dashboard_rows(limit: int = Query(50, ge=1, le=500)) -> list[dict[str, Any]]:
    df = _query_df(
        """
        SELECT report_date, state, new_cases, new_deaths,
               doses_administered, fully_vaccinated, vaccination_rate
        FROM gold.v_kpi_dashboard
        ORDER BY report_date DESC, state
        LIMIT :limit
        """,
        {"limit": limit},
    )
    return _records(df)


@app.get("/")
def index() -> FileResponse:
    index_path = FRONTEND_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Frontend introuvable")
    return FileResponse(index_path)


if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


def main() -> None:
    import os
    import uvicorn

    port = int(os.getenv("KPI_DASHBOARD_PORT", "8500"))
    logger.info("Démarrage KPI Dashboard sur http://0.0.0.0:%d", port)
    uvicorn.run("api.kpi_server:app", host="0.0.0.0", port=port, reload=False)


if __name__ == "__main__":
    main()
