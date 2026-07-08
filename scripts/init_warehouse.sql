-- Initialisation du Data Warehouse PostgreSQL pour la couche Gold
-- Schéma et vues pour dashboards BI

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metabase;

-- Table KPI : incidence par région
CREATE TABLE IF NOT EXISTS gold.kpi_incidence_by_region (
    id SERIAL PRIMARY KEY,
    region VARCHAR(100) NOT NULL,
    state VARCHAR(50),
    total_cases BIGINT DEFAULT 0,
    total_deaths BIGINT DEFAULT 0,
    incidence_rate DOUBLE PRECISION,
    mortality_rate DOUBLE PRECISION,
    report_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table KPI : cas par jour
CREATE TABLE IF NOT EXISTS gold.kpi_cases_per_day (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    state VARCHAR(50),
    new_cases BIGINT DEFAULT 0,
    cumulative_cases BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table KPI : décès par jour
CREATE TABLE IF NOT EXISTS gold.kpi_deaths_per_day (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    state VARCHAR(50),
    new_deaths BIGINT DEFAULT 0,
    cumulative_deaths BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table KPI : hospitalisations par région
CREATE TABLE IF NOT EXISTS gold.kpi_hospitalizations_by_region (
    id SERIAL PRIMARY KEY,
    region VARCHAR(100),
    state VARCHAR(50),
    hospitalized BIGINT DEFAULT 0,
    icu_patients BIGINT DEFAULT 0,
    report_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table KPI : vaccination vs cas
CREATE TABLE IF NOT EXISTS gold.kpi_vaccination_vs_cases (
    id SERIAL PRIMARY KEY,
    state VARCHAR(50),
    report_date DATE,
    doses_administered BIGINT DEFAULT 0,
    fully_vaccinated BIGINT DEFAULT 0,
    new_cases BIGINT DEFAULT 0,
    vaccination_rate DOUBLE PRECISION,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table KPI : évolution temporelle par État
CREATE TABLE IF NOT EXISTS gold.kpi_temporal_evolution_by_state (
    id SERIAL PRIMARY KEY,
    state VARCHAR(50) NOT NULL,
    report_date DATE NOT NULL,
    metric_name VARCHAR(100),
    metric_value DOUBLE PRECISION,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Métadonnées d'ingestion
CREATE TABLE IF NOT EXISTS gold.pipeline_metadata (
    id SERIAL PRIMARY KEY,
    layer VARCHAR(20) NOT NULL,
    job_name VARCHAR(100),
    source VARCHAR(200),
    records_processed BIGINT,
    records_rejected BIGINT,
    duration_seconds DOUBLE PRECISION,
    status VARCHAR(20),
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Vues pour dashboards
CREATE OR REPLACE VIEW gold.v_kpi_dashboard AS
SELECT
    c.report_date,
    c.state,
    c.new_cases,
    c.cumulative_cases,
    d.new_deaths,
    d.cumulative_deaths,
    v.doses_administered,
    v.fully_vaccinated,
    v.vaccination_rate
FROM gold.kpi_cases_per_day c
LEFT JOIN gold.kpi_deaths_per_day d
    ON c.report_date = d.report_date AND c.state = d.state
LEFT JOIN gold.kpi_vaccination_vs_cases v
    ON c.report_date = v.report_date AND c.state = v.state
ORDER BY c.report_date DESC, c.state;

CREATE OR REPLACE VIEW gold.v_regional_summary AS
SELECT
    region,
    state,
    SUM(total_cases) AS total_cases,
    SUM(total_deaths) AS total_deaths,
    AVG(incidence_rate) AS avg_incidence_rate,
    MAX(report_date) AS last_report_date
FROM gold.kpi_incidence_by_region
GROUP BY region, state
ORDER BY total_cases DESC;

CREATE OR REPLACE VIEW gold.v_temporal_trends AS
SELECT
    state,
    report_date,
    metric_name,
    metric_value,
  LAG(metric_value) OVER (PARTITION BY state, metric_name ORDER BY report_date) AS previous_value,
    metric_value - LAG(metric_value) OVER (PARTITION BY state, metric_name ORDER BY report_date) AS change
FROM gold.kpi_temporal_evolution_by_state
ORDER BY state, report_date;

-- Index pour performances
CREATE INDEX IF NOT EXISTS idx_cases_date_state ON gold.kpi_cases_per_day(report_date, state);
CREATE INDEX IF NOT EXISTS idx_deaths_date_state ON gold.kpi_deaths_per_day(report_date, state);
CREATE INDEX IF NOT EXISTS idx_temporal_state_date ON gold.kpi_temporal_evolution_by_state(state, report_date);
