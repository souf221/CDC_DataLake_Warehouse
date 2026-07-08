"""
Définition des expectations Great Expectations pour la couche Silver.
Validations : colonnes obligatoires, non-null, dates valides, valeurs >= 0.
"""

from typing import Any

from utils.logger import get_logger

logger = get_logger(__name__)

# Suites d'expectations par dataset
EXPECTATION_SUITES: dict[str, list[dict[str, Any]]] = {
    "covid_cases_weekly_state": [
        {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}},
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "state"}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "state", "mostly": 0.95}},
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "new_cases", "min_value": 0, "mostly": 0.99},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "new_deaths", "min_value": 0, "mostly": 0.99},
        },
    ],
    "covid_vaccinations_jurisdiction": [
        {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}},
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "location"}},
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "administered", "min_value": 0, "mostly": 0.95},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "series_complete_yes", "min_value": 0, "mostly": 0.95},
        },
    ],
    "provisional_flu_pneumonia_covid_deaths": [
        {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}},
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "jurisdiction"}},
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "covid_19_deaths", "min_value": 0, "mostly": 0.99},
        },
    ],
    "flu_vaccination_coverage": [
        {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}},
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "geography"}},
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "coverage_estimate", "min_value": 0, "max_value": 100, "mostly": 0.95},
        },
    ],
}


def get_expectations_for_dataset(dataset_key: str) -> list[dict[str, Any]]:
    """Retourne les expectations pour un dataset."""
    return EXPECTATION_SUITES.get(dataset_key, [
        {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}},
    ])
