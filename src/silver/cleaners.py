"""
Fonctions de nettoyage des données CDC pour la couche Silver.
Dates, noms d'États, valeurs nulles, doublons, types numériques.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from utils.logger import get_logger

logger = get_logger(__name__)

# Mapping abréviations États US
US_STATE_MAPPING = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    "PR": "Puerto Rico", "GU": "Guam", "VI": "Virgin Islands",
}


def standardize_state_column(df: DataFrame, col_name: str = "state") -> DataFrame:
    """Normalise les noms d'États (abréviations → noms complets)."""
    if col_name not in df.columns:
        # Chercher des colonnes alternatives
        for alt in ["location", "jurisdiction", "state_name"]:
            if alt in df.columns:
                col_name = alt
                break
        else:
            return df

    mapping_expr = F.create_map(
        *[F.lit(x) for pair in US_STATE_MAPPING.items() for x in pair]
    )
    return df.withColumn(
        f"{col_name}_normalized",
        F.coalesce(mapping_expr[F.upper(F.col(col_name))], F.col(col_name)),
    )


def parse_date_columns(df: DataFrame, date_columns: list[str] | None = None) -> DataFrame:
    """Parse et standardise les colonnes de dates."""
    if date_columns is None:
        date_columns = [
            c for c in df.columns
            if any(kw in c.lower() for kw in ["date", "week", "time", "period"])
        ]

    for col in date_columns:
        if col in df.columns:
            df = df.withColumn(
                f"{col}_parsed",
                F.coalesce(
                    F.to_date(F.col(col), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                    F.to_date(F.col(col), "yyyy-MM-dd"),
                    F.to_date(F.col(col), "MM/dd/yyyy"),
                ),
            )
    return df


def cast_numeric_columns(df: DataFrame, numeric_columns: list[str] | None = None) -> DataFrame:
    """Convertit les colonnes numériques (cas, décès, hospitalisations)."""
    if numeric_columns is None:
        numeric_columns = [
            c for c in df.columns
            if any(kw in c.lower() for kw in [
                "case", "death", "hosp", "vaccin", "dose", "count", "total", "rate", "specimen"
            ])
        ]

    for col in numeric_columns:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.col(col).cast(DoubleType()),
            )
    return df


def handle_nulls(df: DataFrame, fill_numeric: float = 0.0) -> DataFrame:
    """Remplace les valeurs nulles : 0 pour numériques, 'Unknown' pour strings."""
    for field in df.schema.fields:
        if field.dataType.simpleString() in ("int", "bigint", "double", "float", "decimal"):
            df = df.fillna({field.name: fill_numeric})
        elif field.dataType.simpleString() == "string":
            df = df.fillna({field.name: "Unknown"})
    return df


def remove_duplicates(df: DataFrame, key_columns: list[str] | None = None) -> DataFrame:
    """Supprime les doublons."""
    if key_columns:
        existing = [c for c in key_columns if c in df.columns]
        if existing:
            before = df.count()
            df = df.dropDuplicates(existing)
            after = df.count()
            logger.info("Doublons supprimés : %d -> %d", before, after)
    else:
        before = df.count()
        df = df.dropDuplicates()
        after = df.count()
        logger.info("Doublons supprimés : %d -> %d", before, after)
    return df


def enforce_non_negative(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    """Assure que cas/décès/hospitalisations >= 0."""
    if columns is None:
        columns = [
            c for c in df.columns
            if any(kw in c.lower() for kw in ["case", "death", "hosp", "dose", "vaccin"])
        ]

    for col in columns:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.when(F.col(col) < 0, F.lit(0.0)).otherwise(F.col(col)),
            )
    return df


def clean_dataframe(df: DataFrame, dataset_key: str) -> DataFrame:
    """Pipeline de nettoyage complet pour un dataset."""
    logger.info("Nettoyage Silver : %s", dataset_key)
    df = standardize_state_column(df)
    df = parse_date_columns(df)
    df = cast_numeric_columns(df)
    df = handle_nulls(df)
    df = enforce_non_negative(df)

    # Clés de déduplication selon le dataset
    dedup_keys = {
        "covid_cases_weekly_state": ["start_date", "end_date", "state"],
        "covid_vaccinations_jurisdiction": ["date", "location"],
        "provisional_flu_pneumonia_covid_deaths": ["week_ending_date", "jurisdiction", "age_group"],
        "flu_vaccination_coverage": ["year_season", "month", "geography", "dimension"],
    }
    df = remove_duplicates(df, dedup_keys.get(dataset_key))

    df = df.withColumn("_silver_processed_at", F.current_timestamp())
    df = df.withColumn("_silver_dataset", F.lit(dataset_key))
    return df
