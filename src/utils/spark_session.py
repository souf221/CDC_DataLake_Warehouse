"""
Factory Spark Session avec support Delta Lake et MinIO S3A.
"""

from pyspark.sql import SparkSession

from utils.config import Config
from utils.logger import get_logger

logger = get_logger(__name__)


def create_spark_session(app_name: str | None = None) -> SparkSession:
    """
    Crée une SparkSession configurée pour :
    - Delta Lake
    - MinIO (S3A)
  - Adaptive Query Execution
    """
    name = app_name or Config.SPARK_APP_NAME
    endpoint = Config.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")

    builder = (
        SparkSession.builder.appName(name)
        .master(Config.SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", Config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", Config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", Config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(Config.MINIO_SECURE).lower())
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession créée : %s (master=%s)", name, Config.SPARK_MASTER)
    return spark
