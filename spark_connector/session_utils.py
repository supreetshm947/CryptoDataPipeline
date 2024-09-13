from pyspark.sql import SparkSession
import logging
from constants import (MINIO_KEY, MINIO_SECRET,MINIO_ENDPOINT)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_spark_session():
    # Create a single SparkSession
    session = SparkSession.builder \
        .appName("MultiDataSourceIntegration") \
        .config("spark.jars.packages",
                        "io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY) \
        .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET) \
        .getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session Started.")
    return session
