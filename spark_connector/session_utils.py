from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_spark_session():
    # Create a single SparkSession
    session = SparkSession.builder \
        .appName("MultiDataSourceIntegration") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.postgresql:postgresql:42.7.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.5.0") \
        .getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session Started.")
    return session
