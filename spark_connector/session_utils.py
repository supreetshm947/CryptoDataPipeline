from pyspark.sql import SparkSession
import logging
from constants import (MINIO_HOST, MINIO_PORT_API, MINIO_USER, MINIO_PASSWORD)
from mylogger import get_logger

logger = get_logger()


# org.apache.hadoop:hadoop-aws:3.3.4

def get_spark_session():
    # Create a single SparkSession
    # session = SparkSession.builder \
    #     .appName("MinIO Delta and Parquet Storage") \
    #     .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_HOST}:{MINIO_PORT_API}") \
    #     .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    #     .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
    #     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    #     .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
    #     .config("spark.jars.packages",
    #             "org.apache.hadoop:hadoop-aws:3.3.4,"
    #             "com.amazonaws:aws-java-sdk-bundle:1.12.500,"
    #             "io.delta:delta-core_2.12:2.1.0") \
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    #     .config("spark.network.timeout", "600s") \
    #     .config("spark.executor.heartbeatInterval", "60s") \
    #     .config("spark.executor.memory", "6g") \
    #     .config("spark.driver.memory", "4g") \
    #     .config("spark.sql.shuffle.partitions", "200") \
    #     .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    #     .getOrCreate()

    session = SparkSession.builder \
        .appName("Hadoop and Spark Integration") \
        .master("spark://localhost:7077") \
        .config("spark.driver.host", "localhost") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.2.1," \
                "com.google.guava:guava:27.0.1-jre," \
                "com.amazonaws:aws-java-sdk-bundle:1.12.500," \
                "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    session.sparkContext.setLogLevel("WARN")

    logger.info("Spark Session Started.")
    return session
