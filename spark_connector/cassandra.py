from pyspark.sql import SparkSession
import logging
from constants import CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_USER, CASSANDRA_PASSWORD, CASSANDRA_KEYSPACE, \
    CASSANDRA_TABLE_CRYPTO_PRICE_DATA
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# def get_session():
#     # auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
#     # cluster = Cluster([CASSANDRA_HOST], CASSANDRA_PORT, auth_provider=auth_provider)
#     # session = cluster.connect()
#     # session.set_keyspace(keyspace)
#     # session = SparkSession.builder \
#     #     .appName("CassandraIntegration") \
#     #     .config("spark_connector.cassandra.connection.host", CASSANDRA_HOST) \
#     #     .config("spark_connector.cassandra.connection.port", CASSANDRA_PORT) \
#     #     .config("spark_connector.jars.packages", "com.datastax.spark_connector:spark_connector-cassandra-connector_2.12:3.2.0") \
#     #     .getOrCreate()
#     # session.sparkContext.setLogLevel("WARN")
#     session = SparkSession.builder \
#         .appName("CassandraIntegration") \
#         .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
#         .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
#         .config("spark.cassandra.auth.username", CASSANDRA_USER) \
#         .config("spark.cassandra.auth.password", CASSANDRA_PASSWORD) \
#         .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
#         .getOrCreate()
#     session.sparkContext.setLogLevel("WARN")
#     logger.info(f"Spark Session for Cassandra Started.")
#     return session


def insert_df(df, db_table, keyspace, id_val=None):
    try:
        # Convert DataFrame to a list of dictionaries
        # data = df.collect()
        #
        # # Insert data into the specified table
        # for row in data:
        #     placeholders = ', '.join(['%s'] * len(row))
        #     columns = ', '.join(row.asDict().keys())
        #     query = f"INSERT INTO {db_table} ({columns}) VALUES ({placeholders})"
        #     session.execute(query, tuple(row.asDict().values()))
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=db_table, keyspace=keyspace) \
            .option("spark.cassandra.connection.host", CASSANDRA_HOST) \
            .option("spark.cassandra.connection.port", CASSANDRA_PORT) \
            .option("spark.cassandra.auth.username", CASSANDRA_USER) \
            .option("spark.cassandra.auth.password", CASSANDRA_PASSWORD) \
            .options(table=CASSANDRA_TABLE_CRYPTO_PRICE_DATA, keyspace=CASSANDRA_KEYSPACE) \
            .save()
        logger.info(f"Inserted {id_val} in table {db_table} successfully.")
    except Exception as e:
        logger.error(f"Error inserting data in {db_table}: {e}")
        raise e


def truncate_table(session, db_table):
    try:
        session.execute(f"TRUNCATE {db_table}")
        logger.info(f"All data deleted from table {db_table}.")
    except Exception as e:
        logger.error(f"Error occurred while truncating table {db_table}: {e}")
        raise e
