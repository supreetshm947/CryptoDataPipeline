from pyspark.sql import SparkSession
import logging
from constants import CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_USER, CASSANDRA_PASSWORD
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_session():
    session = SparkSession.builder \
        .appName("CassandraIntegration") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
        .getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark Session for Cassandra Started.")
    return session


def close_session(session):
    try:
        session.stop()
        logger.info(
            f"Spark Session Closed Successfully.")
    except Exception as e:
        logger.error(f" Something went wrong while closing session: {e}")


def insert_df(df, keyspace, db_table, id_val=None):
    try:
        # Set up authentication and connect to the Cassandra cluster
        auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
        cluster = Cluster([CASSANDRA_HOST], CASSANDRA_PORT, auth_provider=auth_provider)
        session = cluster.connect()

        # Set the keyspace
        session.set_keyspace(keyspace)

        # Convert DataFrame to a list of dictionaries
        data = df.collect()

        # Insert data into the specified table
        for row in data:
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(row.asDict().keys())
            query = f"INSERT INTO {db_table} ({columns}) VALUES ({placeholders})"
            session.execute(query, tuple(row.asDict().values()))

        logger.info(f"Inserted {id_val} in table {db_table} successfully.")
    except Exception as e:
        logger.error(f"Error inserting data in {db_table}: {e}")
    finally:
        # Close the connection
        cluster.shutdown()


def truncate_table(keyspace, db_table):
    auth = PlainTextAuthProvider(CASSANDRA_USER, CASSANDRA_PASSWORD)
    cluster = Cluster([CASSANDRA_HOST], CASSANDRA_PORT, auth_provider=auth)
    session = cluster.connect()
    try:
        session.set_keyspace(keyspace)
        session.execute(f"TRUNCATE {db_table}")
        logger.info(f"All data deleted from table {keyspace}.{db_table}.")
    except Exception as e:
        logger.error(f"Error occurred while truncating table {keyspace}.{db_table}: {e}")
    finally:
        cluster.shutdown()
