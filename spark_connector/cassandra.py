from pyspark.sql import SparkSession
import logging
from constants import CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_USER, CASSANDRA_PASSWORD, CASSANDRA_KEYSPACE, \
    CASSANDRA_TABLE_CRYPTO_PRICE_DATA
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def insert_df(df, db_table, keyspace, id_val=None):
    try:
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
