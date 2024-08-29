from pyspark.sql import SparkSession
from constants import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE_NAME
import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_session():
    session = SparkSession.builder.appName("postgres_connector").config("spark.jars",
                                                                        "postgresql-42.7.4.jar").getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    logger.info(
        f"Spark Session for Postgres Started.")
    return session


def close_session(session):
    try:
        session.stop()
        logger.info(
            f"Spark Session Closed Successfully.")
    except Exception as e:
        logger.error(f" Something went wrong while closing session: {e}")


def check_if_id_already_exists(session, db_table, id_col, id_val):
    try:
        logger.info(
            f"Checking if {id_col}: '{id_val}' exists in table '{db_table}' of database '{POSTGRES_DATABASE_NAME}'")
        df = session.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}") \
            .option("dbtable", f"(SELECT {id_col} FROM {db_table} WHERE {id_col} = '{id_val}') AS subquery") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        count = df.count()
        logger.info(f"{id_col}: '{id_val}' {'exists' if count > 0 else 'does not exist'} in the database.")
        return count
    except Exception as e:
        logger.error(f"Error checking if {id_val} exists: {e}")
    return None


def insert_df(df, db_table, id_val=None):
    try:
        logger.info(f"Inserting {id_val} in table {db_table}.")
        df.write.format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}") \
            .option("dbtable", db_table) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logger.info(f"Inserted {id_val} in table {db_table} successfully.")
    except Exception as e:
        logger.error(f"Error inserting data in {db_table}: {e}")


def remove_by_id(db_table, id_col, id_val):
    try:
        logger.info(
            f"Attempting to remove metadata for {id_col}: {id_val} from table {db_table} in database: {POSTGRES_DATABASE_NAME}.")

        conn = psycopg2.connect(
            dbname=POSTGRES_DATABASE_NAME,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )

        cur = conn.cursor()

        # Execute the delete query
        delete_query = f"DELETE FROM {db_table} WHERE {id_col} = %s"
        cur.execute(delete_query, (id_val,))

        # Commit the transaction
        conn.commit()
        cur.close()
        conn.close()

        logger.info(
            f"Successfully removed metadata for {id_col}: {id_val} from table {db_table} in database: {POSTGRES_DATABASE_NAME}.")
    except Exception as e:
        logger.error(f"Error in removing metadata for {id_val}: {e}")


def get_all_ids(session, db_table, id_col):
    logger.info(f"Fetching all {id_col} values from table {db_table}.")
    try:
        query = f"(SELECT {id_col} FROM {db_table}) AS subquery"
        df = session.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}") \
            .option("dbtable", query) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        ids = [row[id_col] for row in df.collect()]
        return ids
    except Exception as e:
        logger.error(f"Error fetching all {id_col} values: {e}")
    return []


def get_data_by_id(session, db_table, id_col, id_val):
    logger.info(f"Fetching data for {id_col}: {id_val} from table {db_table}.")
    try:
        query = f"(SELECT * from {db_table} where {id_col}='id_val') as subquery"
        df = session.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}") \
            .option("dbtable", query) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
    except Exception as e:
        logger.error(f"Error fetching data for {id_val}: {e}")
    return None
