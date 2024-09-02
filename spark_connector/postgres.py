import logging
from constants import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE_NAME
from mylogger import get_logger
from utils import exception_logger

logger = get_logger()

global_options = {
    "url": f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}",
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

@exception_logger("postgres.check_if_id_already_exists")
def check_if_id_already_exists(session, db_table, id_col, id_val):
    logger.info(
        f"Checking if {id_col}: '{id_val}' exists in table '{db_table}' of database '{POSTGRES_DATABASE_NAME}'")
    options = global_options.copy()
    options["dbtable"] = f"(SELECT {id_col} FROM {db_table} WHERE {id_col} = '{id_val}') AS subquery"
    df = session.read.format("jdbc") \
        .options(**options) \
        .load()
    count = df.count()
    logger.info(f"{id_col}: '{id_val}' {'exists' if count > 0 else 'does not exist'} in the database.")
    return count

@exception_logger("postgres.insert_df")
def insert_df(df, db_table, id_val=None):
    logger.info(f"Inserting {id_val} in table {db_table}.")
    options = global_options.copy()
    options["dbtable"] = db_table
    df.write.format("jdbc") \
        .options(**options) \
        .mode("append") \
        .save()
    logger.info(f"Inserted {id_val} in table {db_table} successfully.")

@exception_logger("postgres.get_data")
def get_data(session, db_table, columns=None, clauses=None):
    # Construct the base query
    columns = columns or ["*"]
    columns_str = ", ".join(columns)

    logger.info(f"Fetching {columns_str} values from table {db_table}.")

    base_query = f"SELECT {columns_str} FROM {db_table}"

    # Build the WHERE clause if conditions are provided
    if clauses:
        where_clause = " AND ".join([f"{col} = '{val}'" for col, val in clauses.items()])
        base_query += f" WHERE {where_clause}"

    # Wrap the query as a subquery
    query = f"({base_query}) AS subquery"

    options = global_options.copy()
    options["dbtable"] = query

    df = session.read.format("jdbc") \
        .options(**options) \
        .load()
    ids = [row.asDict() for row in df.collect()]
    return ids
