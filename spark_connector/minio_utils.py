from utils import exception_logger
from constants import MINIO_COIN_META_BUCKET

from mylogger import get_logger

logger = get_logger()


@exception_logger("minio_utils.insert_df")
def insert_df(df, storage_type, id_column, minio_path):
    if isinstance(id_column, str):
        partition_columns = [id_column]
    else:
        partition_columns = id_column
    df.write \
        .format(storage_type) \
        .partitionBy(*partition_columns) \
        .mode("overwrite") \
        .save(minio_path)
    logger.info(f"Successfully written {df.count()} rows at {minio_path}.")


@exception_logger("minio_utils.load_df")
def load_df(session, storage_type, minio_path, columns=None, filters=None):
    df = session.read.format(storage_type).load(minio_path)

    if filters:
        for col, val in filters.items():
            df = df.filter(df[col] == val)

    if columns:
        df = df.select(columns)

    rows = df.collect()

    data = [row.asDict() for row in rows]

    return data
