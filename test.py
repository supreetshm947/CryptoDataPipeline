from constants import POSTGRES_TABLE_META_DATA, POSTGRES_TABLE_META_DATA_ID
from spark_connector import postgres
from spark_connector.postgres import get_session

# session = postgres.get_session()
# coin_ids = postgres.get_all_ids(session, POSTGRES_TABLE_META_DATA, POSTGRES_TABLE_META_DATA_ID)
# postgres.close_session(session)
session = get_session()
coin_ids = postgres.get_all_ids(session, POSTGRES_TABLE_META_DATA,
                                                POSTGRES_TABLE_META_DATA_ID)