import requests
import pandas as pd
import spark_connector.postgres as postgres
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import Row
from constants import (POSTGRES_TABLE_META_DATA, POSTGRES_TABLE_META_DATA_ID,
                       CASSANDRA_TABLE_CRYPTO_PRICE_DATA, NUMBER_OF_COINS, POSTGRES_TABLE_META_DATA_IS_MONITORED,
                       MINIO_PATH_COIN_META, COIN_META_ID, COIN_META_IS_MONITORED, MINIO_COIN_META_STORAGE_TYPE,
                       COIN_PRICE_COMPOSITE_KEY_READ_TIME, COIN_PRICE_COMPOSITE_KEY_ID, MINIO_PATH_COIN_PRICE,
                       MINIO_COIN_PRICE_STORAGE_TYPE)
from spark_connector import minio_utils
from utils import convert_iso_to_datetime, exception_logger
import spark_connector.cassandra as cassandra
import traceback

from mylogger import get_logger

logger = get_logger()

def fetch_coins():
    try:
        url = "https://api.coinpaprika.com/v1/coins"
        response = requests.get(url)
        response.raise_for_status()

        coins_data = response.json()
        coins = pd.DataFrame(coins_data)
        return coins
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while retrieving coins: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred while retrieving coins: {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return pd.DataFrame()


def fetch_coin_tickers_data(coin_id):
    try:
        response_ticker = requests.get(f"https://api.coinpaprika.com/v1/tickers/{coin_id}")
        response_ticker.raise_for_status()

        return response_ticker.json()

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while retrieving data for {coin_id}: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred while retrieving data for {coin_id}: {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return None


def fetch_coin_meta_data(coin_id):
    try:
        response_meta = requests.get(f"https://api.coinpaprika.com/v1/coins/{coin_id}")
        response_meta.raise_for_status()

        response_ticker = requests.get(f"https://api.coinpaprika.com/v1/tickers/{coin_id}")
        response_ticker.raise_for_status()

        return {**response_meta.json(), **response_ticker.json()}

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while retrieving data for {coin_id}: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred while retrieving data for {coin_id}: {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return None


# use this function to load coin metadata.
def load_coin_metadata(session, n=int(NUMBER_OF_COINS)):
    coins = fetch_coins()
    logger.info(f"Number of coins to fetch: {n}")
    top_n = coins[(coins['rank'] > 0) & (coins['rank'] <= n)]
    top_n_new = coins[(coins['is_new']) & (coins['is_active'])].sort_values(by=['rank'], ascending=True).head(n//2)
    all_coins = pd.concat([top_n, top_n_new])
    #
    all_coins_metadata = [fetch_coin_meta_data(coin['id']) for _, coin in all_coins.iterrows()]
    # import json
    # #
    # with open("../sample_json/meta_btc.json") as f:
    #     all_coins_metadata = [json.load(f)]

    insert_coin_metadata_in_db(all_coins_metadata, session)


@exception_logger("coin_utils.insert_coin_metadata_in_db")
def insert_coin_metadata_in_db(all_coins_metadata, session):
    default_timestamp = '1970-01-01 00:00:00'
    default_string = ""
    default_boolean = False
    default_int = 0
    default_float = 0.0
    coin_rows = []
    for coin_metadata in all_coins_metadata:
        coin_row = Row(
            coin_id=coin_metadata['id'],
            name=coin_metadata.get('name') if coin_metadata.get('name') is not None else default_string,
            symbol=coin_metadata.get('symbol') if coin_metadata.get('symbol') is not None else default_string,
            rank=int(coin_metadata.get('rank')) if coin_metadata.get('rank') is not None else default_int,
            is_new=bool(coin_metadata.get('is_new')) if coin_metadata.get('is_new') is not None else default_boolean,
            is_active=bool(coin_metadata.get('is_active')) if coin_metadata.get(
                'is_active') is not None else default_boolean,
            type=coin_metadata.get('type') if coin_metadata.get('type') is not None else default_string,
            description=coin_metadata.get('description') if coin_metadata.get(
                'description') is not None else default_string,
            started_at=coin_metadata.get('started_at').replace('T', ' ').replace('Z', '') if coin_metadata.get(
                'started_at') is not None else default_timestamp,
            development_status=coin_metadata.get('development_status') if coin_metadata.get(
                'development_status') is not None else default_string,
            hardware_wallet=bool(coin_metadata.get('hardware_wallet')) if coin_metadata.get(
                'hardware_wallet') is not None else default_boolean,
            proof_type=coin_metadata.get('proof_type') if coin_metadata.get(
                'proof_type') is not None else default_string,
            org_structure=coin_metadata.get('org_structure') if coin_metadata.get(
                'org_structure') is not None else default_string,
            hash_algorithm=coin_metadata.get('hash_algorithm') if coin_metadata.get(
                'hash_algorithm') is not None else default_string,
            total_supply=coin_metadata.get('total_supply') if coin_metadata.get(
                'total_supply') is not None else default_int,  # Needs periodic updates
            max_supply=coin_metadata.get('max_supply') if coin_metadata.get('max_supply') is not None else default_int,
            last_updated=coin_metadata.get('last_updated').replace('T', ' ').replace('Z', '') if coin_metadata.get(
                'last_updated') is not None else default_timestamp,
            price_usd=coin_metadata.get('quotes', {}).get('USD', {}).get('price') if coin_metadata.get('quotes',
                                                                                                       {}).get(
                'USD', {}).get('price') is not None else default_float,  # Needs periodic updates
            is_monitored=True
        )
        coin_rows.append(coin_row)
    df = session.createDataFrame(coin_rows)

    # Fix timestamp datatype
    df = df.withColumn('started_at', to_timestamp(col('started_at'), 'yyyy-MM-dd HH:mm:ss'))
    df = df.withColumn('last_updated', to_timestamp(col('last_updated'), 'yyyy-MM-dd HH:mm:ss'))

    minio_utils.insert_df(df, MINIO_COIN_META_STORAGE_TYPE, "rank", MINIO_PATH_COIN_META)


def fetch_coin_pricing_historic(coin_id, start_timestamp, interval='1d'):
    try:
        url = f"https://api.coinpaprika.com/v1/tickers/{coin_id}/historical?start={start_timestamp}&interval={interval}"
        response = requests.get(url)
        response.raise_for_status()

        coins_data = response.json()
        return coins_data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while retrieving coins: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred while retrieving coins: {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return pd.DataFrame()


@exception_logger("coin_utils.insert_coin_price_in_db")
def insert_coin_price_in_db(session, coin_price_data):
    date_time = convert_iso_to_datetime(coin_price_data["last_updated"])
    data = [
        Row(coin_id=coin_price_data["id"],
            read_timestamp=coin_price_data["last_updated"],
            date=date_time.date(),
            hour=date_time.hour,
            price=float(coin_price_data["quotes"]["USD"]["price"]),
            volume_24h=coin_price_data["quotes"]["USD"]["volume_24h"],
            market_cap=coin_price_data["quotes"]["USD"]["market_cap"]
            )
    ]
    df = session.createDataFrame(data)
    minio_utils.insert_df(df, MINIO_COIN_PRICE_STORAGE_TYPE,
                          [COIN_PRICE_COMPOSITE_KEY_ID, COIN_PRICE_COMPOSITE_KEY_READ_TIME], MINIO_PATH_COIN_PRICE)


@exception_logger("coin_utils.insert_historic_coin_price_in_db")
def insert_historic_coin_price_in_db(session, coin_id, coin_price_data):
    data_list = []
    for data in coin_price_data:
        date_time = convert_iso_to_datetime(data["timestamp"])
        data_list.append(
            Row(coin_id=coin_id,
                read_timestamp=data["timestamp"],
                date=date_time.date(),
                hour=date_time.hour,
                price=float(data["price"]),
                volume_24h=data["volume_24h"],
                market_cap=data["market_cap"]
                ),
        )
    df = session.createDataFrame(data_list)
    minio_utils.insert_df(df, MINIO_COIN_PRICE_STORAGE_TYPE,
                          [COIN_PRICE_COMPOSITE_KEY_ID, COIN_PRICE_COMPOSITE_KEY_READ_TIME], MINIO_PATH_COIN_PRICE)


@exception_logger("coin_utils.load_coin_historic_data_in_db")
def load_coin_historic_data_in_db(session, start_date, interval="1d"):
    # first fetch all coin ids from
    data = get_all_active_coins(session, [COIN_META_ID])

    data_existing = minio_utils.load_df(session, MINIO_COIN_PRICE_STORAGE_TYPE, MINIO_PATH_COIN_PRICE, columns=[COIN_META_ID])
    id_existing = set([row[COIN_META_ID] for row in data_existing])

    ids = set([row[COIN_META_ID] for row in data])
    ids = list(ids - id_existing)

    for coin_id in ids:
        coin_historic = fetch_coin_pricing_historic(coin_id, start_date, interval)
        # import json
        # with open("../sample_json/btc_historic_1year.json") as f:
        #     coin_historic = json.load(f)
        logger.info(f"Inserting Price data for {coin_id} from {start_date} for every {interval}.")
        insert_historic_coin_price_in_db(session, coin_id, coin_historic)


@exception_logger("coin_utils.get_all_active_coin_ids")
def get_all_active_coins(session, columns=None):
    clauses = {COIN_META_IS_MONITORED: True}
    data = minio_utils.load_df(session, MINIO_COIN_META_STORAGE_TYPE, MINIO_PATH_COIN_META, columns=columns,
                               filters=clauses)
    return data


from datetime import datetime, timedelta

# start_date = str((datetime.now() - timedelta(days=364)).date())
# load_coin_historic_data(start_date)

# load_coin_metadata()

# import json
#
# with open("sample_json/tickers_USDZ.json") as f:
#     sample_yr_historic = json.load(f)
# insert_coin_price_in_db(sample_yr_historic)

# with open("sample_json/btc_historic_1year.json") as f:
#      sample_yr_historic = json.load(f)
# insert_coin_price_in_db("btc-bitcoin", sample_yr_historic)
# cassandra.truncate_table(CASSANDRA_KEYSPACE, CASSANDRA_TABLE_CRYPTO_PRICE_DATA)

# with open("sample_json/meta_USDZ.json") as f:
#     sample_json = json.load(f)
#
# with open("sample_json/tickers_USDZ.json") as f:
#     sample_ticker = json.load(f)
# sample_coin_meta = {**sample_json, **sample_ticker}
# session = postgres.get_session()
# # postgres.remove_by_id(POSTGRES_TABLE_META_DATA,
# #              POSTGRES_TABLE_META_DATA_ID, sample_coin_meta['id'])
# insert_coin_metadata_in_db(sample_coin_meta, session)
# exists = postgres.check_if_id_already_exists(session, POSTGRES_TABLE_META_DATA,
#                                     POSTGRES_TABLE_META_DATA_ID, sample_coin_meta['id'])
# postgres.remove_by_id(POSTGRES_TABLE_META_DATA,
#              POSTGRES_TABLE_META_DATA_ID, sample_coin_meta['id'])
# postgres.close_session(session)
#
#
# session = cassandra.get_session()
#
# with open("sample_json/btc_historic.json") as f:
#     sample_json = json.load(f)
# insert_coin_price_in_db(sample_json[0], session, "btc-bitcoin")
# cassandra.truncate_table(CASSANDRA_KEYSPACE, CASSANDRA_TABLE_CRYPTO_PRICE_DATA)
# cassandra.close_session(session)
