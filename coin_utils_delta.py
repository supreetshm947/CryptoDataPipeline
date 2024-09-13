import requests
import pandas as pd
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import Row
from utils import convert_iso_to_datetime
import logging
from constants import ( NUMBER_OF_COINS)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_coins():
    try:
        url = "https://api.coinpaprika.com/v1/coins"
        response = requests.get(url)
        response.raise_for_status()

        coins_data = response.json()
        coins = pd.DataFrame(coins_data)
        logger.info(f"Coins fetched: {len(coins)}")
        logger.info(f"Coins first 5 rows: {coins.head()}")
        return coins
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while retrieving coins: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred while retrieving coins: {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return pd.DataFrame()


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

def load_coin_metadata(session, n=int(NUMBER_OF_COINS)):
    coins = fetch_coins()
    logger.info(f"Number of coins to fetch: {n}")
    top_n = coins[(coins['rank'] > 0) & (coins['rank'] <= n)]
    top_n_new = coins[(coins['is_new']) & (coins['is_active'])].sort_values(by=['rank'], ascending=True).head(n)
    all_coins = pd.concat([top_n, top_n_new])

    logger.info(f"Number of coins to load: {len(all_coins)}")
    logger.info(f"First 5 coins to load: {all_coins.head()}")

    all_coins_metadata = [fetch_coin_meta_data(coin['id']) for _, coin in all_coins.iterrows()]

    # for meta_data in all_coins_metadata:
    #     insert_coin_metadata_in_db(meta_data, session)
    return all_coins_metadata


def insert_coin_metadata_in_db(coin_metadata, session):
    default_timestamp = '1970-01-01 00:00:00'
    default_string = ""
    default_boolean = False
    default_int = 0
    default_float = 0.0
    try:
        coin_row = Row(
            id=coin_metadata['id'],
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
        df = session.createDataFrame([coin_row])

        # Fix timestamp datatype
        df = df.withColumn('started_at', to_timestamp(col('started_at'), 'yyyy-MM-dd HH:mm:ss'))
        df = df.withColumn('last_updated', to_timestamp(col('last_updated'), 'yyyy-MM-dd HH:mm:ss'))

        # if not postgres.check_if_id_already_exists(session, POSTGRES_TABLE_META_DATA,
        #                                            POSTGRES_TABLE_META_DATA_ID, coin_metadata['id']):
        #     postgres.insert_df(df, POSTGRES_TABLE_META_DATA, coin_metadata['id'])
    except Exception as e:
        logger.error(f"Something went wrong while persisting metadata for {coin_metadata.get(id, 'id-XXX')}:{e}")