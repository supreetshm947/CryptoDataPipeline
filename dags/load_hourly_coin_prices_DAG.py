from set_project_path import set_project_path
set_project_path()

from constants import COIN_META_ID
from mylogger import get_logger
from coin_utils import get_all_active_coins, fetch_coin_tickers_data, insert_coin_price_in_db
from spark_connector.session_utils import get_spark_session
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

logger = get_logger()


def fetch_and_insert_coin_prices():
    session = get_spark_session()
    coins = get_all_active_coins(session, [COIN_META_ID])

    for coin in coins:
        coin_data = fetch_coin_tickers_data(coin[COIN_META_ID])
        coin = coin_data["id"]
        logger.info(f"Processing message for {coin}.")
        insert_coin_price_in_db(session, coin_price_data=coin_data)
        in_time = coin_data["last_updated"]
        logger.info(f"Successfully inserted price data for {coin} for {in_time}.")

    session.stop()

with DAG(
        'fetch_and_insert_coin_prices_hourly',
        schedule_interval=timedelta(hours=1),
        start_date=datetime.now()-timedelta(hours=10),
        catchup=False,
) as dag:
    hello_task = PythonOperator(
        task_id="fetch_and_insert_coin_prices_hourly",
        python_callable=fetch_and_insert_coin_prices
    )