from set_project_path import set_project_path
set_project_path()

from coin_utils import load_coin_historic_data_in_db
from spark_connector.session_utils import get_spark_session
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from mylogger import get_logger

logger = get_logger()


def insert_historic_coin_prices():
    session = get_spark_session()
    # Todo implement a checker fucntion to see if historic data already exists
    load_coin_historic_data_in_db(session, datetime.now().date()-timedelta(days=364))

    session.stop()

with DAG(
        'insert_historic_coin_prices',
        schedule_interval=timedelta(days=1),
        start_date=datetime.now()-timedelta(hours=4),
        catchup=False,
) as dag:
    hello_task = PythonOperator(
        task_id="fetch_and_insert_coin_prices_hourly",
        python_callable=insert_historic_coin_prices
    )
insert_historic_coin_prices()