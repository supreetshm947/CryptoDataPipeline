from set_project_path import set_project_path
set_project_path()

from coin_utils import load_coin_metadata
from spark_connector.session_utils import get_spark_session
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


from mylogger import get_logger

logger = get_logger()


def fetch_and_insert_coin_metadata():
    session = get_spark_session()
    load_coin_metadata(session,20)
    logger.info("Inserted Metadata.")

    session.stop()

with DAG(
        'fetch_and_insert_coin_metadata',
        schedule_interval=timedelta(days=1),
        start_date=datetime.now()-timedelta(hours=4),
        catchup=False,
) as dag:
    hello_task = PythonOperator(
        task_id="fetch_and_insert_coin_prices_hourly",
        python_callable=fetch_and_insert_coin_metadata
    )

fetch_and_insert_coin_metadata()