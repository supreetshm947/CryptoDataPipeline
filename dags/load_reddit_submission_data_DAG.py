from set_project_path import set_project_path
set_project_path()

from sentiment_data import load_reddit_submission_for_coins_in_db
from mylogger import get_logger
from spark_connector.session_utils import get_spark_session
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

logger = get_logger()


def fetch_and_insert_reddit_submissions():
    session = get_spark_session()
    load_reddit_submission_for_coins_in_db(session, limit=5000)
    session.stop()

with DAG(
        'fetch_and_insert_reddit_submissions',
        schedule_interval=timedelta(hours=12),
        start_date=datetime.now()-timedelta(hours=2),
        catchup=False,
) as dag:
    hello_task = PythonOperator(
        task_id="fetch_and_insert_coin_prices_hourly",
        python_callable=fetch_and_insert_reddit_submissions
    )

fetch_and_insert_reddit_submissions()