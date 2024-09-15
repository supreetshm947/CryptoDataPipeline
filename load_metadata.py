import logging
import coin_utils_delta as coin_utils
from spark_connector.session_utils import get_spark_session
# from constants import (MINIO_KEY, MINIO_SECRET, MINO_BUCKET_COIN, MINO_BUCKET_REDDIT)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# global_options = {
#     "url": f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}",
#     "user": POSTGRES_USER,
#     "password": POSTGRES_PASSWORD,
#     "driver": "org.postgresql.Driver"
# }

def main():
    session = get_spark_session()

    coin_utils.load_coin_metadata(session)

    # Load metadata into postgres
    # coin_utils.load_coin_metadata(session)
    session.stop()

if __name__ == "__main__":
    
    main()
