from constants import ELASTIC_HOST, ELASTIC_PORT, ELASTIC_USERNAME, ELASTIC_PASSWORD, \
    ELASTIC_REDDIT_INDEX_NAME
from elasticsearch import Elasticsearch

from constants import ELASTIC_USERNAME, ELASTIC_PASSWORD, ELASTIC_PORT, ELASTIC_HOST
from mylogger import get_logger
from utils import exception_logger

logger = get_logger()

# def add_index():
#     es = Elasticsearch(
#         [{'host': ELASTIC_HOST, 'port': ELASTIC_PORT, 'scheme': 'http'}],
#         basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD)
#     )
#
#     # Define the index name and mapping
#     index_name = ELASTIC_INDEX_NAME
#     mapping = {
#         'mappings': {
#             'properties': {
#                 'tweet_id': {'type': 'keyword'},
#                 'created_at': {'type': 'date'},
#                 'hashtags': {'type': 'keyword'},
#                 'coin': {'type': 'keyword'},
#                 'symbol': {'type': 'keyword'}
#             }
#         }
#     }
#
#     # Create index if it doesn't exist
#     if not es.indices.exists(index=index_name):
#         es.indices.create(index=index_name, body=mapping)
#         logger.info(f"Index '{index_name}' for Elasticsearch created.")
#     else:
#         logger.info(f"Index '{index_name}' for Elasticsearch already exists.")

@exception_logger("elastic.get_elastic_session")
def get_elastic_session():
    es = Elasticsearch(
        [{'host': ELASTIC_HOST, 'port': int(ELASTIC_PORT), 'scheme': 'http'}],
        basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD)
    )
    return es

@exception_logger("elastic.create_reddit_index")
def create_reddit_index(es):
    index_name = ELASTIC_REDDIT_INDEX_NAME
    # Define the index settings and mappings
    settings = {
        "mappings": {
            "properties": {
                "coin_id": {"type": "keyword"},
                "symbol": {"type": "keyword"},
                "title": {"type": "text"},
                "selftext": {"type": "text"},
                "score": {"type": "integer"},
                "num_comments": {"type": "integer"},
                "created_utc": {"type": "date"},
                "sentiment_score": {"type": "float"}
            }
        }
    }

    # Check if the index already exists
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=settings)
        logger.info(f"Index '{index_name}' created.")
    else:
        logger.info(f"Index '{index_name}' already exists.")

def delete_index(es, index_name=None):
    try:
        if index_name:
            es.indices.delete(index=index_name, ignore=[400, 404])
            logger.info(f"Index '{index_name}' deleted.")
        else:
            # Delete all indexes
            indices = es.indices.get_alias(index="*")
            for idx in indices:
                es.indices.delete(index=idx, ignore=[400, 404])
                logger.info(f"Index '{idx}' deleted.")
    except Exception as e:
        logger.error(f"An error occurred while deleting the index: {e}")

global_option = {
    "es.nodes": ELASTIC_HOST,
    "es.port": ELASTIC_PORT,
    "es.net.http.auth.user": ELASTIC_USERNAME,
    "es.net.http.auth.pass": ELASTIC_PASSWORD,
}

@exception_logger("elastic.insert_df")
def insert_df(df, id_col, index):
    # index=f"{ELASTIC_REDDIT_INDEX_NAME}/_doc"
    option=global_option.copy()
    option.update({
        "es.mapping.id": id_col,
        "es.resource": index
    })
    df.write.format("es").options(**option).mode("append").save()
    logger.info(f"Inserted {df.count()} rows in ElasticSearch.")

# es = get_elastic_session()
# delete_index(es)
# create_reddit_index(es)
