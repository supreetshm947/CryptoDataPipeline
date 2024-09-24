from minio import Minio
from minio.error import S3Error
from constants import (MINIO_HOST, MINIO_PORT_API, MINIO_USER, MINIO_PASSWORD, MINIO_COIN_META_BUCKET,
                       MINIO_COIN_PRICE_BUCKET, MINIO_REDDIT_BUCKET)
from mylogger import get_logger

logger = get_logger()


# MinIO client configuration
minio_client = Minio(
    f"{MINIO_HOST}:{MINIO_PORT_API}",
    access_key=MINIO_USER,
    secret_key=MINIO_PASSWORD,
    secure=False  # Use secure=True if using HTTPS
)

buckets = [MINIO_COIN_META_BUCKET, MINIO_COIN_PRICE_BUCKET, MINIO_REDDIT_BUCKET]


def create_bucket_if_not_exists(bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            logger.info(f"Bucket '{bucket_name}' does not exist. Creating it...")
            minio_client.make_bucket(bucket_name)
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")
    except S3Error as err:
        logger.error(f"Error: {err}")


def main():
    for bucket in buckets:
        create_bucket_if_not_exists(bucket)

if __name__ == "__main__":
    main()
