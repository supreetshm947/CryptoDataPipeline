from minio import Minio
from minio.error import S3Error
from constants import (MINIO_KEY, MINIO_SECRET,MINIO_HOST, MINIO_BUCKET_COIN, MINIO_BUCKET_REDDIT)


# MinIO client configuration
minio_client = Minio(
    MINIO_HOST,  # MinIO endpoint
    access_key=MINIO_KEY,  # MinIO access key
    secret_key=MINIO_SECRET,  # MinIO secret key
    secure=False  # Use secure=True if using HTTPS
)

# Function to create a bucket if it doesn't exist
def create_bucket(bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except S3Error as e:
        print(f"Error creating bucket: {e}")



def main():
    print(f"MINO_HOST: {MINIO_HOST}")
    print(f"MINO_KEY: {MINIO_KEY}")
    print(f"MINO_SECRET: {MINIO_SECRET}")

    # Create 'coin' and 'reddit' buckets to store data in the delta lake format
    create_bucket(MINIO_BUCKET_COIN)
    create_bucket(MINIO_BUCKET_REDDIT)

if __name__ == "__main__":
    
    main()

