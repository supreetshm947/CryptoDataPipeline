#!/bin/sh

# Wait for MinIO to fully start
sleep 120

# Define bucket names
BUCKETS=("coin-metadata" "coin-prices" "reddit-submissions")

# Configure MinIO client (mc) to use the MinIO server
mc alias set myminio http://127.0.0.1:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

# Create each bucket if it doesn't already exist
for BUCKET in "${BUCKETS[@]}"; do
    if ! mc ls myminio/$BUCKET; then
        echo "Creating bucket: $BUCKET"
        mc mb myminio/$BUCKET
    fi
done
