# Use the official MinIO image
FROM minio/minio

# Set environment variables for MinIO credentials
ENV MINIO_ROOT_USER=admin
ENV MINIO_ROOT_PASSWORD=password

# Create data and configuration directories for MinIO
RUN mkdir -p /data /root/.minio

# Expose ports for MinIO (9000 for API, 9001 for web console)
EXPOSE 9000 9001

# Command to run MinIO server with the console address option
ENTRYPOINT ["minio", "server", "/data", "--console-address", ":9001"]