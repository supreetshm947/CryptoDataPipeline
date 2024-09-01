#!/bin/bash

# Load the .env file
set -a
source .env
set +a

# Define the user and group ID for the containers
USER_ID=1000
GROUP_ID=1000
PERMISSIONS=755

# Iterate over all environment variables
for var in $(env | grep '^VOLUME_'); do
    # Extract the directory path from the variable
    dir_path=$(echo $var | cut -d '=' -f 2)

    # Check if the directory exists
    if [ -d "$dir_path" ]; then
        echo "Setting permissions for $dir_path"

        # Change the ownership
        sudo chown -R $USER_ID:$GROUP_ID "$dir_path"

        # Set the permissions
        sudo chmod -R $PERMISSIONS "$dir_path"
    else
        echo "Directory $dir_path does not exist. Skipping..."
    fi
done
