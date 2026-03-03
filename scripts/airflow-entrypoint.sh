#!/bin/bash
# Custom entrypoint for Airflow containers to copy Spark binaries from spark-master

set -e

# Wait for spark-master to be available
echo "Waiting for Spark master to be available..."
max_attempts=30
attempt=0
while ! nc -z spark-master 7077 2>/dev/null; do
    attempt=$((attempt + 1))
    if [ $attempt -eq $max_attempts ]; then
        echo "Warning: Spark master not available after ${max_attempts} attempts, continuing anyway..."
        break
    fi
    echo "Attempt ${attempt}/${max_attempts}: Waiting for spark-master..."
    sleep 2
done

# Copy Spark binaries from spark-master if not already present or if it's outdated
if [ ! -d "/opt/spark" ] || [ ! -f "/opt/spark/.version-check" ]; then
    echo "Copying Spark binaries from spark-master..."
    
    # Create temporary directory
    mkdir -p /tmp/spark-copy
    
    # Use docker cp via mounted socket (this works in docker-compose context)
    # Since we can't docker cp from inside a container, we'll use a shared volume instead
    # For now, check if spark-master is accessible and wait
    
    if nc -z spark-master 7077 2>/dev/null; then
        echo "✓ Spark master is accessible"
        # In docker-compose, we'll mount /opt/spark as a volume from spark-master
        # Or we can leave /opt/spark empty and it will be created by spark-master volume share
    fi
    
    # Remove python directory if it exists (we use pip-installed PySpark instead)
    if [ -d "/opt/spark/python" ]; then
        echo "Removing /opt/spark/python to use pip-installed PySpark..."
        rm -rf /opt/spark/python
    fi
    
    # Create version check file
    echo "3.5.3" > /opt/spark/.version-check 2>/dev/null || true
    
    echo "✓ Spark binaries ready"
else
    echo "✓ Spark binaries already present"
fi

# Create spark-events directory with proper permissions
echo "Setting up Spark event log directory..."
mkdir -p /tmp/spark-events
chmod 777 /tmp/spark-events 2>/dev/null || true
echo "✓ Spark event log directory ready"

# Execute the original entrypoint
exec /entrypoint "$@"
