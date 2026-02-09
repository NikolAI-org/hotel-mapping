#!/bin/bash

# Setup script for Delta Lake Airflow Pipeline
# This script initializes the environment and starts all services

set -e

echo "=========================================="
echo "Delta Lake Airflow Pipeline Setup"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker and Docker Compose are installed${NC}"

# Create necessary directories
echo ""
echo "Creating necessary directories..."
mkdir -p ./logs ./plugins ./data ./spark/jars
echo -e "${GREEN}✓ Directories created${NC}"

# Download required Spark JARs if not present
echo ""
echo "Checking Spark JAR dependencies..."
JARS_DIR="./spark/jars"
MAVEN_REPO="https://repo1.maven.org/maven2"

declare -A JARS=(
    ["hadoop-aws-3.3.4.jar"]="org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    ["aws-java-sdk-bundle-1.12.262.jar"]="com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
    ["delta-spark_2.12-3.1.0.jar"]="io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar"
    ["delta-storage-3.1.0.jar"]="io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar"
)

jars_missing=false
for jar_name in "${!JARS[@]}"; do
    if [ ! -f "$JARS_DIR/$jar_name" ]; then
        jars_missing=true
        break
    fi
done

if [ "$jars_missing" = true ]; then
    echo "Downloading missing JAR dependencies..."
    for jar_name in "${!JARS[@]}"; do
        jar_path="${JARS[$jar_name]}"
        output_file="$JARS_DIR/$jar_name"
        
        if [ -f "$output_file" ]; then
            echo -e "${GREEN}  ✓ $jar_name already exists${NC}"
            continue
        fi
        
        echo "  - Downloading $jar_name..."
        url="$MAVEN_REPO/$jar_path"
        
        if command -v wget &> /dev/null; then
            wget -q --show-progress "$url" -O "$output_file" || {
                echo -e "${RED}  ✗ Failed to download $jar_name${NC}"
                rm -f "$output_file"
                exit 1
            }
        elif command -v curl &> /dev/null; then
            curl -L --progress-bar "$url" -o "$output_file" || {
                echo -e "${RED}  ✗ Failed to download $jar_name${NC}"
                rm -f "$output_file"
                exit 1
            }
        else
            echo -e "${RED}  ✗ Neither wget nor curl found. Please install one of them.${NC}"
            exit 1
        fi
        echo -e "${GREEN}  ✓ Downloaded $jar_name${NC}"
    done
    echo -e "${GREEN}✓ All JAR dependencies downloaded${NC}"
else
    echo -e "${GREEN}✓ All JAR dependencies already present${NC}"
fi

# Set AIRFLOW_UID if not set (for Linux)
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo ""
    echo "Setting AIRFLOW_UID for Linux..."
    if ! grep -q "AIRFLOW_UID" .env; then
        echo "AIRFLOW_UID=$(id -u)" >> .env
        echo -e "${GREEN}✓ AIRFLOW_UID set to $(id -u)${NC}"
    else
        echo -e "${YELLOW}⚠ AIRFLOW_UID already set in .env${NC}"
    fi
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo ""
    echo -e "${YELLOW}⚠ .env file not found. Creating from .env.example...${NC}"
    cp .env.example .env
    echo -e "${GREEN}✓ .env file created${NC}"
else
    echo -e "${GREEN}✓ .env file exists${NC}"
fi

# Build and start services
echo ""
echo "Building Docker images (this may take several minutes)..."
docker-compose build

echo ""
echo "Starting all services..."
docker-compose up -d

echo ""
echo "Waiting for services to be healthy..."
sleep 10

# Copy Spark binaries from spark-master to Airflow containers
echo ""
echo "Copying Spark binaries to Airflow containers..."
if docker ps | grep -q "hotel-mapping-spark-master"; then
    echo "  - Copying from spark-master..."
    docker exec -u root hotel-mapping-airflow-worker-1 mkdir -p /opt/spark 2>/dev/null || true
    docker cp hotel-mapping-spark-master-1:/opt/spark /tmp/spark-setup 2>/dev/null || echo "    Note: Spark already exists or will be copied on next start"
    
    if [ -d "/tmp/spark-setup" ]; then
        docker cp /tmp/spark-setup hotel-mapping-airflow-worker-1:/opt/ 2>/dev/null || true
        docker exec -u root hotel-mapping-airflow-worker-1 rm -rf /opt/spark/python 2>/dev/null || true
        rm -rf /tmp/spark-setup
        echo "  ✓ Spark binaries copied to airflow-worker"
    fi
    
    # Copy to other Airflow containers
    for container in $(docker ps --filter "name=hotel-mapping-airflow" --format "{{.Names}}" | grep -v worker); do
        docker cp hotel-mapping-spark-master-1:/opt/spark /tmp/spark-setup 2>/dev/null || true
        if [ -d "/tmp/spark-setup" ]; then
            docker cp /tmp/spark-setup "$container":/opt/ 2>/dev/null || true
            docker exec -u root "$container" rm -rf /opt/spark/python 2>/dev/null || true
            rm -rf /tmp/spark-setup
            echo "  ✓ Spark binaries copied to $container"
        fi
    done
else
    echo "  ⚠ Spark master not running yet, skipping Spark copy"
fi

# Check service status
echo ""
echo "Checking service status..."
docker-compose ps

echo ""
echo "=========================================="
echo -e "${GREEN}✓ Setup Complete!${NC}"
echo "=========================================="
echo ""
echo "Access the services:"
echo "  - Airflow UI:      http://localhost:8080"
echo "    Username: airflow"
echo "    Password: airflow"
echo ""
echo "  - Spark Master UI: http://localhost:8081"
echo ""
echo "  - MinIO Console:   http://localhost:9001"
echo "    Username: minioadmin"
echo "    Password: minioadmin"
echo ""
echo "Next steps:"
echo "  1. Wait 2-3 minutes for all services to be fully ready"
echo "  2. Access Airflow UI and enable the DAGs"
echo "  3. Trigger 'hotel_data_ingestion' DAG first"
echo "  4. Then trigger 'hotel_delta_transformation' DAG"
echo ""
echo "To view logs: docker-compose logs -f [service-name]"
echo "To stop: docker-compose down"
echo "To stop and remove volumes: docker-compose down -v"
echo ""
