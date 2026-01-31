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
mkdir -p ./logs ./plugins ./data
echo -e "${GREEN}✓ Directories created${NC}"

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
