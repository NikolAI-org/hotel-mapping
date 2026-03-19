.PHONY: help build up down restart logs clean setup init-airflow prepare-host install-hooks

help:
	@echo "Delta Lake Airflow Pipeline - Available Commands"
	@echo "================================================="
	@echo "make setup        - Initial setup and start all services"
	@echo "make build        - Build Docker images"
	@echo "make up           - Start all services"
	@echo "make down         - Stop all services"
	@echo "make restart      - Restart all services"
	@echo "make logs         - View all logs"
	@echo "make clean        - Stop and remove all containers, volumes, and images"
	@echo "make init-airflow - Initialize Airflow database"
	@echo "make install-hooks - Install git pre-commit hook (ruff check)"
	@echo "================================================="

install-hooks:
	@echo "Installing git hooks..."
	cp hooks/pre-commit .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
	@echo "  ✓ pre-commit hook installed"

setup:
	@echo "Running initial setup..."
	$(MAKE) prepare-host
	./setup.sh

prepare-host:
	@echo "Preparing host bind-mount directories (/tmp dirs are cleared on reboot)..."
	mkdir -p /tmp/hotel-mapping-spark-tmp
	chmod -R 777 /tmp/hotel-mapping-spark-tmp
	@echo "  ✓ /tmp/hotel-mapping-spark-tmp"
	@echo "  ✓ hf-model-cache is a Docker named volume (no host prep needed)"

build:
	@echo "Building Docker images..."
	docker-compose build

up:
	@echo "Starting all services..."
	$(MAKE) prepare-host
	docker-compose up -d

down:
	@echo "Stopping all services..."
	docker-compose down

restart:
	@echo "Restarting all services..."
	docker-compose restart

logs:
	@echo "Showing logs (Ctrl+C to exit)..."
	docker-compose logs -f

logs-airflow:
	@echo "Showing Airflow logs..."
	docker-compose logs -f airflow-webserver airflow-scheduler airflow-worker

logs-spark:
	@echo "Showing Spark logs..."
	docker-compose logs -f spark-master spark-worker

logs-minio:
	@echo "Showing MinIO logs..."
	docker-compose logs -f minio

clean:
	@echo "Cleaning up all resources..."
	@read -p "Are you sure? This will delete all data [y/N]: " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v --rmi all; \
		rm -rf logs/* data/*; \
		echo "Cleanup complete!"; \
	fi

init-airflow:
	@echo "Initializing Airflow database..."
	docker-compose up airflow-init

ps:
	@echo "Service status..."
	docker-compose ps

shell-airflow:
	@echo "Opening Airflow shell..."
	docker-compose exec airflow-webserver bash

shell-spark:
	@echo "Opening Spark Master shell..."
	docker-compose exec spark-master bash

test-connection:
	@echo "Testing MinIO connection..."
	@docker-compose exec -T airflow-webserver python -c "from minio import Minio; client = Minio('minio:9000', 'minioadmin', 'minioadmin', secure=False); print('✓ MinIO connection successful'); print('Buckets:', [b.name for b in client.list_buckets()])"
