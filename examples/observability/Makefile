.PHONY: up down logs clean build help

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start all services
	docker-compose up --build

up-d: ## Start all services in detached mode
	docker-compose up --build -d
	@echo ""
	@echo "✅ Services started!"
	@echo ""
	@echo "📊 Grafana:    http://localhost:3000 (admin/admin)"
	@echo "📈 Prometheus: http://localhost:9090"
	@echo "🔍 Jaeger:     http://localhost:16686"
	@echo ""
	@echo "Run 'make logs' to view logs"
	@echo "Run 'make down' to stop services"

down: ## Stop all services
	docker-compose down

down-v: ## Stop all services and remove volumes
	docker-compose down -v

logs: ## Follow logs from all services
	docker-compose logs -f

logs-generator: ## Follow logs from load generator only
	docker-compose logs -f load-generator

build: ## Build the load generator locally
	cd generator && go build -o load-generator .

clean: ## Clean up build artifacts and volumes
	docker-compose down -v
	rm -f generator/load-generator
	rm -rf prometheus-data grafana-data

restart: down up-d ## Restart all services

status: ## Show status of all services
	docker-compose ps
