.PHONY: help install deps test build run docs clean debug

# Default target
help:
	@echo "S3 to Iceberg dbt Project"
	@echo "Available targets:"
	@echo "  install         Install dependencies"
	@echo "  deps            Download dbt packages"
	@echo "  build           Build all models"
	@echo "  test            Run all tests"
	@echo "  test-quick      Run tests with fail-fast"
	@echo "  run             Run dbt models"
	@echo "  run-dev         Run models for development"
	@echo "  run-staging     Run models for staging"
	@echo "  run-prod        Run models for production"
	@echo "  run-dbt         Run dbt with arguments"
	@echo "  compile         Compile SQL files"
	@echo "  docs            Generate documentation"
	@echo "  serve           Serve documentation"
	@echo "  debug           Debug dbt configuration"
	@echo "  clean           Clean target directories"
	@echo ""
	@echo "Usage: make [target] ENV=development"

# Install dbt and dependencies
install:
	pip install -r requirements.txt
	dbt deps

# Download dbt packages
deps:
	dbt deps

# Build all models
build:
	dbt build --profiles-dir . --target $(ENV)

# Run all models
run:
	dbt run --profiles-dir . --target $(ENV)

run-dev:
	dbt run --profiles-dir . --target development

run-staging:
	dbt run --profiles-dir . --target staging

run-prod:
	dbt run --profiles-dir . --target production

# Run dbt with arguments
run-dbt:
	dbt $(ARGS) --profiles-dir . --target $(ENV)

# Run tests
test:
	dbt test --profiles-dir . --target $(ENV)

test-quick:
	dbt test --profiles-dir . --target $(ENV) --fail-fast

# Compile SQL
compile:
	dbt compile --profiles-dir . --target $(ENV)

# Generate and serve docs
docs:
	dbt docs generate --profiles-dir . --target $(ENV)

serve:
	dbt docs serve

# Debug configuration
debug:
	dbt debug --profiles-dir . --target $(ENV)

# Clean generated artifacts
clean:
	dbt clean

# Run specific model
run-model:
	dbt run --select $(MODEL) --profiles-dir . --target $(ENV)

# Test specific model
test-model:
	dbt test --select $(MODEL) --profiles-dir . --target $(ENV)

# Show model lineage
lineage:
	dbt docs generate && dbt docs serve

# Full pipeline run (parse → compile → run → test)
full-pipeline: clean deps compile run test docs
	@echo "✓ Full pipeline completed successfully"

# Environment setup
setup-dev:
	@bash switch-env.sh development

setup-staging:
	@bash switch-env.sh staging

setup-prod:
	@bash switch-env.sh production

.DEFAULT_GOAL := help