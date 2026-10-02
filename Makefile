# Run "make" or "make help" to list the targets.
.DEFAULT_GOAL := help
.PHONY: help up validate smoke extended-smoke audit traffic app-observability traces \
        kip714 kip714-status fault stop start down diagnostics package

help: ## List the targets
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-18s %s\n", $$1, $$2}'

# --- Deploy and test ---
up: ## Build, deploy and test the complete demo (first run: 20-40 min)
	./scripts/up.sh
validate: ## Static checks (configs, Ansible syntax, unit tests); no running lab needed
	./scripts/validate.sh
smoke: ## End-to-end test: CP metrics and logs on every host
	python3 scripts/smoke.py
extended-smoke: ## End-to-end test: application observability and KIP-714
	python3 scripts/extended-smoke.py
audit: ## List dashboard metrics present / missing in VictoriaMetrics
	python3 scripts/dashboard-audit.py > /dev/null && echo 'Report: artifacts/dashboard-audit.json'

# --- Live demo ---
traffic: ## Continuous Kafka traffic (Ctrl-C to stop)
	./scripts/traffic.sh
app-observability: ## Start the demo application and send traced orders
	docker compose up -d customer-observability-demo
	./scripts/customer-traffic.sh
traces: ## Send more traced orders to the demo application
	./scripts/customer-traffic.sh
kip714: ## Configure the KIP-714 subscription and start the KIP-714 client
	./scripts/configure-client-metrics.sh
	docker compose up -d kip714-demo
kip714-status: ## Show the KIP-714 subscription, plugin counters and client series
	./scripts/kip714-status.sh
fault: ## Stop broker3 for 45 s, then start it again
	./scripts/broker-failure.sh

# --- Lifecycle ---
stop: ## Stop all containers (keeps everything)
	docker compose stop
start: ## Start the containers again after "make stop"
	docker compose start
down: ## Remove the containers (CP installs are lost; data volumes are kept)
	@echo 'Removing the containers. CP must be reinstalled with "make up". Volumes are kept.'
	docker compose down
diagnostics: ## Collect logs into artifacts/diagnostics
	./scripts/diagnostics.sh
package: ## Create ../cp-otel-client-demo.zip (sources only, no .env or artifacts)
	python3 scripts/package.py
