VM          := flink-lab
VM_CONFIG   := infra/lima/flink-lab.yaml
COMPOSE     := docker compose -f infra/docker-compose.yml -p hashtags

# Always talk to the Docker daemon inside the Lima VM, never Docker Desktop.
export DOCKER_HOST := unix://$(HOME)/.lima/$(VM)/sock/docker.sock

.DEFAULT_GOAL := help

help: ## List targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

# ---- VM lifecycle -----------------------------------------------------------

vm-create: ## Create the Lima VM (one-time, ~5 min)
	limactl create --name=$(VM) --mount-only="$(CURDIR)" --tty=false $(VM_CONFIG)

vm-start: ## Start the VM if it is not running
	@limactl list --format '{{.Status}}' $(VM) 2>/dev/null | grep -q Running || limactl start $(VM)

vm-stop: ## Stop the VM (frees its RAM)
	limactl stop $(VM)

vm-delete: ## Delete the VM and everything in it
	limactl delete --force $(VM)

vm-shell: ## Open a shell inside the VM
	limactl shell $(VM)

# ---- Stack ------------------------------------------------------------------

up: vm-start ## Start VM + all services and wait until healthy
	$(COMPOSE) up -d --wait
	@$(MAKE) --no-print-directory topics urls

topics: ## Create Kafka topics (idempotent)
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
		--create --if-not-exists --topic posts --partitions 6 --replication-factor 1 \
		--config retention.ms=86400000

down: ## Stop services and the VM
	-$(COMPOSE) down
	$(MAKE) --no-print-directory vm-stop

clean: ## Stop services and delete their data volumes
	$(COMPOSE) down -v

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Tail logs (make logs s=taskmanager-1)
	$(COMPOSE) logs -f --tail=100 $(s)

stats: ## Live CPU/RAM per container
	docker stats

urls: ## Print UI endpoints
	@echo ""
	@echo "  Flink UI      http://localhost:8081"
	@echo "  Kafka         localhost:29092   (topic: posts)"
	@echo "  Redis         localhost:6379"
	@echo ""

# ---- Pokes ------------------------------------------------------------------

smoke: ## Run a bundled Flink example job to check the cluster
	./infra/scripts/smoke.sh

smoke-stop: ## Cancel running jobs
	./infra/scripts/smoke.sh stop

kafka-topics: ## Describe Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe

redis-cli: ## Open redis-cli
	$(COMPOSE) exec redis redis-cli


.PHONY: smoke-stop topics help vm-create vm-start vm-stop vm-delete vm-shell up down clean ps logs stats urls smoke kafka-topics redis-cli
