.DEFAULT_GOAL := help
.PHONY: build-all build-php up down bash test
.ONESHELL:

WARNING='\033[0;33m'
INFO='\033[0;32m'
NC='\033[0m'

help: ## shows this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-27s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ => Run the targets below on your host machine 💻

##@ -> 🐳 Docker Utils: build, start/stop, ...

build-all: ## builds all images
	docker compose build

build-php: ## rebuild php container. use this if you changed the Dockerfile
	docker compose build dal-php

up: ## start postgres container
	docker compose up -d dal-postgresql-test

down: ## stops postgres container
	docker compose down --remove-orphans dal-postgresql-test

bash: ## bash session on php container. useful to run tests and stuff
	docker compose run -it --rm dal-php bash

##@ -> 🧪 Tests

init-db: ## creates test database
	docker compose run -it --rm dal-php sh -c "cd /dal && php scripts/init-db.php"

test: ## runs tests
	docker compose run -it --rm dal-php sh -c "cd /dal && php vendor/bin/phpunit --coverage-text"

test-with-coverage: ## runs tests
	docker compose run -it --rm dal-php sh -c "cd /dal && phpdbg -qrr ./vendor/bin/phpunit --coverage-text --coverage-html ./build/coverage"

