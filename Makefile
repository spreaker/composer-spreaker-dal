.DEFAULT_GOAL := help
.PHONY: test
.ONESHELL:

WARNING='\033[0;33m'
INFO='\033[0;32m'
NC='\033[0m'

help: ## shows this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-27s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ => Run the targets below on your host machine 💻

##@ -> 🐳 Docker Utils: build, start/stop, ...

build-all: ## builds all images
	docker-compose build

build-php: ## rebuild php container. use this if you changed the Dockerfile
	docker-compose build php

up: ## start postgres container
	docker-compose up -d dal-postgresql-test

down: ## stops postgres container
	docker-compose down --remove-orphans dal-postgresql-test

bash: ## bash session on php container. useful to run tests and stuff
	docker-compose exec php bash

##@ -> 🧪 Tests

test: ## runs tests
	docker-compose run -it --rm dal-php sh -c "cd /dal && php vendor/bin/phpunit"

