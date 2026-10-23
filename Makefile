SHELL := /bin/bash
PWD := $(shell pwd)

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	script -q -c "docker compose -f docker-compose.yaml logs --follow" /dev/null | grep -v "rabbitmq"
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs