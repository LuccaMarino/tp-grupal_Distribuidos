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

serial:
	docker build -q -t tp-serial validation
	for dir in datasets/client_*; do docker run --rm --user $$(id -u):$$(id -g) -v "$(PWD)":/app -w /app tp-serial python3 validation/serial_run.py $$dir expected $$(basename $$dir); done
.PHONY: serial

compare:
	python3 validation/compare.py output expected
.PHONY: compare