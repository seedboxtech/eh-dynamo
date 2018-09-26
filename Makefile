default: services test

test:
	go test ./...
.PHONY: test

test_docker:
	docker-compose run --rm golang make test
.PHONY: test_docker

services:
	docker-compose pull dynamodb
	docker-compose up -d dynamodb
.PHONY: services

stop:
	docker-compose down
.PHONY: stop
