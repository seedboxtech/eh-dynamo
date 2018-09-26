# Event Horizon Dynamo

Event Horizon Dynamo contains the DynamoDB driver for [Event Horizon] a CQRS/ES toolkit for Go.

[Event Horizon]: https://github.com/looplab/eventhorizon

# Usage

See the Event Horizon example folder for a few examples to get you started and replace the storage drivers (event store and/or repo)

## Development

To develop Event Horizon Dynamo you need to have Docker and Docker Compose installed.

To start all needed services and run all tests, simply run make:

```bash
make
```

To manually run the services and stop them:

```bash
make services
make stop
```

When the services are running testing can be done either locally or with Docker:

```bash
make test
make test_docker
go test ./...
```
