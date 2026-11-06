.PHONY: up down build test produce-mk consume-mk clean learning-broker fmt vet

up:
	docker compose -f deployments/docker-compose.yml up --build -d

down:
	docker compose -f deployments/docker-compose.yml down

produce-mk:
	go run ./cmd/produce -addr 127.0.0.1:9092 -topic orders -partitions 3 -count 50 -acks -1

consume-mk:
	go run ./cmd/consume -addr 127.0.0.1:9092 -topic orders -group workers

build:
	go build -o bin/broker ./cmd/broker
	go build -o bin/produce ./cmd/produce
	go build -o bin/consume ./cmd/consume

test:
	go test ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

clean:
	docker compose -f deployments/docker-compose.yml down -v
	rm -rf bin data

learning-broker:
	go run ./cmd/broker -id broker-1 -listen :9092 -http :8080 -data ./data/broker-1
