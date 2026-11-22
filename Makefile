.PHONY: build test vet fmt run cluster cluster-down e2e chaos

build:
	go build -o bin/fleetd ./cmd/fleetd
	go build -o bin/fleetctl ./cmd/fleetctl

test:
	go test -race ./...

vet:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

fmt:
	gofmt -w .

run: build
	./bin/fleetd

cluster:
	kind create cluster --name fleet --wait 120s

cluster-down:
	kind delete cluster --name fleet

e2e: build
	./scripts/e2e.sh

chaos: build
	./scripts/chaos.sh
