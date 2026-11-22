.PHONY: build image test vet fmt run cluster cluster-down e2e chaos

build:
	go build -o bin/fleetd ./cmd/fleetd
	go build -o bin/fleetctl ./cmd/fleetctl

# Linux binaries for the in-cluster image (GOARCH follows the host, which matches kind's node).
image:
	CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o bin/linux/fleetd ./cmd/fleetd
	CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o bin/linux/fleet-load ./cmd/fleet-load
	docker build -q -f Dockerfile.dev -t fleetd:dev . >/dev/null
	docker image prune -f >/dev/null

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
