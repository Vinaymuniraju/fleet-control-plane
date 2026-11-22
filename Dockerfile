FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fleetd ./cmd/fleetd && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fleet-load ./cmd/fleet-load

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/fleetd /out/fleet-load /
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/fleetd", "-kubeconfig="]
