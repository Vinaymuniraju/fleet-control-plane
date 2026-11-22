-- Desired and observed state for every managed service.
CREATE TABLE services (
    id               text PRIMARY KEY,
    name             text NOT NULL UNIQUE,
    engine           text NOT NULL CHECK (engine IN ('kong', 'postgres')),
    spec             jsonb NOT NULL,
    status           jsonb NOT NULL,
    resource_version bigint NOT NULL,
    generation       bigint NOT NULL,
    deleting         boolean NOT NULL DEFAULT false,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL
);

-- Append-only audit trail, capped per service by the store.
CREATE TABLE service_events (
    id         bigserial PRIMARY KEY,
    service_id text NOT NULL REFERENCES services (id) ON DELETE CASCADE,
    at         timestamptz NOT NULL,
    type       text NOT NULL,
    reason     text NOT NULL,
    message    text NOT NULL
);
CREATE INDEX service_events_service_id_id ON service_events (service_id, id);
