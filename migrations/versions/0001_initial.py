"""initial schema: raw, core, lake

Revision ID: 0001
Revises:
"""
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

UP = """
CREATE SCHEMA raw;
CREATE SCHEMA core;
CREATE SCHEMA lake;

CREATE TABLE raw.fetch_log (
    id             bigserial PRIMARY KEY,
    source         text NOT NULL,
    url            text NOT NULL,
    request_params jsonb,
    status_code    integer,
    fetched_at     timestamptz NOT NULL DEFAULT now(),
    payload        jsonb,
    payload_raw    bytea,
    parse_status   text NOT NULL DEFAULT 'pending'
        CHECK (parse_status IN ('pending', 'ok', 'error', 'empty'))
);
CREATE INDEX fetch_log_source_fetched_idx ON raw.fetch_log (source, fetched_at);

CREATE TABLE core.flight_offer (
    offer_id         text PRIMARY KEY,
    source           text NOT NULL,
    origin_iata      char(3) NOT NULL,
    destination_iata char(3) NOT NULL,
    departure_at     timestamptz NOT NULL,
    arrival_at       timestamptz,
    airline_iata     text NOT NULL,
    flight_number    text,
    stops            integer NOT NULL DEFAULT 0,
    price            numeric(12,2) NOT NULL CHECK (price >= 0),
    currency         char(3) NOT NULL,
    seats_total      integer CHECK (seats_total > 0),
    seats_left       integer,
    status           text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'sold_out', 'expired')),
    first_seen_at    timestamptz NOT NULL,
    last_seen_at     timestamptz NOT NULL,
    CHECK (seats_left IS NULL OR (seats_left >= 0 AND seats_left <= seats_total))
);
CREATE INDEX flight_offer_departure_idx ON core.flight_offer (departure_at);
CREATE INDEX flight_offer_route_idx ON core.flight_offer (origin_iata, destination_iata);
CREATE INDEX flight_offer_status_idx ON core.flight_offer (status);

CREATE TABLE core.flight_offer_history (
    id          bigserial PRIMARY KEY,
    offer_id    text NOT NULL REFERENCES core.flight_offer (offer_id),
    observed_at timestamptz NOT NULL,
    price       numeric(12,2) NOT NULL,
    seats_left  integer,
    status      text NOT NULL,
    cause       text NOT NULL CHECK (cause IN ('scrape', 'generator'))
);
CREATE INDEX flight_offer_history_offer_idx
    ON core.flight_offer_history (offer_id, observed_at);

CREATE TABLE core.ticket_sale (
    sale_id     uuid PRIMARY KEY,
    offer_id    text NOT NULL REFERENCES core.flight_offer (offer_id),
    quantity    integer NOT NULL CHECK (quantity > 0),
    unit_price  numeric(12,2) NOT NULL,
    total_price numeric(12,2) NOT NULL,
    currency    char(3) NOT NULL,
    sold_at     timestamptz NOT NULL
);
CREATE INDEX ticket_sale_offer_idx ON core.ticket_sale (offer_id);
CREATE INDEX ticket_sale_sold_at_idx ON core.ticket_sale (sold_at);

CREATE TABLE core.theater_performance (
    performance_id text PRIMARY KEY,
    title          text NOT NULL,
    stage          text,
    starts_at      timestamptz NOT NULL,
    url            text,
    status         text NOT NULL DEFAULT 'on_sale'
        CHECK (status IN ('on_sale', 'sold_out', 'cancelled', 'past')),
    first_seen_at  timestamptz NOT NULL,
    last_seen_at   timestamptz NOT NULL
);
CREATE INDEX theater_performance_starts_idx ON core.theater_performance (starts_at);

CREATE TABLE core.theater_snapshot (
    id              bigserial PRIMARY KEY,
    performance_id  text NOT NULL REFERENCES core.theater_performance (performance_id),
    observed_at     timestamptz NOT NULL,
    category        text NOT NULL,
    price           numeric(12,2),
    currency        char(3),
    seats_available integer NOT NULL CHECK (seats_available >= 0)
);
CREATE INDEX theater_snapshot_perf_idx
    ON core.theater_snapshot (performance_id, observed_at);

CREATE TABLE core.event_log (
    event_id    uuid PRIMARY KEY,
    seq         bigserial NOT NULL UNIQUE,
    event_type  text NOT NULL,
    topic       text NOT NULL,
    occurred_at timestamptz NOT NULL,
    payload     jsonb NOT NULL
);
CREATE INDEX event_log_type_idx ON core.event_log (event_type, occurred_at);

CREATE TABLE lake.message (
    id           bigserial PRIMARY KEY,
    team         text NOT NULL,
    channel      text NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now(),
    source_ref   jsonb,
    content_type text,
    payload_raw  bytea NOT NULL,
    payload_json jsonb
);
CREATE INDEX lake_message_team_received_idx ON lake.message (team, received_at);
CREATE UNIQUE INDEX lake_message_team_source_ref_uq
    ON lake.message (team, source_ref) WHERE source_ref IS NOT NULL;
"""

DOWN = """
DROP SCHEMA lake CASCADE;
DROP SCHEMA core CASCADE;
DROP SCHEMA raw CASCADE;
"""


def upgrade() -> None:
    op.execute(UP)


def downgrade() -> None:
    op.execute(DOWN)
