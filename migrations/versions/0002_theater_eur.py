"""divadlo: repertoire id, ceny v EUR, kurzy, zistené predaje

Revision ID: 0002
Revises: 0001
"""
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

UP = """
CREATE TABLE core.fx_rate (
    rate_date  date NOT NULL,
    currency   char(3) NOT NULL,
    per_eur    numeric(14,6) NOT NULL CHECK (per_eur > 0),  -- jednotiek meny na 1 EUR (ECB)
    fetched_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, currency)
);

ALTER TABLE core.theater_performance
    ADD COLUMN repertoire_id integer,   -- id v predajnom systéme (mapa miest)
    ADD COLUMN instance_id integer,     -- id predstavenia na webe divadla (stabilné)
    ADD COLUMN location text;
CREATE UNIQUE INDEX theater_performance_repertoire_uq
    ON core.theater_performance (repertoire_id) WHERE repertoire_id IS NOT NULL;
CREATE UNIQUE INDEX theater_performance_instance_uq
    ON core.theater_performance (instance_id) WHERE instance_id IS NOT NULL;

ALTER TABLE core.theater_snapshot
    ADD COLUMN price_eur numeric(12,2),
    ADD COLUMN fx_rate numeric(14,6);

CREATE TABLE core.theater_sale (
    sale_id        uuid PRIMARY KEY,
    performance_id text NOT NULL REFERENCES core.theater_performance (performance_id),
    category       text NOT NULL,
    quantity       integer NOT NULL CHECK (quantity <> 0),  -- záporné = miesta vrátené
    unit_price     numeric(12,2) NOT NULL,
    currency       char(3) NOT NULL,
    unit_price_eur numeric(12,2),
    fx_rate        numeric(14,6),
    detected_from  timestamptz NOT NULL,
    detected_to    timestamptz NOT NULL
);
CREATE INDEX theater_sale_perf_idx ON core.theater_sale (performance_id, detected_to);
"""

DOWN = """
DROP TABLE core.theater_sale;
ALTER TABLE core.theater_snapshot DROP COLUMN price_eur, DROP COLUMN fx_rate;
DROP INDEX core.theater_performance_instance_uq;
DROP INDEX core.theater_performance_repertoire_uq;
ALTER TABLE core.theater_performance
    DROP COLUMN repertoire_id, DROP COLUMN instance_id, DROP COLUMN location;
DROP TABLE core.fx_rate;
"""


def upgrade() -> None:
    op.execute(UP)


def downgrade() -> None:
    op.execute(DOWN)
