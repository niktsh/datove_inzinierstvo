"""theater_sale: pôvod predaja (zistený zo snímok alebo simulovaný generátorom)

Revision ID: 0005
Revises: 0004
"""
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None

UP = """
-- observed = reálny predaj zistený z rozdielu snímok, generator = simulovaný predaj.
-- Doterajšie riadky sú všetky reálne, preto predvolená hodnota 'observed'.
ALTER TABLE core.theater_sale
    ADD COLUMN source text NOT NULL DEFAULT 'observed'
    CHECK (source IN ('observed', 'generator'));
"""

DOWN = "ALTER TABLE core.theater_sale DROP COLUMN source;"


def upgrade() -> None:
    op.execute(UP)


def downgrade() -> None:
    op.execute(DOWN)
