"""flight_offer: prirážka generátora k cene zo zdroja

Revision ID: 0004
Revises: 0003
"""
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

UP = """
-- price = cena zo zdroja (zberač ju prepisuje); price_markup = prirážka generátora podľa
-- obsadenosti (>= 1). Cena, za ktorú sa letenka predáva, je price * price_markup.
ALTER TABLE core.flight_offer
    ADD COLUMN price_markup numeric(8,5) NOT NULL DEFAULT 1 CHECK (price_markup >= 1);
"""

DOWN = "ALTER TABLE core.flight_offer DROP COLUMN price_markup;"


def upgrade() -> None:
    op.execute(UP)


def downgrade() -> None:
    op.execute(DOWN)
