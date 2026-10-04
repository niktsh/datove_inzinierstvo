"""event_log: značka publikovania (neodoslané udalosti sa dajú odoslať znova)

Revision ID: 0003
Revises: 0002
"""
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

UP = """
ALTER TABLE core.event_log ADD COLUMN published_at timestamptz;
CREATE INDEX event_log_unpublished_idx ON core.event_log (seq) WHERE published_at IS NULL;
"""

DOWN = """
DROP INDEX core.event_log_unpublished_idx;
ALTER TABLE core.event_log DROP COLUMN published_at;
"""


def upgrade() -> None:
    op.execute(UP)


def downgrade() -> None:
    op.execute(DOWN)
