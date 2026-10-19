"""network_links — core topology adjacencies.

Revision ID: 0002
Revises: 0001
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "network_links",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column(
            "a_device_id", sa.Uuid(), sa.ForeignKey("network_devices.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("a_interface", sa.String(128), nullable=False),
        sa.Column(
            "b_device_id", sa.Uuid(), sa.ForeignKey("network_devices.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("b_interface", sa.String(128), nullable=False),
        sa.Column("link_type", sa.String(16), nullable=False, server_default="core"),
        sa.Column("bandwidth_gbps", sa.Integer(), nullable=True),
        sa.Column("igp_metric", sa.Integer(), nullable=True),
        sa.Column("operational_status", sa.String(16), nullable=False, server_default="UP"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("a_device_id", "a_interface", name="uq_link_a_interface"),
        sa.UniqueConstraint("b_device_id", "b_interface", name="uq_link_b_interface"),
    )
    op.create_index("ix_network_links_a_device_id", "network_links", ["a_device_id"])
    op.create_index("ix_network_links_b_device_id", "network_links", ["b_device_id"])


def downgrade() -> None:
    op.drop_index("ix_network_links_b_device_id", table_name="network_links")
    op.drop_index("ix_network_links_a_device_id", table_name="network_links")
    op.drop_table("network_links")
