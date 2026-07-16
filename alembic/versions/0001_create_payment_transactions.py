"""create payment transactions table

Revision ID: 0001
Revises: None
Create Date: 2026-07-15 13:37:44

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '0001'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Create the payment_transactions table
    op.create_table(
        'payment_transactions',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('idempotency_key', sa.String(length=255), nullable=False),
        sa.Column('amount', sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column('currency', sa.String(length=3), nullable=False),
        sa.Column('user_id', sa.String(length=255), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('retry_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('idempotency_key')
    )
    
    # Create additional indexes (idempotency_key is already unique/indexed by unique constraint)
    op.create_index(op.f('ix_payment_transactions_user_id'), 'payment_transactions', ['user_id'], unique=False)
    op.create_index(op.f('ix_payment_transactions_status'), 'payment_transactions', ['status'], unique=False)
    op.create_index(op.f('ix_payment_transactions_idempotency_key'), 'payment_transactions', ['idempotency_key'], unique=True)

def downgrade() -> None:
    op.drop_index(op.f('ix_payment_transactions_status'), table_name='payment_transactions')
    op.drop_index(op.f('ix_payment_transactions_user_id'), table_name='payment_transactions')
    op.drop_index(op.f('ix_payment_transactions_idempotency_key'), table_name='payment_transactions')
    op.drop_table('payment_transactions')
