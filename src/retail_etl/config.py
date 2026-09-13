from pathlib import Path

# Resolve paths independently of the terminal's current directory.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SEED_DIR = PROJECT_ROOT / 'data' / 'seed'
RUNTIME_DIR = PROJECT_ROOT / 'runtime'
BATCH_IDS = ('2026-04-01', '2026-04-02', '2026-04-03')
BUSINESS_KEY = ('order_id', 'order_line_id')
SALES_FIELDS = (
    'order_id', 'order_line_id', 'store_id', 'product_id', 'sale_date',
    'quantity', 'unit_price', 'unit_cost', 'discount_amount',
    'order_status', 'updated_at',
)
SOURCE_FIELDS = ('source_row_id',) + SALES_FIELDS
ALLOWED_STATUSES = ('COMPLETED', 'CANCELLED')
