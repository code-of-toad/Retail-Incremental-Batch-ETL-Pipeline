from datetime import datetime
from decimal import Decimal

# Tiny independent practice input; timestamps are already parsed for key comparisons.
PRACTICE_RECORDS = [
    {'order_id': 'O1001', 'order_line_id': 1, 'updated_at': datetime(2026, 4, 1, 9), 'unit_price': Decimal('10.00')},
    {'order_id': 'O1001', 'order_line_id': 1, 'updated_at': datetime(2026, 4, 1, 9), 'unit_price': Decimal('10.00')},
    {'order_id': 'O1001', 'order_line_id': 1, 'updated_at': datetime(2026, 4, 1, 10), 'unit_price': Decimal('12.00')},
    {'order_id': 'O1002', 'order_line_id': 1, 'updated_at': datetime(2026, 4, 1, 11), 'unit_price': Decimal('5.00')},
    {'order_id': 'O1002', 'order_line_id': 1, 'updated_at': datetime(2026, 4, 1, 11), 'unit_price': Decimal('6.00')},
    {'order_id': 'O1003', 'order_line_id': 1, 'updated_at': datetime(2026, 4, 1, 8), 'unit_price': Decimal('7.00')},
]


def count_keys(records: list[dict]) -> dict[tuple[str, int], int]:
    # TODO P1: Count occurrences of each composite key; empty input returns {}.
    raise NotImplementedError('P1: implement count_keys')


def find_duplicate_keys(records: list[dict]) -> set[tuple[str, int]]:
    # TODO P2: Use sets to identify keys appearing more than once.
    raise NotImplementedError('P2: implement find_duplicate_keys')


def sort_records(records: list[dict]) -> list[dict]:
    # TODO P3: Return a new list, ascending by (updated_at, order_id, order_line_id).
    # Write your own key= lambda. Do not change the input list.
    raise NotImplementedError('P3: implement sort_records')


def sort_in_place(records: list[dict]) -> None:
    # TODO P4: Apply the same ordering in place using list.sort and a lambda.
    raise NotImplementedError('P4: implement sort_in_place')


def timestamp_bounds(records: list[dict]) -> tuple[dict | None, dict | None]:
    # TODO P5: Use min/max with key= lambdas. Empty input returns (None, None).
    # Equal timestamp ties use the same order/key tuple as P3.
    raise NotImplementedError('P5: implement timestamp_bounds')


def select_latest(records: list[dict]) -> tuple[list[dict], list[dict]]:
    # TODO P6: Return winners and conflict occurrences without mutating input.
    # Quarantine an entire key group if any equal-time payloads disagree.
    # Timestamp sorting alone cannot resolve a conflicting source version.
    raise NotImplementedError('P6: implement select_latest')


if __name__ == '__main__':
    print('Start with P1: count_keys. The README contains expected results.')
    print(f'Practice fixture contains {len(PRACTICE_RECORDS)} records.')
