from datetime import datetime
from decimal import Decimal
from pprint import pprint

# Tiny independent practice input; timestamps are already parsed for key comparisons.
PRACTICE_RECORDS = [
    {
        'order_id': 'O1001',
        'order_line_id': 1,
        'updated_at': datetime(2026, 4, 1, 9),
        'unit_price': Decimal('10.00')
    },
    {
        'order_id': 'O1001',
        'order_line_id': 1,
        'updated_at': datetime(2026, 4, 1, 9),
        'unit_price': Decimal('10.00')
    },
    {
        'order_id': 'O1001',
        'order_line_id': 1,
        'updated_at': datetime(2026, 4, 1, 10),
        'unit_price': Decimal('12.00')
    },
    {   
        'order_id': 'O1002',
        'order_line_id': 1,
        'updated_at': datetime(2026, 4, 1, 11),
        'unit_price': Decimal('5.00')
    },
    {
        'order_id': 'O1002',
        'order_line_id': 1,
        'updated_at': datetime(2026, 4, 1, 11),
        'unit_price': Decimal('6.00')
    },
    {
        'order_id': 'O1003',
        'order_line_id': 1,
        'updated_at': datetime(2026, 4, 1, 8),
        'unit_price': Decimal('7.00')
    },
]


def count_keys(records: list[dict]) -> dict[tuple[str, int], int]:
    """
    Count occurrences of each composite key; empty input returns {}.

    Time Complexity:
        O(n)
    Space Complexity:
        O(k), where k = number of distinct composite keys
    
    Q: Why use both order ID and line ID as the key?
    A: (In the final fact table) order ID and line ID together must form a
       a unique business key. I.e., no two rows can share the same combination
       of the two key columns.

    Q: Why can tuples serve as a dictionary key?
    A: Dictionary keys must be hashable, and `tuple[str, int]` qualifies.
       Counter e.g., `tuple[list]` would NOT qualify as dictionary keys.
    """
    key_counts = {}
    for record in records:
        curr_key = (record['order_id'], record['order_line_id'])
        key_counts[curr_key] = key_counts.get(curr_key, 0) + 1
    return key_counts


def find_duplicate_keys(records: list[dict]) -> set[tuple[str, int]]:
    """
    Use sets to identify keys appearing more than once.

    Time Complexity:
        O(n)
    Space Complexity:
        O(k), where k = number of distinct composite keys
    
    Q: What information do you need to retain as you process each record?
    A: For each record, we must keep track of whether its key has already
       been encountered.

    Q: Why is a set appropriate here?
    A: Sets are appropriate for duplicate detection because you need to track
       membership w/o counting occurrences.
    """
    appeared_already = set()
    duplicate_keys = set()
    for record in records:
        curr_key = (record['order_id'], record['order_line_id'])
        if curr_key not in appeared_already:
            appeared_already.add(curr_key)
        else:
            duplicate_keys.add(curr_key)
    return duplicate_keys


def sort_records(records: list[dict]) -> list[dict]:
    """
    Return a new list, ascending by (updated_at, order_id, order_line_id).

    Time Complexity:
        O(n log n)
    Space Complexity:
        O(n)
    
    The lambda receives one dictionary and returns a tuple of sorting values.
    Python compares tuples left to right: timestamp first, then order ID,
    then line ID.
       
    sorted() returns a new list containing the original dictionaries, leaving
    the input list unchanged. Sorting is stable: records with identical
    sorting tuples retain their original relative order.

    Empty input returns [].
    """
    # HINT: Use `sorted()` w/ your own key= lambda.
    #       Do NOT change the input list.
    return sorted(
        records,
        key=lambda record: (
            record['updated_at'],
            record['order_id'],
            record['order_line_id'],
        )
    )


def sort_in_place(records: list[dict]) -> None:
    """
    Apply the same ordering in place using `list.sort` and a lambda.
    """
    return records.sort(
        key=lambda record: (
            record['updated_at'],
            record['order_id'],
            record['order_line_id'],
        )
    )


def timestamp_bounds(records: list[dict]) -> tuple[dict | None, dict | None]:
    """
    Use min/max with key= lambdas. Empty input returns (None, None).

    Equal timestamp ties use the same order/key tuple as P3.

    Time Complexity:
        O(n)
    Space Complexity:
        O(1)
    
    """
    # if not records:
    if not records:
        return (None, None)

    # Both operations use the same timestamp and business-key ordering.
    comparison_key = lambda record: (
        record['updated_at'],
        record['order_id'],
        record['order_line_id'],
    )
    earliest = min(records, key=comparison_key)
    latest = max(records, key=comparison_key)

    return (earliest, latest)


def select_latest(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Return winners and conflict occurrences without mutating input.

    Time Complexity:
        O(n)
    Space Complexity:
        O(n)
    
    Q: Consider these records for ('O1001', 1):
            Record	updated_at	Unit price
              A       09:00       10.00
              B       10:00       12.00
              C       10:00       13.00
              D       11:00       14.00
       (a) Which records belong in winners, and which belong in
          conflict_occurrences?
       (b) Does record D resolve the conflict under the starter's rule?
    A: (a) winners = []
           conflict_occurrences = [A, B, C, D]
       (b) No.
    
    Q: Why is select_latest() O(n) on average despite having nested loops?
    A: It's about the total number of records that the nested loops visit.
       Each record belongs to exactly one group.
    """
    # HINT: Quarantine an entire key group if any equal-time payloads disagree.
    #       Timestamp sorting alone cannot resolve a conflicting source version.

    # Group every occurrence by its order-line business key.
    records_by_key = {}
    for record in records:
        curr_key = (record['order_id'], record['order_line_id'])
        records_by_key.setdefault(curr_key, []).append(record)

    winners = []
    conflict_occurrences = []
    non_payload_fields = {'order_id', 'order_line_id', 'updated_at'}

    for group in records_by_key.values():
        # Track versions separately for each order line.
        payload_by_timestamp = {}
        has_conflict = False
        for record in group:
            timestamp = record['updated_at']
            payload = {
                field: value
                for field, value in record.items()
                if field not in non_payload_fields
            }
            if timestamp not in payload_by_timestamp:
                payload_by_timestamp[timestamp] = payload
            elif payload_by_timestamp[timestamp] != payload:
                # Equal timestamps w/ different business values conflict.
                has_conflict = True
                break
        if has_conflict:
            # Quarantine the entire group, including unexamined records.
            conflict_occurrences.extend(group)
        else:
            # All records in this group have the same composite key.
            winner = max(group, key=lambda record: record['updated_at'])
            winners.append(winner)

    return (winners, conflict_occurrences)


if __name__ == '__main__':
    # print('Start with P1: count_keys. The README contains expected results.')
    # print(f'Practice fixture contains {len(PRACTICE_RECORDS)} records.')

    # pprint(count_keys(PRACTICE_RECORDS), width=4)
    # print(find_duplicate_keys(PRACTICE_RECORDS))
    # pprint(sort_records(PRACTICE_RECORDS), width=4)
    # pprint(sort_in_place(PRACTICE_RECORDS), width=4)
    # pprint(timestamp_bounds(PRACTICE_RECORDS), width=4)
    pprint(select_latest(PRACTICE_RECORDS), width=4)
