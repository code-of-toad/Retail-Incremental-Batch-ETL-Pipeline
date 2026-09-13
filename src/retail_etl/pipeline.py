import argparse
import json
import tempfile
from pathlib import Path
from retail_etl.config import BATCH_IDS, RUNTIME_DIR, SEED_DIR
from retail_etl.session import make_spark


def run_batch(batch_id: str, fail_at: str | None = None,
              seed_dir: Path = SEED_DIR, output_root: Path = RUNTIME_DIR) -> dict:
    # TODO D1: Wire ingestion -> parsing -> validation -> initial versions -> fact -> checks.
    # TODO D2: Load committed state, upsert, reconcile, publish, and record run outcomes.
    # Use try/finally to stop your Spark session, even when a stage fails.
    raise NotImplementedError('Implement the Day 1 pipeline stages before running a batch.')


def smoke_test() -> dict:
    # Supplied setup only: exercise a Spark action and a Parquet round trip.
    # No retail validation or transformation solution runs here.
    with tempfile.TemporaryDirectory(prefix='retail-etl-smoke-') as work:
        spark = make_spark('retail-etl-smoke', Path(work))
        try:
            path = str(Path(work) / 'parquet')
            spark.range(3).write.mode('overwrite').parquet(path)
            rows = spark.read.parquet(path).count()
            if rows != 3:
                raise RuntimeError('Spark Parquet smoke test returned the wrong count.')
            return {'status': 'SETUP_OK', 'spark_version': spark.version, 'parquet_rows': rows}
        finally:
            spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description='Retail ETL learning scaffold')
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--smoke-test', action='store_true')
    action.add_argument('--batch-id', choices=BATCH_IDS)
    parser.add_argument('--fail-at', choices=('after_stage', 'after_publish'))
    args = parser.parse_args()
    if args.smoke_test and args.fail_at:
        parser.error('--fail-at requires --batch-id')
    try:
        result = smoke_test() if args.smoke_test else run_batch(args.batch_id, args.fail_at)
    except NotImplementedError as error:
        print(f'EXERCISE_NOT_IMPLEMENTED: {error}')
        return 2
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
