import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from retail_etl.config import RUNTIME_DIR


def make_spark(app_name: str = 'retail-etl', runtime_dir: Path = RUNTIME_DIR) -> SparkSession:
    # Local workers must use this environment's Python interpreter.
    os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master('local[2]')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.shuffle.partitions', '4')
        .config('spark.sql.adaptive.enabled', 'true')
        .config('spark.sql.ansi.enabled', 'true')
        .config('spark.sql.warehouse.dir', (runtime_dir / 'warehouse').as_uri())
        .getOrCreate()
    )
    # Four shuffle partitions is a tiny local starting point, not a tuning rule.
    spark.sparkContext.setLogLevel('WARN')
    return spark
