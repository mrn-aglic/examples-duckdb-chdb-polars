import logging

import polars as pl

from app.redis import helper as redis_helper

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def load_csvs_df(pattern: str):
    return pl.read_csv(pattern)


def _load_in_batches(pattern: str, chunk_size: int) -> pl.DataFrame:
    NUM_ROWS = 100_000

    lz_df = pl.scan_csv(pattern, n_rows=NUM_ROWS)

    offset = 0

    while True:
        batch = (
            lz_df.slice(offset=offset, length=chunk_size).with_columns(
                pl.col("tpep_pickup_datetime")
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                .dt.strftime("%Y-%m-%dT%H:%M:%S")
                .alias("tpep_pickup_datetime"),
                pl.col("tpep_dropoff_datetime")
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                .dt.strftime("%Y-%m-%dT%H:%M:%S")
                .alias("tpep_dropoff_datetime"),
            )
        ).collect()

        if batch.is_empty():
            return None

        offset = offset + chunk_size

        yield offset, batch

        if offset >= NUM_ROWS:
            return None


def csv_to_redis(pattern: str):
    DEFAULT_CHUNK_SIZE = 10_000

    for offset, batch in _load_in_batches(pattern, chunk_size=DEFAULT_CHUNK_SIZE):
        redis_helper.store_df_to_redis(
            df=batch, key_prefix="polars", offset=offset - DEFAULT_CHUNK_SIZE
        )


def polars_sql_query(pattern: str):
    with pl.SQLContext(trip_data=pl.scan_csv(pattern)) as ctx:
        query = """SELECT *
                REPLACE(
                    strftime(strptime(tpep_pickup_datetime, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%dT%H:%M:%S') AS tpep_pickup_datetime,
                    strftime(strptime(tpep_dropoff_datetime, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%dT%H:%M:%S') AS tpep_dropoff_datetime
                )
            FROM trip_data LIMIT 100000"""

        lz_df = ctx.execute(query)
        print(lz_df.collect())
