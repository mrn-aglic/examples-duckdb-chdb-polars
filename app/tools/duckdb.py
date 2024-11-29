import duckdb

from app.redis import helper as redis_helper


def load_csvs(pattern: str):
    conn = duckdb.connect()

    data = conn.execute(
        f"""
        SELECT * FROM read_csv('{pattern}')
    """
    ).fetchall()

    conn.close()

    return data


def load_csvs_df(pattern: str):
    conn = duckdb.connect()

    df = conn.execute(
        f"""
        SELECT * FROM read_csv('{pattern}')
    """
    ).fetchdf()

    conn.close()

    return df


def csv_to_redis(pattern: str):
    conn = duckdb.connect()
    DEFAULT_CHUNK_SIZE = 10_000

    offset = 0

    query_res = conn.execute(
        f"""SELECT *
                  REPLACE(
                      strftime(tpep_pickup_datetime, '%Y-%m-%dT%H:%M:%S') AS tpep_pickup_datetime,
                      strftime(tpep_dropoff_datetime, '%Y-%m-%dT%H:%M:%S') AS tpep_dropoff_datetime
                  )
              FROM read_csv('{pattern}')
              LIMIT 100_000
    """
    )

    for batch in query_res.fetch_record_batch(rows_per_batch=DEFAULT_CHUNK_SIZE):
        batch = batch.to_pandas()

        redis_helper.store_df_to_redis(batch, key_prefix="duckdb", offset=offset)
        offset = offset + DEFAULT_CHUNK_SIZE


def csv_to_redis_test():
    conn = duckdb.connect()
    offset = 0

    DEFAULT_CHUNK_SIZE = 10

    query_res = conn.execute("SELECT * FROM read_csv('data/sample_100_rows.csv')")

    for batch in query_res.fetch_record_batch(rows_per_batch=DEFAULT_CHUNK_SIZE):
        batch = batch.to_pandas()

        redis_helper.store_df_to_redis(batch, key_prefix="duckdb", offset=offset)
        offset = offset + DEFAULT_CHUNK_SIZE
