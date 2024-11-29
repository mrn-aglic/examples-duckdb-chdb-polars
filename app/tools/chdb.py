import json

import chdb

from app.redis import helper as redis_helper


def load_csvs(pattern: str, output_format: str | None = None):
    query = f"SELECT * FROM file('{pattern}')"

    res = (
        chdb.query(query, output_format=output_format)
        if output_format
        else chdb.query(query)
    )

    data = res.data()

    return data


def load_csvs_df(pattern: str):
    query = f"SELECT * FROM file('{pattern}')"

    df = chdb.query(query, output_format="DataFrame")

    return df


def load_csv(pattern: str):
    query = f"SELECT * FROM file('{pattern}')"

    res = chdb.query(query)

    return res.data()


def csv_to_redis(pattern: str):
    DEFAULT_CHUNK_SIZE = 10_000

    offset = 0

    query_template = f"""
            SELECT *
                REPLACE(
                    formatDateTime(tpep_pickup_datetime, '%Y-%m-%dT%H:%i:%S') AS tpep_pickup_datetime,
                    formatDateTime(tpep_dropoff_datetime, '%Y-%m-%dT%H:%i:%S') AS tpep_dropoff_datetime
                )
            FROM file('{pattern}') LIMIT {DEFAULT_CHUNK_SIZE} OFFSET {{offset}}
        """

    while True:
        query = query_template.format(offset=offset)

        batch = chdb.query(query, output_format="DataFrame")

        # Break if no more rows
        if batch.empty:
            break

        redis_helper.store_df_to_redis(batch, key_prefix="chdb", offset=offset)

        offset = offset + DEFAULT_CHUNK_SIZE

        if offset >= 100_000:
            break


def csv_to_redis_table(pattern: str):
    DEFAULT_CHUNK_SIZE = 10_000
    offset = 0

    session = chdb.session.Session()

    query = f"""DESCRIBE TABLE file('{pattern}', 'CSVWithNames') SETTINGS describe_compact_output=1"""

    result = session.query(query, fmt="JSON")

    json_result = json.loads(result.data())
    json_schema = json_result["data"]

    table_cols = [
        f'{details["name"]} {details["type"].replace("DateTime", "String")}'
        for details in json_schema
    ]

    table_cols = str.join(",", table_cols)

    session.query("CREATE DATABASE IF NOT EXISTS redis_db")

    session.query(
        f"""CREATE TABLE IF NOT EXISTS redis_db.sample(
        id UUID DEFAULT generateUUIDv4(),
        {table_cols}
    ) ENGINE = Redis('redis:6379', 1) PRIMARY KEY (id)
    """
    )

    session.query(
        f"""INSERT INTO redis_db.sample
        SELECT *
                REPLACE(
                    formatDateTime(parseDateTime(tpep_pickup_datetime), '%Y-%m-%dT%H:%i:%S') AS tpep_pickup_datetime,
                    formatDateTime(parseDateTime(tpep_dropoff_datetime), '%Y-%m-%dT%H:%i:%S') AS tpep_dropoff_datetime
                )
            FROM file('{pattern}') LIMIT {DEFAULT_CHUNK_SIZE} OFFSET {offset}
    """
    )

    df = session.query(
        "SELECT * EXCEPT id FROM redis_db.sample LIMIT 10", fmt="DataFrame"
    )

    print(df)

    # chdb.query("CREATE TABLE IF NOT EXISTS file('{pattern}')")
