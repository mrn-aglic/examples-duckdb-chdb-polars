import duckdb

from app.redis import helper as redis_helper


def load_csv_df(file_path: str):
    conn = duckdb.connect()

    df = conn.execute(
        f"""
        SELECT * FROM read_csv('{file_path}')
    """
    ).fetchdf()

    print(f"DataFrame shape:> {df.shape}")

    conn.close()

    return df


def load_csv(file_path: str):
    conn = duckdb.connect()

    result = conn.execute(
        f"""
        SELECT * FROM read_csv('{file_path}', header=TRUE, auto_detect=TRUE)
    """
    )

    data = result.fetchall()

    print(f"#rows:> {len(data)}")
    conn.close()

    return data


def load_csvs():
    conn = duckdb.connect()

    data = conn.execute(
        """
        SELECT * FROM read_csv('yellow_tripdata_20*.csv', union_by_name = true)
    """
    ).fetchall()

    print("number of rows:", len(data))
    conn.close()


def load_csvs_df():
    conn = duckdb.connect()

    df = conn.execute(
        """
        SELECT * FROM read_csv('yellow_tripdata_20*.csv', union_by_name = true)
    """
    ).fetchdf()

    print(f"DataFrame shape:> {df.shape}")
    conn.close()


def transfer_df_data(file_paths: list[str], key: str):
    for file_path in file_paths:
        df = load_csv_df(file_path)
        redis_helper.store_df_to_redis(df, key)


def transfer_data(file_paths: list[str], key: str):
    for file_path in file_paths:
        df = load_csv(file_path)
        redis_helper.store_to_redis(df, key)
