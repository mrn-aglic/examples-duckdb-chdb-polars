import duckdb

from app.redis.helper import store_df_to_redis


def load_csv_df(file_path: str):
    conn = duckdb.connect()

    df = conn.execute(
        f"""
        SELECT * FROM read_csv('{file_path}')
    """
    ).fetchdf()

    print(f"shape of df:> {df.shape}")
    conn.close()

    return df


def load_csv(file_path: str):
    conn = duckdb.connect()

    data = conn.execute(
        f"""
        SELECT * FROM read_csv('{file_path}')
    """
    ).fetchall()

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


def transfer_df_data(file_paths: list[str]):
    for file_path in file_paths:
        df = load_csv_df(file_path)
        store_df_to_redis(df)


def transfer_data(file_paths: list[str]):
    for file_path in file_paths:
        df = load_csv(file_path)
        store_df_to_redis(df)
