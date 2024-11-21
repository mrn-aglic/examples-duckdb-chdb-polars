import polars as pl

from app.redis import helper as redis_helper


def load_csv_df(file_path: str):
    df = pl.read_csv(file_path)

    print(f"DataFrame shape:> {df.shape}")

    return df


def load_csvs_df():
    file_paths = [
        "data/yellow_tripdata_2016-01.csv",
        "data/yellow_tripdata_2016-02.csv",
        "data/yellow_tripdata_2015-01.csv",
        "data/yellow_tripdata_2016-03.csv",
    ]

    df = pl.DataFrame()
    for file_path in file_paths:
        batch_df = load_csv_df(file_path)
        df = pl.concat([df, batch_df], how="vertical")

    print(f"DataFrame shape:> {df.shape}")


def transfer_df_data(file_paths: list[str], key: str):
    for file_path in file_paths:
        df = load_csv_df(file_path)
        redis_helper.store_df_to_redis(df, key)
