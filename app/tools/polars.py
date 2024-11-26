import polars as pl

from app.redis import helper as redis_helper


def transfer_df_data(file_paths: list[str], key: str):
    for file_path in file_paths:
        df = load_csvs_df(file_path)
        redis_helper.store_df_to_redis(df, key)


def load_csvs_df(pattern: str):
    return pl.read_csv(pattern)
