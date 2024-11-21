import json

import pandas as pd
import polars as pl
from redis import StrictRedis


def _get_redis():
    return StrictRedis(host="redis", port=6379, db=0)


def store_to_redis(data: list[tuple | dict], key_prefix: str):

    r = _get_redis()

    for idx, row in enumerate(data):
        key = f"{key_prefix}_{idx}"

        if isinstance(row, dict):
            r.hset(key, mapping=row)
        else:
            r.hset(key, str(idx), json.dumps(row))


# REQUIREMENT: want to store each row with a unique key - use index
# we want to store the value as a dictionary
def store_df_to_redis(df: pd.DataFrame | pl.DataFrame, key_prefix: str):

    r = _get_redis()

    if isinstance(df, pd.DataFrame):

        for idx, row in df.iterrows():
            key = f"{key_prefix}_{idx}"
            r.hset(key, mapping=row.to_dict())

    elif isinstance(df, pl.DataFrame):

        for idx, row in enumerate(df.to_dicts()):
            key = f"{key_prefix}_{idx}"
            r.hset(key, mapping=row)