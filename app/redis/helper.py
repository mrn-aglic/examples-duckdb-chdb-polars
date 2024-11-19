import pandas as pd
from redis import StrictRedis


def _get_redis():
    return StrictRedis(host="redis", port=6379, db=0)


def store_to_redis(data: list[dict], key_prefix="comparison"):

    r = _get_redis()

    for idx, row in enumerate(data):
        key = f"{key_prefix}_{idx}"
        r.hset(key, mapping=row)


def store_df_to_redis(df: pd.DataFrame, key_prefix="comparison"):

    r = _get_redis()

    for idx, row in df.iterrows():
        key = f"{key_prefix}_{idx}"
        r.hset(key, mapping=row.to_dict())
