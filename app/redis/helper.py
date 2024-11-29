from typing import Iterable

import pandas as pd
import polars as pl
from redis import StrictRedis


def _get_redis():
    return StrictRedis(host="redis", port=6379, db=0)


def _get_iterable(df: pl.DataFrame | pd.DataFrame) -> Iterable:
    return df.iterrows() if isinstance(df, pd.DataFrame) else enumerate(df.to_dicts())


def store_df_to_redis(
    df: pd.DataFrame | pl.DataFrame, key_prefix: str, offset: int = 0
):
    r = _get_redis()

    iterable = _get_iterable(df)

    with r.pipeline() as pipe:

        for idx, row in iterable:
            num = (idx + 1) + offset

            key = f"{key_prefix}:{num}"

            if not isinstance(row, dict):
                row = row.to_dict()

            pipe.hset(key, mapping=row)

        pipe.execute()
