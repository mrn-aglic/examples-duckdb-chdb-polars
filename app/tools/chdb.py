import ast
import json

import chdb
from app.redis import helper as redis_helper

VALUES = "VALUES"
PRETTY_JSON = "PrettyJSONEachRow"

def _format_data_output(output_format: str, data):
    if output_format == VALUES:
        formatted_string = f"[{data}]"
        data = ast.literal_eval(formatted_string)

    if output_format == PRETTY_JSON:
        data = json.loads(data)

    return data

def load_csv_df(file_path: str):
    query = f"SELECT * FROM file('{file_path}', 'CSV')"

    output_format = "DataFrame"

    df = chdb.query(query, output_format=output_format)

    print(f"shape of df:> {df.shape}")

    return df


def load_csv(file_path: str, output_format: str = PRETTY_JSON):
    query = f"SELECT * FROM file('{file_path}', 'CSV')"

    res = chdb.query(query, output_format=output_format)

    data = res.data()

    # regardless of the format I tried (except DataFrame) res.data() returned a str
    # I wasn't expecting this
    # print(type(data))
    data = _format_data_output(output_format, data)

    print(f"#rows:> {len(data)}")

    return data


def load_csvs(output_format: str = PRETTY_JSON):
    query = f"SELECT * FROM file('yellow_tripdata_20*.csv', 'CSV')"

    res = chdb.query(query)

    data = res.data()
    data = _format_data_output(output_format, data)

    return data


def load_csvs_df():
    query = f"SELECT * FROM file('yellow_tripdata_20*.csv', 'CSV')"

    res = chdb.query(query)

    df = res.to_pandas()

    return df


def transfer_df_data(file_paths: list[str], key: str):
    for file_path in file_paths:
        df = load_csv_df(file_path)
        redis_helper.store_df_to_redis(df, key)


def transfer_data(file_paths: list[str], key: str, output_format: str):
    for file_path in file_paths:
        df = load_csv(file_path, output_format=output_format)
        redis_helper.store_to_redis(df, key)
