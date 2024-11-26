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


