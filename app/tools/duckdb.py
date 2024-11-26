import duckdb

from app.redis import helper as redis_helper

def load_csvs(pattern: str):
    conn = duckdb.connect()

    data = conn.execute(
        f"""
        SELECT * FROM read_csv('{pattern}')
    """
    ).fetchall()

    conn.close()

    return data


def load_csvs_df(pattern: str):
    conn = duckdb.connect()

    df = conn.execute(
        f"""
        SELECT * FROM read_csv('{pattern}')
    """
    ).fetchdf()

    conn.close()

    return df
