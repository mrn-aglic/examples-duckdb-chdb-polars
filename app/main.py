import functools

from tools import duckdb, chdb, polars

STORE_TO_REDIS_TUPLES = "STORE_TO_REDIS_TUPLES"
STORE_TO_REDIS_DICTS = "STORE_TO_REDIS_DICTS"
STORE_TO_REDIS_DF = "STORE_TO_REDIS_DF"

LOAD_FROM_REDIS = "LOAD_FROM_REDIS"

COMPARISONS = {
    "duckdb": {
        STORE_TO_REDIS_TUPLES: duckdb.transfer_data,
        STORE_TO_REDIS_DF: duckdb.transfer_data,
    },
    "chdb": {
        STORE_TO_REDIS_TUPLES: functools.partial(chdb.transfer_data, output_format=chdb.VALUES),
        STORE_TO_REDIS_DICTS: functools.partial(chdb.transfer_data, output_format=chdb.PRETTY_JSON),
        STORE_TO_REDIS_DF: chdb.transfer_df_data,
    },
    "polars": {
        STORE_TO_REDIS_DF: polars.transfer_df_data,
    }
}

# def duckdb_benchmark(data):
#     con = duckdb.connect()
#     con.register("data", data)
#     con.execute("CREATE TABLE result AS SELECT * FROM data")
#     con.execute("COPY result TO 'duckdb_output.csv' (FORMAT CSV)")
#
# def chdb_benchmark(data):
#     df = pl.DataFrame(data)
#     df.write_csv("chdb_input.csv")
#     con = chdb.connect("chdb_input.csv")
#     con.execute("SELECT * FROM file").write_csv("chdb_output.csv")
#
# def polars_benchmark(data):
#     df = pl.DataFrame(data)
#     df.write_csv("polars_output.csv")

if __name__ == "__main__":

    print("Benchmarking DuckDB...")
    # benchmark("DuckDB", lambda: duckdb_benchmark(data))
    #
    # print("Benchmarking CHDB...")
    # benchmark("CHDB", lambda: chdb_benchmark(data))
    #
    # print("Benchmarking Polars...")
    # benchmark("Polars", lambda: polars_benchmark(data))
