import polars as pl

from app.benchmarks import benchmark_start
from app.constants import TEST_LOAD_CSV_TO_MEMORY

TESTS_TO_USE = TEST_LOAD_CSV_TO_MEMORY


def start_tests():
    results = None

    for test_setting in TESTS_TO_USE:
        res = benchmark_start(test_setting=test_setting)

        if results is None:
            results = res
        else:
            results = pl.concat([results, res], how="vertical")

    with pl.Config(fmt_str_lengths=150, tbl_rows=30, tbl_cols=30):
        print(results)

    results.write_csv("stats.csv")

if __name__ == "__main__":
    start_tests()

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
