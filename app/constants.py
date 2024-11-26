import functools

from app.classes import TestSetting
from app.tools import chdb, duckdb, polars

RESULT_COLUMNS = [
    "pid",
    "tool_name",
    "data_type",
    "execution_time",
    "time",
    "memory_before",
    "memory_after",
    "memory",
    "run_number",
    "_int_mem_before",
    "_int_mem_after",
]


FILE_PATHS = [
    "data/yellow_tripdata_2016-01.csv",
    "data/yellow_tripdata_2016-02.csv",
    "data/yellow_tripdata_2015-01.csv",
    "data/yellow_tripdata_2016-03.csv",
]


# FILE_PATTERN = "data/yellow_tripdata_20*.csv" # for all files - use at your own risk
FILE_PATTERN = "data/yellow_tripdata_2016-03.csv"
# FILE_PATTERN = "data/testing.csv"

TEST_LOAD_CSV_TO_MEMORY = [
    TestSetting(
        "duckdb",
        functools.partial(duckdb.load_csvs, pattern=FILE_PATTERN),
        "list[tuple]",
    ),
    TestSetting(
        "duckdb_df",
        functools.partial(duckdb.load_csvs_df, pattern=FILE_PATTERN),
        "pd.DataFrame",
    ),
    TestSetting(
        "chdb",
        functools.partial(
            chdb.load_csvs,
            pattern=FILE_PATTERN,
        ),
        "str - default",
    ),
    # TestSetting(
    #     "chdb_json",
    #     functools.partial(
    #         chdb.load_csvs,
    #         pattern=FILE_PATTERN,
    #         output_format=chdb.JSON,
    #         skip_parse=False,
    #     ),
    #     "dict - parsed with json",
    # ),
    TestSetting(
        "chdb_df",
        functools.partial(chdb.load_csvs_df, pattern=FILE_PATTERN),
        "pd.DataFrame",
    ),
    TestSetting(
        "polars",
        functools.partial(polars.load_csvs_df, pattern=FILE_PATTERN),
        "pl.DataFrame",
    ),
]
