from app.tools import chdb, polars, duckdb

test_file = "data/testing.csv"

print("Loading small csv for testing - mostly to verify data schemas")

print("Loading data with DuckDB")
print("Function:> load_csv")

duckdb_data = duckdb.load_csv(test_file)

print(f"data:> {duckdb_data}")

print("Function:> load_csv_df")

duckdb_df = duckdb.load_csv_df(test_file)

print(f"type of duckdb df:> {type(duckdb_df)}")
print(duckdb_df)

print("Loading data with ChDB")
print("Function:> load_csv")

chdb_data = chdb.load_csv(test_file, output_format=chdb.SIMILAR_TO_DUCKDB_FETCHALL)

print(f"data:> {chdb_data}")

print("Function:> load_csv_df")

chdb_df = chdb.load_csv_df(test_file)

print(f"type of chdb df:> {type(chdb_df)}")
print(chdb_df)


print("Loading data with Polars")

print("Function:> load_csv_df")

polars_df = polars.load_csv_df(test_file)

print(polars_df)

polars.transfer_df_data([test_file])
