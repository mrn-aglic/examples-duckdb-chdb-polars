from app.tools import chdb, duckdb, polars

test_file = "data/testing.csv"

print("Loading small csv for testing - mostly to verify data schemas")

print("DuckDB ******")

duckdb_data = duckdb.load_csvs(test_file)

print(f"data:> {duckdb_data}")
print(f"type(data): {type(duckdb_data)}")
print(f"type(data[0]): {type(duckdb_data[0])}")

print("ChDB ********")

chdb_data = chdb.load_csv(test_file)

print(f"data:> {chdb_data}")
print(f"type(data): {type(chdb_data)}")


print("Loading data with Polars")
print("PolarsDB ****")

polars_df = polars.load_csvs_df(test_file)

print(f"data:> {polars_df}")
print(f"type(data): {type(polars_df)}")
