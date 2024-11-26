#!/bin/bash

python -m app.main --entry duckdb
python -m app.main --entry duckdb_df
python -m app.main --entry chdb
python -m app.main --entry chdb_df
python -m app.main --entry polars
