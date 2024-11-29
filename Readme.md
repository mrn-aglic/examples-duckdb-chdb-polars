# DuckDB, ChDB, Polars examples

This repo was created for the purpose of this medium article:
https://medium.com/@MarinAgli1/comparing-duckdb-chdb-and-polars-like-a-noob-ad74584456b9

When running the tests mentioned in the article, I ran the tests using
```shell
python -m app.run_tests
```
from the root directory.

Running the tests in Docker currently results in the following message:
```shell
run_tests  | Starting benchmark for entry: duckdb
run_tests  | /usr/local/lib/python3.12/multiprocessing/popen_fork.py:66: RuntimeWarning: Using fork() can cause Polars to deadlock in the child process.
run_tests  | In addition, using fork() with Python in general is a recipe for mysterious
run_tests  | deadlocks and crashes.
run_tests  | 
run_tests  | The most likely reason you are seeing this error is because you are using the
run_tests  | multiprocessing module on Linux, which uses fork() by default. This will be
run_tests  | fixed in Python 3.14. Until then, you want to use the "spawn" context instead.
run_tests  | 
run_tests  | See https://docs.pola.rs/user-guide/misc/multiprocessing/ for details.
run_tests  | 
run_tests  |   self.pid = os.fork()

```

When running the CSV to Redis example, use
```shell
make run-csv-redis
```
