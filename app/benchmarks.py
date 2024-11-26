import multiprocessing
import os
import time
import timeit
from multiprocessing import Process, Queue

import numpy as np
import polars as pl
from memory_profiler import memory_usage

from app.classes import TestSetting


def _calc_sust_avg(mem_usage: list):
    profiler_peak = max(mem_usage)
    threshold = 0.05
    lower_threshold = profiler_peak * (1 - threshold)
    upper_threshold = profiler_peak * (1 + threshold)

    return np.average([m for m in mem_usage if lower_threshold <= m <= upper_threshold])


def benchmark(
    tool_name: str,
    data_type: str,
    action,
    run_number: int,
    queue: multiprocessing.Queue,
) -> dict:
    mem_usage = memory_usage((action,), interval=0.1)

    sust_avg = _calc_sust_avg(mem_usage)

    time.sleep(2)
    execution_time = timeit.timeit(action, number=1)

    result = {
        "pid": os.getpid(),
        "tool_name": tool_name,
        "data_type": data_type,
        "profiler_min_mem_usage": min(mem_usage),
        "profiler_max_mem_usage": max(mem_usage),
        "profiler_median_mem_usage": np.median(mem_usage),
        # "profiler_avg_mem_usage": np.average(mem_usage),
        "profiler_sust_avg_mem_usage": sust_avg,
        "profiler_percentile_95_mem_usage": np.percentile(mem_usage, 95),
        "execution_time": execution_time,
        "run_number": run_number,
    }

    queue.put(result)

    return result


def benchmark_start(test_setting: TestSetting):
    num_runs = 1
    results = []

    queue = Queue()

    for i in range(num_runs):
        fun = test_setting.fun
        data_type = test_setting.data_type

        tool_name = test_setting.entry.split("_", 1)[0]

        print(f"Starting benchmark for entry: {test_setting.entry}")

        p = Process(target=benchmark, args=(tool_name, data_type, fun, (i + 1), queue))
        p.start()

        p.join()
        print(f"Finished benchmark for entry: {test_setting.entry}")

    while not queue.empty():
        results.append(queue.get())

    return pl.DataFrame(results)
