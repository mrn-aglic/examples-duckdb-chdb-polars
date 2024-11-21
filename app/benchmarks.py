import os
import time

import psutil


def benchmark(tool_name, action):
    start_time = time.time()
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / (1024 * 1024)  # in MB

    action()

    mem_after = process.memory_info().rss / (1024 * 1024)  # in MB
    end_time = time.time()

    print(
        f"{tool_name}: Time={end_time - start_time:.2f}s, Memory={mem_after - mem_before:.2f}MB"
    )


def load_to_redis_benchmark(tool_name: str):
    benchmark()


def benchmark_start(work_dict: dict[str, dict]):

    for tool_name, details in work_dict.items():
        for format in details["formats"]:
            for work in details["works"]:
                redis_key = f"{tool_name}:{format}:{work}"
