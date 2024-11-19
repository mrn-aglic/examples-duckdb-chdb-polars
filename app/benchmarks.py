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
