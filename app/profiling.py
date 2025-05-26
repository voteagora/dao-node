
import time
import threading
import atexit
from collections import defaultdict

class Profiler:
    def __init__(self, print_on_exit=False):
        self._lock = threading.Lock()
        self._stats = defaultdict(lambda: {"count": 0, "total": 0.0})
        self._sections = {}
        if print_on_exit:
            atexit.register(self.report)
        self.max_lable_len = 20

    class _Section:
        def __init__(self, profiler, label=None):
            self.profiler = profiler
            self.label = label
            self.start = None

        def __enter__(self):
            self.start = time.perf_counter()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            duration = time.perf_counter() - self.start
            with self.profiler._lock:
                stat = self.profiler._stats[self.label]
                stat["count"] += 1
                stat["total"] += duration
                stat["min"] = min(stat["min"], duration) if "min" in stat else duration
                stat["max"] = max(stat["max"], duration) if "max" in stat else duration


    def __call__(self, label=None):
        return self._Section(self, label)

    def report(self):
        print("\n--- Profiler Report ---")
        if len(self._stats) == 0:
            print("No stats to report.")
            return
        max_label = max(len(label) for label in self._stats.keys())

        print(f"{'Label':<{max_label}} {'Count':<8} {'Total(s)':<12} {'Avg(s)':<20} {'Min(s)':<20} {'Max(s)':<20}")
        print("-" * 56)
        for label, stat in sorted(self._stats.items()):
            count = stat["count"]
            total = stat["total"]
            avg = total / count if count > 0 else 0
            print(f"{label:<{max_label}} {count:<8} {total:<12.6f} {avg:<20.12f} {stat['min']:<20.12f} {stat['max']:<20.12f}")
        print("-" * 56)