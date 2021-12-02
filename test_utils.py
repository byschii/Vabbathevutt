
from contextlib import contextmanager
from time import perf_counter as timing, sleep

@contextmanager
def timeit_context(name, max_time_ms):
    start_time = timing()
    yield
    et = (timing() - start_time) * 1000 #elaplsed time in ms
    log = f"[{name}] {'NOT ' if et>max_time_ms else ''}fast enough ({round(et,1)}ms)"
    print( log )
    sleep(0.1)


class timer:
    def __init__(self, max_time_ms:int, test_name:str=None):
        self._start = None
        self.elapsed = 0.0
        self.max_time_ms = max_time_ms
        self.test_name = test_name

    def start(self):
        if self._start is not None:
            raise RuntimeError('Timer already started...')
        self._start = timing()

    def stop(self):
        if self._start is None:
            raise RuntimeError('Timer not yet started...')
        end = timing()
        self.elapsed += ( end - self._start)  
        self._start = None

        if self.test_name is not None: print( self._get_log() )

    def _is_ok(self):
        return self.elapsed <= self.max_time_ms

    def _get_log(self):
        return f"""
        [{self.test_name}] {'NOT ' if not self._is_ok() else ''}fast enough ({round(self.elapsed,1)}sec)"""

    def __enter__(self):  # Setup
        self.start()
        return self

    def __exit__(self, *args):  # Teardown
        self.stop()