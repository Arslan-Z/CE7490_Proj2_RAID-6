import time

class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""
    
# Timer to measure the time of the execution of the code in seconds
class Timer(object):
    def __init__(self):
        self._start_time = None
        
    def start(self):
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")
        self._start_time = time.perf_counter()

    def stop(self):
        if self._start_time is None:
            raise ValueError("Timer is not running. Use start() to start it")
        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        return elapsed_time
    
if __name__ == "__main__":
    timer = Timer()
    timer.start()
    time.sleep(1)
    # print(f"Elapsed time: {timer.stop():0.4f} seconds")
    print(f"Elapsed time: {timer.stop()} seconds")