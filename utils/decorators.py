import logging
from functools import wraps
import time


class Decorators:
    def __init__(self,logger:logging.Logger):
        self.logger = logger
     
    
    def timer(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            self.logger.info(f"Executing {func.__name__}() function...")
            result = func(*args, **kwargs)
            end_time = time.time()
            self.logger.info(f"Finished executing {func.__name__}() function and took {end_time - start_time:.2f}"
                             f" seconds to run.")
            return result

        return wrapper

    def retry(self, func, max_tries=3, delay_seconds=1):
        @wraps(func)
        def wrapper_retry(*args, **kwargs):
            tries = 0
            while tries < max_tries:
                try:
                    self.logger.info(f"Retry {func.__name__}() function {tries} times...")
                    return func(*args, **kwargs)
                except Exception as e:
                    tries += 1
                    if tries == max_tries:
                        raise e
                    time.sleep(delay_seconds)

        return wrapper_retry
