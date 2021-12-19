import functools
import logging
import random
import time

logger = logging.getLogger()


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """Exponential time decorator to make retries if service is not allowed."""

    def exponential_sleep_generator(start_time, factor_incr, border_time):
        """Generates sleep intervals based on the exponential back-off algorithm."""
        delay = start_time
        while True:
            yield min(random.uniform(0.0, delay * 2.0), border_time)
            delay = delay * factor_incr

    def retry_target(target, sleep_generator):
        """Call a function and retry if it fails."""

        for sleep in sleep_generator:
            try:
                return target()
            except Exception as exc:
                logger.info('Service unavailable. will retry. Exception %s', str(exc))
                time.sleep(sleep)

        raise ValueError("Sleep generator stopped yielding sleep values.")

    def func_wrapper(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            """A wrapper that calls target function with retry."""
            target = functools.partial(func, *args, **kwargs)
            sleep_generator = exponential_sleep_generator(start_sleep_time, factor, border_sleep_time)
            return retry_target(target, sleep_generator)

        return wrapped

    return func_wrapper
