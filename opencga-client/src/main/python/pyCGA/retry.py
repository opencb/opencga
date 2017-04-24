"""
This module defines a function to retry execution of a function.
"""
import sys
import time

from pyCGA.commons import is_not_logged_in_exception, is_bad_login_exception


def retry(func, max_attempts, initial_retry_seconds, max_retry_seconds,
          login_handler=None, on_retry=None):
    """
    Attempt a function and retry until success or until running out of
    allowed attempts.
    :param func: function to invoke - parameterless
    :param max_attempts: int: maximum number of attempts allowed
    :param initial_retry_seconds: initial number of seconds to wait between retries.
        The wait time is doubled after each failure, until max_retry_seconds is
        reached.
    :param max_retry_seconds: int: see initial_retry_seconds
    :param login_handler: optional; if provided, then if calling func()
        results in an error because the sessionId is invalid, login_handler()
        will be called and this failed attempt will not count towards the maximum
        of attempts
    :param on_retry: a callback to be called before retrying. Must accept 3 parameters:
        exception value, exception type, traceback.
    :return: the result of func() if successful, otherwise the last exception raised
    by calling func.
    """
    attempt_number = 1
    retry_seconds = initial_retry_seconds
    while True:
        try:
            return func()
        except Exception as e:
            if is_not_logged_in_exception(e):
                if login_handler:
                    login_handler()
                else:
                    raise  # there's no point in retrying if we can't log in
            elif is_bad_login_exception(e):
                raise  # no point in retrying login if we have the wrong credentials
            else:
                if attempt_number >= max_attempts:  # last attempt failed, propagate error:
                    raise
                if on_retry:
                    # notify that we are retrying
                    exc_type, exc_val, exc_tb = sys.exc_info()
                    on_retry(exc_type, exc_val, exc_tb)

                time.sleep(retry_seconds)
                attempt_number += 1
                retry_seconds = min(retry_seconds * 2, max_retry_seconds)
