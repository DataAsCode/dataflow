from metaflow import retry as meta_retry
import functools


def retry(func):
    # Ensure the function is properly wrapped by the Metaflow retry function before moving on
    meta_func = meta_retry(func)

    # Wrap the function in another function to enable call-time logic
    def _decorate(meta_func):
        @functools.wraps(meta_func)
        def wrapped_function(self, *args, **kwargs):
            return meta_func(self, *args, **kwargs)

        return wrapped_function

    # Return the decorated function
    return _decorate(meta_func)
