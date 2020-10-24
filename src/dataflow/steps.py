from metaflow import step as meta_step
import functools


def step(func, starting_point=False, optional_argument2=None):
    # Ensure the function is properly wrapped by the Metaflow step function before moving on
    meta_func = meta_step(func)

    # Wrap the function in another function to enable call-time logic
    def _decorate(meta_func):
        @functools.wraps(meta_func)
        def wrapped_function(*args, **kwargs):
            meta_func(*args, **kwargs)

        return wrapped_function

    # Return the decorated function
    return _decorate(meta_func)
