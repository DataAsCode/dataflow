from metaflow import step as meta_step
import functools


def step(func):
    # Ensure the function is properly wrapped by the Metaflow step function before moving on
    meta_func = meta_step(func)

    # Wrap the function in another function to enable call-time logic
    def _decorate(meta_func):
        @functools.wraps(meta_func)
        def wrapped_function(self, *args, **kwargs):
            return meta_func(self, *args, **kwargs)

        return wrapped_function

    # Return the decorated function
    return _decorate(meta_func)


