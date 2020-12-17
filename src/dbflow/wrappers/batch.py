from metaflow import batch as meta_batch
import functools


def batch(func=None, cpu=1, memory=500):
    if func is None:
        return functools.partial(batch, cpu=cpu, memory=memory)

    # Ensure the function is properly wrapped by the Metaflow batch function before moving on
    batch_func = meta_batch(cpu=cpu, memory=memory)(func)

    # Wrap the function in another function to enable call-time logic
    def _decorate(batch_func):
        @functools.wraps(batch_func)
        def wrapped_function(self, *args, **kwargs):
            return batch_func(self, *args, **kwargs)

        return wrapped_function

    # Return the decorated function
    return _decorate(batch_func)
