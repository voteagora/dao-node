import time
from functools import wraps

from sanic.response import json

def with_duration(handler):
    
    @wraps(handler)
    async def wrapper(request, *args, **kwargs):
        start_time = time.monotonic()

        result = await handler(request, *args, **kwargs)

        end_time = time.monotonic()
        duration = int((end_time - start_time) * 1_000_000) # microseconds.

        return json({
            "d": duration,
            "r": result
        })

    return wrapper