import time
from functools import wraps
from sanic.request import Request
from sanic import response, HTTPResponse

async def start_timer(request: Request):
    # Record the start time for this request
    request.ctx.start_time = time.monotonic()

async def add_server_timing_header(request: Request, res: response.HTTPResponse):
    # Calculate duration in milliseconds
    duration_ms = (time.monotonic() - request.ctx.start_time) * 1000.0
    
    # Add Server-Timing header
    # e.g. 'Server-Timing: app;dur=123.45'
    res.headers["Server-Timing"] = res.headers.get("Server-Timing", "") + f'total;dur={duration_ms:.3f}'

def measure(handler):
    
    @wraps(handler)
    async def wrapper(request, *args, **kwargs):
        start_time = time.time()

        res = await handler(request, *args, **kwargs)

        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000.0 # milliseconds.

        res.headers["Server-Timing"] = f'data;dur={duration_ms:.3f},'

        return res

    return wrapper